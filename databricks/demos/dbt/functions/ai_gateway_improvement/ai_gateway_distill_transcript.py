# Python UDF managed by dbt (a `functions/` resource). dbt pastes this file
# into the body of CREATE FUNCTION ... LANGUAGE PYTHON, so the arguments
# declared in _ai_gateway_improvement__functions.yml (request, response,
# api_type, max_chars) are in scope and the body ends with a top-level
# return. Never two consecutive dollar signs here, and no Jinja braces.
# Standard library only: Unity Catalog runs it in a sandbox.
#
# What it does: turns ONE logged gateway request/response pair into a small,
# bounded JSON summary of the conversation, so ai_decide judges a session
# from a few thousand characters instead of a 200 KB payload.
#
# Coding agents resend the whole conversation on every turn, so the largest
# request of a session already holds the full history. One payload per
# session is enough.
#
# Three payload shapes are understood:
#   * chat completions  (mlflow/openai .../chat/completions): messages[]
#   * Anthropic messages (anthropic/v1/messages): system + messages[] with blocks
#   * OpenAI responses  (.../responses): instructions + input[] items
import json
import re

EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
SECRET = re.compile(
    r"(dapi[0-9a-f]{20,}|dose[0-9a-f]{20,}|sk-[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{20,}"
    r"|AKIA[0-9A-Z]{16}|xox[abposr]-[A-Za-z0-9-]{10,}|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,})"
)
# context blocks coding agents inject into user turns; not user-authored
REMINDER = re.compile(r"<(system-reminder|environment_context|user_instructions|command-message)>.*?</\1>", re.S)
ERROR_TEXT = re.compile(
    r"(?i)(^\s*(error|traceback|exception|fatal)\b|exit code [1-9]|command failed|permission denied|no such file)"
)


def redact(text):
    return SECRET.sub("<secret>", EMAIL.sub("<email>", text))


def clip(text, limit):
    text = redact(" ".join(text.split()))
    return text if len(text) <= limit else text[: limit - 3] + "..."


def as_text(content):
    """Flatten a content field (string, block list, or dict) into plain text."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        for key in ("text", "content", "output", "input_text", "output_text"):
            if key in content:
                return as_text(content[key])
        return ""
    if isinstance(content, list):
        return "\n".join(as_text(part) for part in content)
    return str(content)


def human_text(content):
    """User-authored text only: drop tool results and injected reminders."""
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") in ("tool_result", "function_call_output", "image", "image_url", "input_image"):
                continue
            parts.append(as_text(block))
        content = "\n".join(parts)
    text = REMINDER.sub("", as_text(content)).strip()
    return text


def walk(req, resp):
    """Yield normalized events: (kind, text, extra) in conversation order."""
    system = req.get("system") or req.get("instructions") or ""
    yield ("system", as_text(system), None)

    items = req.get("messages")
    if items is None:
        items = req.get("input")
    if isinstance(items, str):
        items = [{"role": "user", "content": items}]

    for item in items or []:
        if not isinstance(item, dict):
            continue
        role = item.get("role")
        itype = item.get("type")
        content = item.get("content")

        # OpenAI responses: top-level tool items
        if itype in ("function_call", "custom_tool_call", "local_shell_call", "mcp_call"):
            yield ("tool_call", "", item.get("name") or itype)
            continue
        if itype in ("function_call_output", "custom_tool_call_output", "local_shell_call_output"):
            out = as_text(item.get("output"))
            yield ("tool_result", out, bool(ERROR_TEXT.search(out[:400])))
            continue
        if itype in ("reasoning", "additional_tools"):
            continue

        if role in ("system", "developer"):
            yield ("system", as_text(content), None)
            continue

        if role == "tool":  # chat completions tool result
            out = as_text(content)
            yield ("tool_result", out, bool(ERROR_TEXT.search(out[:400])))
            continue

        if isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                btype = block.get("type")
                if btype == "tool_use":
                    yield ("tool_call", "", block.get("name"))
                elif btype == "tool_result":
                    out = as_text(block.get("content"))
                    is_err = bool(block.get("is_error")) or bool(ERROR_TEXT.search(out[:400]))
                    yield ("tool_result", out, is_err)
        for call in item.get("tool_calls") or []:  # chat completions assistant tool calls
            name = (call.get("function") or {}).get("name") if isinstance(call, dict) else None
            yield ("tool_call", "", name)

        if role == "user":
            text = human_text(content)
            if text:
                yield ("user", text, None)
        elif role == "assistant":
            text = as_text([b for b in content if isinstance(b, dict) and b.get("type") in ("text", "output_text")]) if isinstance(content, list) else as_text(content)
            if text.strip():
                yield ("assistant", text, None)

    # the final reply lives in the response
    if isinstance(resp, dict):
        final = ""
        if resp.get("choices"):
            msg = (resp["choices"][0] or {}).get("message") or {}
            final = as_text(msg.get("content"))
            for call in msg.get("tool_calls") or []:
                yield ("tool_call", "", (call.get("function") or {}).get("name"))
        elif isinstance(resp.get("content"), list):  # anthropic
            for block in resp["content"]:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    yield ("tool_call", "", block.get("name"))
            final = as_text([b for b in resp["content"] if isinstance(b, dict) and b.get("type") == "text"])
        elif isinstance(resp.get("output"), list):  # responses
            for out in resp["output"]:
                if not isinstance(out, dict):
                    continue
                if out.get("type") == "message":
                    final += as_text(out.get("content"))
                elif out.get("type") in ("function_call", "custom_tool_call", "local_shell_call", "mcp_call"):
                    yield ("tool_call", "", out.get("name") or out.get("type"))
        if final.strip():
            yield ("final", final, None)


def distill(request_json, response_json, api, limit):
    limit = int(limit or 12000)
    try:
        req = json.loads(request_json) if request_json else {}
    except (ValueError, TypeError):
        req = {}
    try:
        resp = json.loads(response_json) if response_json else {}
    except (ValueError, TypeError):
        resp = {}
    if not isinstance(req, dict):
        req = {}

    users, assistants, errors, tools = [], 0, [], {}
    system_chars, n_events, n_tool_calls, n_tool_errors, final = 0, 0, 0, 0, ""
    for kind, text, extra in walk(req, resp):
        if kind == "system":
            system_chars += len(text)
            continue
        n_events += 1
        if kind == "user":
            users.append(text)
        elif kind == "assistant":
            assistants += 1
        elif kind == "tool_call":
            n_tool_calls += 1
            name = extra or "unknown"
            tools[name] = tools.get(name, 0) + 1
        elif kind == "tool_result" and extra:
            n_tool_errors += 1
            errors.append(text)
        elif kind == "final":
            final = text

    summary = {
        "api_type": api,
        "n_events": n_events,
        "n_user_turns": len(users),
        "n_assistant_turns": assistants,
        "n_tool_calls": n_tool_calls,
        "n_tool_errors": n_tool_errors,
        "top_tools": [name for name, _ in sorted(tools.items(), key=lambda kv: -kv[1])[:6]],
        "system_prompt_chars": system_chars,
        "first_user_request": clip(users[0], 1500) if users else "",
        "recent_user_turns": [clip(u, 600) for u in users[1:][-8:]],
        "tool_error_snippets": [clip(e, 240) for e in errors[-5:]],
        "final_assistant_reply": clip(final, 1500),
        "truncated": False,
    }

    # enforce the overall bound: drop the oldest turns first, then shorten
    def size():
        return len(json.dumps(summary))

    while size() > limit and summary["recent_user_turns"]:
        summary["recent_user_turns"].pop(0)
        summary["truncated"] = True
    while size() > limit and summary["tool_error_snippets"]:
        summary["tool_error_snippets"].pop(0)
        summary["truncated"] = True
    for key in ("final_assistant_reply", "first_user_request"):
        if size() > limit:
            summary[key] = clip(summary[key], 400)
            summary["truncated"] = True
    return json.dumps(summary)


return distill(request, response, api_type, max_chars)  # noqa: F706, F821 -- UDF body, not a module
