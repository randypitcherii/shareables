"""Row 08 — does Omnigent launch its native Claude terminal through `ug claude --provider`?

Installs nothing globally. Checks the plugin in ../omnigent_ug_launcher is importable, that its
entry point is registered in Omnigent's `omnigent.claude_launcher` group, and that Omnigent's
own resolve_claude_launch() returns the wrapped argv for this experiment's sub_a service. A live
session additionally needs a signed-in subscription (runbook row R3).
"""
import _row  # noqa: F401
import json
import os
import subprocess

from _common import ROOT, host, record, section, service

section("08 Omnigent native Claude → ug claude --provider")
env = {**os.environ, "OMNIGENT_CLAUDE_LAUNCHER": "ug", "UG_CLAUDE_PROVIDER": service("sub_a"),
       "UG_WORKSPACE": host()}
probe = (
    "import json, importlib.metadata as m\n"
    "eps=[e.name for e in m.entry_points(group='omnigent.claude_launcher')]\n"
    "try:\n"
    "    from omnigent.claude_launcher import resolve_claude_launch\n"
    "    cmd, args = resolve_claude_launch('claude', ['--mcp-config', '/tmp/x.json', '--session-id', 's1'])\n"
    "    via = 'omnigent.resolve_claude_launch'\n"
    "except ImportError:\n"
    "    from omnigent_ug_launcher import UgClaudeLauncher\n"
    "    cmd, args = UgClaudeLauncher().launch('claude', ['--mcp-config', '/tmp/x.json', '--session-id', 's1'])\n"
    "    via = 'plugin only (omnigent not installed in this env)'\n"
    "print(json.dumps({'entry_points': eps, 'cmd': cmd, 'args': args, 'via': via}))\n"
)
run = subprocess.run(["uv", "run", "--quiet", "--with-editable", str(ROOT / "omnigent_ug_launcher"),
                      *([] if os.environ.get("NO_OMNIGENT") else ["--with", "omnigent"]),
                      "python", "-c", probe], cwd=ROOT, env=env, capture_output=True, text=True)
if run.returncode != 0:
    record("08_omnigent_launcher_wraps_ug", "❌", f"probe failed: {run.stderr[-300:]}")
else:
    out = json.loads(run.stdout.strip().splitlines()[-1])
    want = ["claude", "--provider", service("sub_a"), "--workspace", host(), "--mcp-config", "/tmp/x.json",
            "--session-id", "s1"]
    good = out["cmd"] in {"ug", "ucode"} and out["args"] == want and "ug" in out["entry_points"]
    full = good and out["via"].startswith("omnigent")
    record("08_omnigent_launcher_wraps_ug", "◑" if good else "❌",
           f"{'Omnigent resolves' if full else 'plugin produces'} {out['cmd']} {' '.join(out['args'][:5])} … with every "
           "Omnigent-injected arg preserved (ucode is ug's legacy name, used when ug is not on PATH). "
           "Live session with a real subscription: runbook R3"
           if good else f"unexpected launch: {out}", out)
