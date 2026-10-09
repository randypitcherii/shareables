{#-
  The ai_decide rubrics. Each macro body is LITERAL JSON: the `questions`
  argument ai_decide applies to every row (a constant, as the function
  requires). tests/test_ai_gateway_improvement.py parses these bodies and
  checks them against ai_decide's contract.

  Do not ask the judge what a rule already knows. Workload type, for
  example, follows from invocation source + client + requester type; an
  early rubric asked ai_decide anyway and it agreed with the rule only 24%
  of the time. Questions here need judgement.

  Rules for editing (the pytest enforces them):
    * score criteria: 2-10 entries, lowest first
    * every choice list has an escape label (`unclear` or `other`)
    * never use `none` as a choice label: ai_decide (Beta) sometimes returns
      an extra probability for it and fails the WHOLE call -- use `no_friction`
    * no apostrophes: the JSON is embedded in a SQL string literal
    * changing a rubric changes rubric_version (an md5 of the JSON), and
      sessions are scored once per version -- so a rubric edit re-scores
      new sessions only, never history, unless you backfill on purpose

  Taxonomy: the content rubric reuses the facet categories from Claude
  Code's /insights report (goal, outcome, friction, session type), so the
  labels are field-tested and familiar to coding-agent users.
-#}

{#- Metadata rubric: state = one session's metrics as JSON. Every session. -#}
{% macro ai_gateway_rubric_metadata() -%}
{
  "token_efficiency": {
    "type": "score",
    "instructions": "Given these AI model session metrics, how efficiently did the session use tokens? Consider uncached input share, prompt cache hit ratio, input growth per turn, retries and errors, and output relative to input. Long agent sessions are normal; judge waste, not size.",
    "criteria": [
      "Very wasteful: most input uncached and repeated, heavy retries or runaway context growth",
      "Wasteful: low cache use or fast context growth with little output",
      "Typical: some avoidable waste",
      "Efficient: good cache use and controlled context",
      "Very efficient: high cache reuse, little waste"
    ]
  },
  "model_fit": {
    "type": "choice",
    "instructions": "Was the model tier a good fit for this workload, judging from request count, token volumes, output size, reasoning effort and model tier?",
    "criteria": {
      "overpowered": "A premium model for short, simple or high-volume mechanical requests that a cheaper tier would handle",
      "right_sized": "The model tier matches the apparent difficulty and volume",
      "underpowered": "An economy model on long, complex, error-prone work",
      "unclear": "Not enough signal to judge"
    }
  },
  "context_pressure": {
    "type": "score",
    "instructions": "How close did this session come to context window limits, judging from max input tokens and input growth per turn?",
    "criteria": [
      "None: small, stable context",
      "Moderate: context grows but stays well below limits",
      "High: very large context, likely compaction or truncation",
      "Severe: near or past typical context limits"
    ]
  },
  "thrash": {
    "type": "noul",
    "instructions": "Does this session show retry loops, repeated errors, rate limiting, or fallback churn rather than productive progress?"
  }
}
{%- endmacro %}


{#- Content rubric: state = the distilled transcript + key metrics.
    Sessions whose model service logs payloads to an inference table. -#}
{% macro ai_gateway_rubric_content() -%}
{
  "goal_category": {
    "type": "choice",
    "instructions": "What did the USER explicitly ask for in this session? Count only what the user requested, not work the assistant decided to do on its own.",
    "criteria": {
      "debug_investigate": "Debug or investigate a problem",
      "implement_feature": "Implement a feature",
      "fix_bug": "Fix a known bug",
      "write_script_tool": "Write a script or tool",
      "refactor_code": "Refactor code",
      "configure_system": "Configure a system or environment",
      "create_pr_commit": "Create a PR or commit",
      "analyze_data": "Analyze data",
      "understand_codebase": "Understand a codebase",
      "write_tests": "Write tests",
      "write_docs": "Write documentation",
      "deploy_infra": "Deploy or manage infrastructure",
      "question_answer": "Ask a general question",
      "content_generation": "Generate or edit non-code content",
      "warmup_minimal": "Warmup, probe, or trivially short session",
      "other": "Something else or unclear"
    }
  },
  "complexity": {
    "type": "score",
    "instructions": "How complex was the task the user asked for?",
    "criteria": [
      "Trivial: a lookup or one-line change",
      "Simple: one small, well-defined change",
      "Moderate: several steps or files",
      "Complex: multi-file design work or tricky debugging",
      "Very complex: multi-system, ambiguous, long-horizon work"
    ]
  },
  "outcome": {
    "type": "choice",
    "instructions": "Did the session achieve what the user asked for?",
    "criteria": {
      "fully_achieved": "The request was completed",
      "mostly_achieved": "Completed with minor gaps",
      "partially_achieved": "Some of the request was completed",
      "not_achieved": "The request was not completed",
      "unclear": "Cannot tell from the transcript"
    }
  },
  "quality": {
    "type": "score",
    "instructions": "How good was the assistant output for the request: correct, relevant, and appropriately scoped?",
    "criteria": [
      "Poor: wrong or harmful",
      "Weak: significant errors or off target",
      "Acceptable: mostly right with notable issues",
      "Good: correct with minor issues",
      "Excellent: correct, focused, and well scoped"
    ]
  },
  "user_frustration": {
    "type": "score",
    "instructions": "How frustrated was the user? Use ONLY explicit user signals such as corrections, complaints, stop requests, or praise.",
    "criteria": [
      "Calm or pleased",
      "Mild: small corrections",
      "Frustrated: repeated corrections or complaints",
      "Very frustrated: gave up, or strong negative language"
    ]
  },
  "primary_friction": {
    "type": "choice",
    "instructions": "What was the main source of friction in this session, if any? Pick the most specific label.",
    "criteria": {
      "no_friction": "No meaningful friction: the user got what they asked for without corrections",
      "misunderstood_request": "The assistant misread what the user wanted",
      "wrong_approach": "Right goal, but a method, pattern, or library the user rejected",
      "buggy_code": "Code the assistant wrote did not work or broke something",
      "user_rejected_action": "The user refused a proposed action or tool call",
      "assistant_got_blocked": "The assistant kept retrying the same failing thing and could not make progress",
      "user_stopped_early": "The user abandoned the task before it was done",
      "wrong_file_or_location": "The assistant edited or targeted the wrong file, path, or location",
      "excessive_changes": "The assistant changed more than asked: rewrote, over-engineered, or touched unrequested code",
      "slow_or_verbose": "The response was too long, too wordy, or too slow",
      "tool_failed": "A tool, command, build, or deploy returned an error",
      "user_unclear": "The initial user request was too vague to act on",
      "external_issue": "A problem outside the assistant and the code: outage, network, quota, or access",
      "other": "Some other friction"
    }
  },
  "prompt_clarity": {
    "type": "score",
    "instructions": "How clear and specific were the user requests: goal, constraints, and context?",
    "criteria": [
      "Vague: goal unclear",
      "Partial: goal clear, key context missing",
      "Clear: goal and main constraints stated",
      "Precise: goal, constraints, context, and done criteria stated"
    ]
  },
  "repeated_instruction": {
    "type": "noul",
    "instructions": "Did the user restate a standing rule, preference, or convention that the assistant should already have known, such as coding style, tools to use, or files to avoid? Such rules belong in an AGENTS.md or CLAUDE.md file."
  },
  "automation_candidate": {
    "type": "noul",
    "instructions": "Is this a repetitive, well-defined workflow that could be captured as a reusable skill, script, or template?"
  },
  "is_novel_friction": {
    "type": "noul",
    "instructions": "Is there a friction in this session that none of these labels describe well: misunderstood request, wrong approach, buggy code, rejected action, blocked, stopped early, wrong file, excessive changes, slow or verbose, tool failure, unclear request, external issue?"
  },
  "sensitive_data_present": {
    "type": "noul",
    "instructions": "Does the transcript contain credentials, secrets, personal data, or customer confidential data?"
  }
}
{%- endmacro %}


{#- rubric JSON -> SQL string literal (no apostrophes allowed; see header) -#}
{% macro ai_gateway_rubric_literal(kind) -%}
{%- set body = ai_gateway_rubric_metadata() if kind == 'metadata' else ai_gateway_rubric_content() -%}
{%- if "'" in body -%}
  {{ exceptions.raise_compiler_error("ai_gateway rubric '" ~ kind ~ "' contains an apostrophe") }}
{%- endif -%}
'{{ body | replace('\n', ' ') }}'
{%- endmacro %}


{#- stable version id: changes iff the rubric text changes -#}
{% macro ai_gateway_rubric_version(kind) -%}
{%- set body = ai_gateway_rubric_metadata() if kind == 'metadata' else ai_gateway_rubric_content() -%}
{{ kind }}_v1_{{ local_md5(body)[:8] }}
{%- endmacro %}


{#- One typed column (+ confidence) per rubric question, read from the
    ai_decide VARIANT. Column names: <prefix>_<question>[_confidence]. -#}
{% macro ai_gateway_answer_columns(kind, decision_col, prefix) -%}
{%- set rubric = fromjson(ai_gateway_rubric_metadata() if kind == 'metadata' else ai_gateway_rubric_content()) -%}
{%- for qid, q in rubric.items() %}
    {%- if q.type == 'choice' %}
    try_variant_get({{ decision_col }}, '$.response.answers.{{ qid }}.choice', 'string') as {{ prefix }}_{{ qid }},
    try_variant_get({{ decision_col }}, '$.response.answers.{{ qid }}.confidence', 'double') as {{ prefix }}_{{ qid }}_confidence,
    {%- elif q.type == 'score' %}
    try_variant_get({{ decision_col }}, '$.response.answers.{{ qid }}.score', 'double') as {{ prefix }}_{{ qid }},
    try_variant_get({{ decision_col }}, '$.response.answers.{{ qid }}.confidence', 'double') as {{ prefix }}_{{ qid }}_confidence,
    {%- else %}
    try_variant_get({{ decision_col }}, '$.response.answers.{{ qid }}.probability', 'double') as {{ prefix }}_{{ qid }},
    {%- endif %}
{%- endfor %}
    try_variant_get({{ decision_col }}, '$.error_message', 'string') as {{ prefix }}_error
{%- endmacro %}


{#- confidence gating: below ai_gateway_min_confidence a choice reads
    `unclear` and a score reads NULL -#}
{% macro ai_gateway_gated_choice(col) -%}
case when {{ col }}_confidence >= {{ var('ai_gateway_min_confidence') }} then {{ col }} when {{ col }} is not null then 'unclear' end
{%- endmacro %}

{% macro ai_gateway_gated_score(col) -%}
case when {{ col }}_confidence >= {{ var('ai_gateway_min_confidence') }} then {{ col }} end
{%- endmacro %}
