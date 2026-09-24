# Python UDF managed by dbt (a `functions/` resource, dbt 1.11+).
#
# dbt pastes this file, unchanged, into the body of a
#   CREATE OR REPLACE FUNCTION ... (raw STRING) RETURNS STRING LANGUAGE PYTHON
# statement, between dollar-quote delimiters. Two consequences:
#   * the argument declared in _patterns__functions.yml (`raw`) is in scope,
#     and the body ends with a top-level `return`
#   * the file must never contain two consecutive dollar signs, and (because
#     dbt renders it as Jinja first) no Jinja braces either, even in a comment Unity Catalog runs it in a sandbox:
# standard library only, no network.
#
# Why Python and not SQL: alias tables and multi-step string cleanup are a few
# readable lines here and a nested CASE chain in SQL. Call it from SQL with
# the function() Jinja helper, e.g. in audience_members_validated.sql.
ALIASES = {
    "UK": "GB",  # common, but not an ISO 3166-1 code
    "USA": "US",
    "UNITED STATES": "US",
    "CAN": "CA",
    "DEU": "DE",
    "GERMANY": "DE",
    "JPN": "JP",
    "BRA": "BR",
    "MEX": "MX",
    "AUS": "AU",
}


def normalize(value):
    if value is None:
        return None
    cleaned = "".join(ch for ch in value.upper() if ch.isalpha() or ch == " ").strip()
    if not cleaned:
        return None
    return ALIASES.get(cleaned, cleaned)


return normalize(raw)  # noqa: F706, F821 -- the file is a UDF body, not a module
