class PolicyEngine:
    def __init__(self, allowed_tools: set[str] | None = None) -> None:
        self._allowed_tools = allowed_tools or {"sql/query", "files/read"}
        self.version = "2026.03.08"

    def authorize_tool(self, tool: str, args: dict) -> tuple[bool, str]:
        if tool not in self._allowed_tools:
            return False, "tool_not_allowlisted"

        if tool == "sql/query" and "statement" not in args:
            return False, "schema_validation_failed"

        return True, "allowed"
