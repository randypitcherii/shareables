from dataclasses import dataclass


@dataclass(frozen=True)
class AliasMapping:
    alias: str
    target_model: str


class AliasRegistry:
    def __init__(self) -> None:
        self._mappings = {
            "claude-sonnet-latest": AliasMapping(
                alias="claude-sonnet-latest",
                target_model="claude-sonnet-4-5-20260215",
            ),
            "claude-opus-latest": AliasMapping(
                alias="claude-opus-latest",
                target_model="claude-opus-4-6-20260201",
            ),
        }

    def resolve(self, alias_or_model: str) -> str:
        mapping = self._mappings.get(alias_or_model)
        if mapping:
            return mapping.target_model
        return alias_or_model
