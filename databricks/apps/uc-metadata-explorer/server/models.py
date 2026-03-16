from pydantic import BaseModel, Field


class TagKV(BaseModel):
    name: str
    value: str


class ColumnMetadata(BaseModel):
    column_name: str
    data_type: str
    comment: str | None = None
    tags: list[TagKV] = Field(default_factory=list)
    description_missing: bool
    tags_missing: bool


class MetadataObjectSummary(BaseModel):
    catalog_name: str
    schema_name: str
    object_name: str
    object_type: str
    owner: str | None = None
    comment: str | None = None
    table_tag_count: int
    column_count: int
    columns_missing_description: int
    columns_missing_tags: int
    table_description_missing: bool
    table_owner_missing: bool
    table_tags_missing: bool
    total_gap_score: int


class MetadataObjectDetail(BaseModel):
    summary: MetadataObjectSummary
    table_tags: list[TagKV] = Field(default_factory=list)
    columns: list[ColumnMetadata] = Field(default_factory=list)


class ProposedChange(BaseModel):
    target_type: str
    target_name: str
    field: str
    current_value: str | None = None
    proposed_value: str
    rationale: str


class GenerateProposalRequest(BaseModel):
    catalog_name: str
    schema_name: str
    object_name: str
    object_type: str = "TABLE"


class GenerateProposalResponse(BaseModel):
    source: str
    changes: list[ProposedChange]


class ConfirmProposalRequest(BaseModel):
    catalog_name: str
    schema_name: str
    object_name: str
    object_type: str = "TABLE"
    changes: list[ProposedChange]
    reviewer_notes: str = ""


class ConfirmProposalResponse(BaseModel):
    proposal_id: int | None = None
    stored: bool
    storage_mode: str
    message: str
