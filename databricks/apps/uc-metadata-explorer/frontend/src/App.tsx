import { useEffect, useMemo, useState } from "react";
import {
  buildChartsModel,
  groupObjectsByCatalogSchema,
  type ChartDatum,
  type MetadataObjectSummary,
} from "./lib/metadataTransforms";

type TagKV = {
  name: string;
  value: string;
};

type ColumnMetadata = {
  column_name: string;
  data_type: string;
  comment: string | null;
  tags: TagKV[];
  description_missing: boolean;
  tags_missing: boolean;
};

type MetadataObjectDetail = {
  summary: MetadataObjectSummary;
  table_tags: TagKV[];
  columns: ColumnMetadata[];
};

type ProposedChange = {
  target_type: string;
  target_name: string;
  field: string;
  current_value: string | null;
  proposed_value: string;
  rationale: string;
};

type FallbackModalState = {
  visible: boolean;
  reason: string;
  reasonCode: string;
  detail: string;
};

type ContentTab = "details" | "charts";

const apiBase = "/api/v1";

const EMPTY_FALLBACK_MODAL: FallbackModalState = {
  visible: false,
  reason: "",
  reasonCode: "",
  detail: "",
};

function selectedChangeKey(change: ProposedChange, index: number): string {
  return `${index}:${change.target_name}:${change.field}`;
}

function isSameObject(left: MetadataObjectSummary, right: MetadataObjectSummary): boolean {
  return (
    left.catalog_name === right.catalog_name &&
    left.schema_name === right.schema_name &&
    left.object_name === right.object_name
  );
}

function buildObjectListParams({
  search,
  gapOnly,
  useLocalCache,
  useFallback,
}: {
  search: string;
  gapOnly: boolean;
  useLocalCache: boolean;
  useFallback: boolean;
}): URLSearchParams {
  const params = new URLSearchParams({
    search,
    gap_only: String(gapOnly),
    limit: "250",
  });
  if (useLocalCache) {
    params.set("source", "cache");
  }
  if (useFallback) {
    params.set("fallback", "true");
  }
  return params;
}

async function readErrorDetail(response: Response): Promise<string> {
  try {
    const payload = (await response.json()) as { detail?: string };
    return payload.detail ?? "";
  } catch {
    return `HTTP ${response.status}`;
  }
}

async function assertOk(response: Response, fallbackMessage: string): Promise<void> {
  if (response.ok) return;
  let detail = "";
  try {
    const payload = (await response.json()) as { detail?: string };
    detail = payload.detail ?? "";
  } catch {
    // Ignore parse errors and keep fallback status message.
  }
  const suffix = detail ? `: ${detail}` : `: ${response.status}`;
  throw new Error(`${fallbackMessage}${suffix}`);
}

function FallbackModal({
  state,
  onRetryFallback,
  onDismiss,
}: {
  state: FallbackModalState;
  onRetryFallback: () => void;
  onDismiss: () => void;
}) {
  if (!state.visible) return null;

  const reasonMessages: Record<string, string> = {
    timeout: "The SQL warehouse query timed out. The warehouse may be cold, overloaded, or unreachable.",
    warehouse_unavailable: "The SQL warehouse is not running or unavailable.",
    warehouse_not_configured: "No SQL warehouse is configured for primary metadata loading.",
    sql_listing_disabled: "SQL warehouse listing is disabled in the configuration.",
    query_error: "An unexpected error occurred while querying the SQL warehouse.",
  };

  const friendlyReason = reasonMessages[state.reasonCode] || state.detail;

  return (
    <div className="modal-overlay" onClick={onDismiss}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>Primary Metadata Loading Failed</h2>
        </div>
        <div className="modal-body">
          <p className="modal-reason">{friendlyReason}</p>
          <p className="modal-explanation">
            The primary metadata loading method uses SQL warehouse queries for fast, comprehensive results.
            When unavailable, you can use the slower fallback approach that queries Unity Catalog APIs directly.
          </p>
          <div className="modal-info-box">
            <strong>Fallback Mode:</strong>
            <ul>
              <li>Uses Unity Catalog REST APIs instead of SQL warehouse</li>
              <li>Slower but more reliable when warehouse is unavailable</li>
              <li>May return limited metadata (no tag counts or column-level gaps)</li>
            </ul>
          </div>
        </div>
        <div className="modal-actions">
          <button className="modal-button modal-button-primary" onClick={onRetryFallback}>
            Use Fallback Mode
          </button>
          <button className="modal-button modal-button-secondary" onClick={onDismiss}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

function HorizontalBarChart({ data }: { data: ChartDatum[] }) {
  const maxValue = Math.max(...data.map((item) => item.value), 1);

  if (data.length === 0) {
    return <div className="empty-state">No data available for this chart.</div>;
  }

  return (
    <div className="chart-bars">
      {data.map((item) => (
        <div key={item.label} className="chart-row">
          <div className="chart-label" title={item.label}>
            {item.label}
          </div>
          <div className="chart-track">
            <div className="chart-fill" style={{ width: `${(item.value / maxValue) * 100}%` }} />
          </div>
          <div className="chart-value">{item.value}</div>
        </div>
      ))}
    </div>
  );
}

function ChartPanel({ title, data }: { title: string; data: ChartDatum[] }) {
  return (
    <section className="panel chart-panel">
      <h3>{title}</h3>
      <HorizontalBarChart data={data} />
    </section>
  );
}

function App() {
  const [objects, setObjects] = useState<MetadataObjectSummary[]>([]);
  const [selected, setSelected] = useState<MetadataObjectSummary | null>(null);
  const [detail, setDetail] = useState<MetadataObjectDetail | null>(null);
  const [changes, setChanges] = useState<ProposedChange[]>([]);
  const [selectedChangeKeys, setSelectedChangeKeys] = useState<Record<string, boolean>>({});
  const [search, setSearch] = useState("");
  const [gapOnly, setGapOnly] = useState(true);
  const [loadingObjects, setLoadingObjects] = useState(false);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [saving, setSaving] = useState(false);
  const [proposalSource, setProposalSource] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [reviewerNotes, setReviewerNotes] = useState("");
  const [fallbackModal, setFallbackModal] = useState<FallbackModalState>(EMPTY_FALLBACK_MODAL);
  const [useFallback, setUseFallback] = useState(false);
  const [useLocalCache, setUseLocalCache] = useState(false);
  const [activeTab, setActiveTab] = useState<ContentTab>("details");

  const groupedObjects = useMemo(() => groupObjectsByCatalogSchema(objects), [objects]);
  const charts = useMemo(() => buildChartsModel(objects), [objects]);

  const selectedChanges = useMemo(
    () => changes.filter((change, index) => selectedChangeKeys[selectedChangeKey(change, index)]),
    [changes, selectedChangeKeys],
  );

  const loadObjects = async (forceFallback = false) => {
    setLoadingObjects(true);
    setError("");
    try {
      const params = buildObjectListParams({
        search,
        gapOnly,
        useLocalCache,
        useFallback: forceFallback || useFallback,
      });
      const response = await fetch(`${apiBase}/metadata/objects?${params.toString()}`);

      if (!response.ok) {
        const primaryFailed = response.headers.get("x-uc-metadata-primary-failed") === "true";
        const fallbackAvailable = response.headers.get("x-uc-metadata-fallback-available") === "true";
        const reasonCode = response.headers.get("x-uc-metadata-primary-reason") || "query_error";

        if (primaryFailed && fallbackAvailable && !forceFallback && !useFallback) {
          const detailMessage = await readErrorDetail(response);
          setFallbackModal({ visible: true, reason: detailMessage, reasonCode, detail: detailMessage });
          setLoadingObjects(false);
          return;
        }
      }

      await assertOk(response, "Failed to load objects");
      const data = (await response.json()) as MetadataObjectSummary[];
      setObjects(data);
      if (data.length > 0) {
        const hasSelected = selected ? data.some((item) => isSameObject(item, selected)) : false;
        setSelected(hasSelected && selected ? selected : data[0]);
      } else {
        setSelected(null);
        setDetail(null);
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoadingObjects(false);
    }
  };

  const loadDetail = async (objectSummary: MetadataObjectSummary) => {
    setLoadingDetail(true);
    setError("");
    setMessage("");
    setChanges([]);
    setSelectedChangeKeys({});
    setProposalSource("");
    try {
      const path = `${apiBase}/metadata/objects/${encodeURIComponent(objectSummary.catalog_name)}/${encodeURIComponent(objectSummary.schema_name)}/${encodeURIComponent(objectSummary.object_name)}`;
      const detailParams = new URLSearchParams();
      if (useLocalCache) {
        detailParams.set("source", "cache");
      }
      const detailUrl = detailParams.size > 0 ? `${path}?${detailParams.toString()}` : path;
      const response = await fetch(detailUrl);
      await assertOk(response, "Failed to load detail");
      setDetail((await response.json()) as MetadataObjectDetail);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoadingDetail(false);
    }
  };

  useEffect(() => {
    void loadObjects(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!selected) return;
    void loadDetail(selected);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected, useLocalCache]);

  const onGenerateProposals = async () => {
    if (!selected) return;
    setGenerating(true);
    setError("");
    setMessage("");
    try {
      const response = await fetch(`${apiBase}/proposals/generate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          catalog_name: selected.catalog_name,
          schema_name: selected.schema_name,
          object_name: selected.object_name,
          object_type: selected.object_type,
        }),
      });
      await assertOk(response, "Failed to generate proposals");
      const data = (await response.json()) as { source: string; changes: ProposedChange[] };
      setProposalSource(data.source);
      setChanges(data.changes);
      const defaults: Record<string, boolean> = {};
      data.changes.forEach((change, index) => {
        defaults[selectedChangeKey(change, index)] = true;
      });
      setSelectedChangeKeys(defaults);
      setMessage(`Generated ${data.changes.length} proposal(s) from ${data.source}.`);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setGenerating(false);
    }
  };

  const onConfirmSelected = async () => {
    if (!selected || selectedChanges.length === 0) return;
    setSaving(true);
    setError("");
    setMessage("");
    try {
      const response = await fetch(`${apiBase}/proposals/confirm`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          catalog_name: selected.catalog_name,
          schema_name: selected.schema_name,
          object_name: selected.object_name,
          object_type: selected.object_type,
          changes: selectedChanges,
          reviewer_notes: reviewerNotes,
        }),
      });
      await assertOk(response, "Failed to confirm proposals");
      const data = (await response.json()) as { message: string; stored: boolean; proposal_id: number | null };
      setMessage(data.proposal_id ? `${data.message} Proposal ID: ${data.proposal_id}` : data.message);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const gapBadge = (objectSummary: MetadataObjectSummary) => {
    if (objectSummary.total_gap_score > 10) return "critical";
    if (objectSummary.total_gap_score > 3) return "warning";
    return "good";
  };

  const handleRetryFallback = () => {
    setFallbackModal(EMPTY_FALLBACK_MODAL);
    setUseFallback(true);
    void loadObjects(true);
  };

  const handleDismissModal = () => {
    setFallbackModal(EMPTY_FALLBACK_MODAL);
  };

  return (
    <>
      <FallbackModal state={fallbackModal} onRetryFallback={handleRetryFallback} onDismiss={handleDismissModal} />
      <div className="page">
        <header className="topbar">
          <div>
            <h1>Unity Catalog Metadata Command Space</h1>
            <p>Inspect metadata quality gaps and stage AI-generated updates before persisting to Lakebase.</p>
          </div>
          <div className="topbar-actions">
            <button onClick={() => void loadObjects(false)} disabled={loadingObjects}>
              {loadingObjects ? "Refreshing..." : "Refresh"}
            </button>
            <button onClick={onGenerateProposals} disabled={!selected || generating || loadingDetail}>
              {generating ? "Generating..." : "Generate Missing Metadata"}
            </button>
            <button onClick={onConfirmSelected} disabled={saving || selectedChanges.length === 0}>
              {saving ? "Saving..." : `Confirm ${selectedChanges.length} Selected`}
            </button>
          </div>
        </header>

        <section className="status-strip">
          {message && <span className="status success">{message}</span>}
          {error && <span className="status error">{error}</span>}
          {proposalSource && <span className="status info">Proposal source: {proposalSource}</span>}
        </section>

        <div className="layout">
          <aside className="sidebar">
            <div className="filters">
              <input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Search catalog.schema.table"
              />
              <label>
                <input
                  type="checkbox"
                  checked={gapOnly}
                  onChange={(event) => setGapOnly(event.target.checked)}
                />
                Show only objects with gaps
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={useLocalCache}
                  onChange={(event) => {
                    setUseLocalCache(event.target.checked);
                    setUseFallback(false);
                  }}
                />
                Use local SQLite cache (faster dev loop)
              </label>
              <button onClick={() => void loadObjects(false)} disabled={loadingObjects}>
                Apply Filters
              </button>
            </div>
            <div className="object-list grouped-nav">
              {groupedObjects.map((catalog) => (
                <div className="group-catalog" key={catalog.catalogName}>
                  <div className="group-catalog-title">
                    <strong>{catalog.catalogName}</strong>
                    <span>{catalog.tableCount} tables</span>
                  </div>
                  {catalog.schemas.map((schema) => (
                    <div className="group-schema" key={`${catalog.catalogName}.${schema.schemaName}`}>
                      <div className="group-schema-title">
                        {schema.schemaName}
                        <span>{schema.tableCount}</span>
                      </div>
                      {schema.tables.map((item) => (
                        <button
                          key={`${item.catalog_name}.${item.schema_name}.${item.object_name}`}
                          className={`object-item ${
                            selected?.object_name === item.object_name &&
                            selected?.schema_name === item.schema_name &&
                            selected?.catalog_name === item.catalog_name
                              ? "active"
                              : ""
                          }`}
                          onClick={() => setSelected(item)}
                        >
                          <div className="object-title">
                            <span>{item.object_name}</span>
                            <span className={`pill ${gapBadge(item)}`}>gap {item.total_gap_score}</span>
                          </div>
                          <div className="object-metrics">
                            <span>cols {item.column_count}</span>
                            <span>desc miss {item.columns_missing_description}</span>
                            <span>tag miss {item.columns_missing_tags}</span>
                          </div>
                        </button>
                      ))}
                    </div>
                  ))}
                </div>
              ))}
            </div>
          </aside>

          <main className="content">
            <section className="tabs" role="tablist" aria-label="Metadata views">
              <button
                className={`tab-button ${activeTab === "details" ? "active" : ""}`}
                role="tab"
                aria-selected={activeTab === "details"}
                onClick={() => setActiveTab("details")}
              >
                Details
              </button>
              <button
                className={`tab-button ${activeTab === "charts" ? "active" : ""}`}
                role="tab"
                aria-selected={activeTab === "charts"}
                onClick={() => setActiveTab("charts")}
              >
                Charts
              </button>
            </section>

            {activeTab === "charts" ? (
              <div className="charts-grid">
                <ChartPanel title="Objects by Catalog" data={charts.objectsByCatalog} />
                <ChartPanel title="Objects by Schema" data={charts.objectsBySchema.slice(0, 10)} />
                <ChartPanel title="Gap Score Distribution" data={charts.gapScoreBuckets} />
                <ChartPanel title="Table Metadata Gaps" data={charts.tableMetadataGaps} />
                <ChartPanel title="Top Tables by Gap Score" data={charts.topTablesByGapScore} />
                <ChartPanel title="Column Gap Totals" data={charts.columnGapTotals} />
              </div>
            ) : !detail || loadingDetail ? (
              <div className="empty-state">{loadingDetail ? "Loading object details..." : "Select an object."}</div>
            ) : (
              <>
                <section className="panel">
                  <h2>
                    {detail.summary.catalog_name}.{detail.summary.schema_name}.{detail.summary.object_name}
                  </h2>
                  <div className="table-meta-grid">
                    <div>
                      <strong>Type</strong>
                      <div>{detail.summary.object_type}</div>
                    </div>
                    <div>
                      <strong>Owner</strong>
                      <div>{detail.summary.owner || "Missing owner"}</div>
                    </div>
                    <div>
                      <strong>Description</strong>
                      <div>{detail.summary.comment || "Missing description"}</div>
                    </div>
                    <div>
                      <strong>Table Tags</strong>
                      <div>
                        {detail.table_tags.length > 0
                          ? detail.table_tags.map((tag) => `${tag.name}=${tag.value}`).join(", ")
                          : "Missing tags"}
                      </div>
                    </div>
                  </div>
                </section>

                <section className="panel">
                  <h3>Columns</h3>
                  <table>
                    <thead>
                      <tr>
                        <th>Column</th>
                        <th>Type</th>
                        <th>Description</th>
                        <th>Tags</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.columns.map((column) => (
                        <tr key={column.column_name}>
                          <td>{column.column_name}</td>
                          <td>{column.data_type}</td>
                          <td className={column.description_missing ? "missing" : ""}>
                            {column.comment || "Missing description"}
                          </td>
                          <td className={column.tags_missing ? "missing" : ""}>
                            {column.tags.length > 0
                              ? column.tags.map((tag) => `${tag.name}=${tag.value}`).join(", ")
                              : "Missing tags"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </section>

                <section className="panel">
                  <h3>Proposed Changes</h3>
                  <textarea
                    className="notes"
                    value={reviewerNotes}
                    onChange={(event) => setReviewerNotes(event.target.value)}
                    placeholder="Reviewer notes (optional)"
                  />
                  {changes.length === 0 ? (
                    <div className="empty-state">Generate proposals to review AI-suggested metadata updates.</div>
                  ) : (
                    <div className="change-list">
                      {changes.map((change, index) => {
                        const key = `${index}:${change.target_name}:${change.field}`;
                        const checked = !!selectedChangeKeys[key];
                        return (
                          <div className="change-item" key={key}>
                            <label className="checkline">
                              <input
                                type="checkbox"
                                checked={checked}
                                onChange={(event) =>
                                  setSelectedChangeKeys((previous) => ({
                                    ...previous,
                                    [key]: event.target.checked,
                                  }))
                                }
                              />
                              <span>
                                {change.target_type}.{change.target_name} - {change.field}
                              </span>
                            </label>
                            <div className="change-fields">
                              <div>
                                <strong>Current</strong>
                                <div>{change.current_value || "Missing"}</div>
                              </div>
                              <div>
                                <strong>Proposed</strong>
                                <textarea
                                  value={change.proposed_value}
                                  onChange={(event) =>
                                    setChanges((previous) =>
                                      previous.map((item, itemIndex) =>
                                        itemIndex === index
                                          ? { ...item, proposed_value: event.target.value }
                                          : item,
                                      ),
                                    )
                                  }
                                />
                              </div>
                            </div>
                            <p>{change.rationale}</p>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </section>
              </>
            )}
          </main>
        </div>
      </div>
    </>
  );
}

export default App;
