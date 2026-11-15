"""Row 05 — can a relayed service be pinned to one Anthropic organization?

If it could, the gateway would reject a subscription-B token on subscription A's service. The
relayed config has no fields at all (`ModelProviderServiceConfigAnthropicProviderRelayedConfig`
is an empty object with additionalProperties: false). This row asks the live API to accept an
organization field and records what it does. Creates nothing that survives: a service that
does get created is deleted at once.
"""
import _row  # noqa: F401
from _common import api, record, schema, section

section("05 relayed service has no Anthropic-organization binding")
name = "claude_org_binding_probe"
fq = f"{schema()}.{name}"
base = f"/api/2.1/unity-catalog/model-provider-services?model_provider_service_id={name}&parent=schemas/{schema()}"
attempts = {}
# "control" proves the create shape works, so a missing field below is the API dropping it.
for field in ("control", "organization_id", "anthropic_organization_id", "allowed_organizations"):
    relayed = {} if field == "control" else {field: "00000000-0000-0000-0000-000000000000"}
    body = {"config": {"provider_type": "EXTERNAL_MODEL_PROVIDER_TYPE_ANTHROPIC",
                       "anthropic": {"relayed": relayed}, "allow_all_targets": True}}
    s, b = api("owner", "POST", base, body)
    kept = None
    if s == 200:
        kept = field in ((b.get("config") or {}).get("anthropic") or {}).get("relayed", {})
    api("owner", "DELETE", f"/api/2.1/unity-catalog/model-provider-services/{fq}")
    attempts[field] = {"status": s, "field_kept": kept, "message": str(b.get("message", ""))[:160]}
control_ok = attempts["control"]["status"] == 200
bound = any(a["field_kept"] for k, a in attempts.items() if k != "control")
if not control_ok:
    record("05_no_org_binding_on_service", "❓", f"control create failed, nothing isolated: {attempts['control']}",
           attempts)
else:
    record("05_no_org_binding_on_service", "❌" if not bound else "◑",
           "no binding: create accepts each organization field with HTTP 200 and silently drops it "
           "(stored config is relayed: {}). A relayed service relays ANY subscription token; group grants "
           "(row 04) plus per-service usage (row 07) are the control. Confirm with a real "
           "cross-subscription call (runbook R2)" if not bound else f"an org field was stored: {attempts}",
           attempts)
