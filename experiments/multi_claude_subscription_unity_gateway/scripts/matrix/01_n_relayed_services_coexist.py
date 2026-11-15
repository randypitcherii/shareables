"""Row 01 — can one schema hold N relayed Anthropic provider services, one per subscription?

Reads the deployed services back from the Unity Catalog API and checks each is relayed
(`config.anthropic.relayed` present, no stored credential).
"""
import _row  # noqa: F401
from _common import SUBSCRIPTIONS, api, record, schema, section, service

section("01 N relayed services in one schema")
found = {}
for sub in SUBSCRIPTIONS:
    s, b = api("owner", "GET", f"/api/2.1/unity-catalog/model-provider-services/{service(sub)}")
    cfg = b.get("config") or {}
    anth = cfg.get("anthropic") or {}
    found[sub] = {"status": s, "provider_type": cfg.get("provider_type"),
                  "relayed": "relayed" in anth, "stores_credential": "direct" in anth,
                  "inference_table_prefix": (cfg.get("inference_table") or {}).get("table_name_prefix")}
good = all(v["status"] == 200 and v["relayed"] and not v["stores_credential"] for v in found.values())
record("01_n_relayed_services_coexist", "✅" if good else "❌",
       f"{sum(v['relayed'] for v in found.values())}/{len(SUBSCRIPTIONS)} relayed Anthropic services "
       f"side by side in {schema()}, none storing a credential, each with its own inference table",
       found)
