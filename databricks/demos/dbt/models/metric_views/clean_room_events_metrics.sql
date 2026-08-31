{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/standalone_facts.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_access', 'clean_room_events') }}
comment: Clean room events (metastore/account scoped standalone fact).
dimensions:
  - name: Event Time
    expr: source.event_time
    comment: Timestamp when the event took place.
  - name: Event Date
    expr: CAST(source.event_time AS DATE)
    comment: Calendar date of the event.
    format:
      type: date
      date_format: year_month_day
  - name: Metastore ID
    expr: source.metastore_id
    comment: The ID of the Unity Catalog metastore.
  - name: Clean Room Name
    expr: source.clean_room_name
    comment: Name of the clean room associated with the event.
  - name: Central Clean Room ID
    expr: source.central_clean_room_id
    comment: The ID of central clean room.
  - name: Event Type
    expr: source.event_type
    comment: The type of the event.
  - name: Initiator Alias
    expr: source.initiator_collaborator_alias
    comment: The alias of the collaborator who initiated the event.
measures:
  - name: Clean Room Events
    expr: COUNT(1)
    comment: Number of clean room events in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Clean Rooms
    expr: COUNT(DISTINCT source.central_clean_room_id)
    comment: Distinct number of clean rooms in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

# ============================================================
# data_classification_metrics  (system.data_classification.results)  [BLOCKED]
# ============================================================
