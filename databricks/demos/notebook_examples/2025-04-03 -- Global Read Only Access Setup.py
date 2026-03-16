#!/usr/bin/env python
# coding: utf-8

# # Global Read Only Access Setup
# 
# This script creates a specified group (if it doesn't exist) and grants it read-only/view/run/attach permissions
# on various Databricks Workspace and Unity Catalog objects.
#
# Workspace Permissions Applied via Permissions API:
# - Clusters: CAN_ATTACH_TO
# - Jobs: CAN_VIEW
# - SQL Warehouses: CAN_VIEW
# - Instance Pools: CAN_ATTACH_TO
# - Dashboards (Lakeview): CAN_RUN
# - Alerts: CAN_RUN
# - Pipelines (DLT): CAN_VIEW
#
# Workspace Permissions Applied via SQL GRANT:
# - SQL Queries: CAN_VIEW
#
# Unity Catalog Permissions Applied via SQL GRANT:
# - CATALOG: USE CATALOG, USE SCHEMA, SELECT, READ VOLUME, BROWSE
#
# Intentionally Skipped Objects:
# - Root Directory ('/'): Permissions API does not support.
# - Experiments, Registered Models, Serving Endpoints: Permissions API incompatible or not supported.
# - Secrets, Vector Search Endpoints: Skipped by choice.
# - Individual Files/Folders/Notebooks: Skipped for performance; rely on future folder permissions or manual grants.

import time
import pandas as pd
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

# Parameters - Adjust these as needed
GROUP_NAME = "GLOBAL_READ_ONLY_GROUP"
DRY_RUN = False  # Set to False to apply changes
VERBOSE = False  # True = log each grant/failure; False = log per-type summaries.

# Initialize the Databricks workspace client
w = WorkspaceClient()

# Collection to track successful and failed permissions
permission_results = []

def apply_permission(obj_type: str, obj_id: str, permission_level: str, display_name: str) -> bool:
    """Apply a permission to an object.
    
    Args:
        obj_type: The type of object (e.g., "clusters", "directories")
        obj_id: The ID of the object
        permission_level: The permission level to grant
        display_name: The display name of the object for logging
    
    Returns:
        bool: True if successful, False otherwise
    """
    if DRY_RUN:
        if VERBOSE:
            print(f"  DRY RUN: Would grant {permission_level} on {obj_type} {display_name}")
        # Record dry run result
        permission_results.append({
            "type": obj_type,
            "name": display_name,
            "permission": permission_level,
            "status": "dry_run_success", # Indicate dry run
            "error": None,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        return True
    
    try:
        # Map string permission levels to PermissionLevel enum values
        permission_level_enum = getattr(iam.PermissionLevel, permission_level)
        
        # Update the permissions using the correct parameter structure
        w.permissions.update(
            request_object_type=obj_type,
            request_object_id=obj_id,
            access_control_list=[iam.AccessControlRequest(
                group_name=GROUP_NAME,
                permission_level=permission_level_enum
            )]
        )
        
        if VERBOSE:
            print(f"  ✅ Granted {permission_level} on {obj_type} {display_name}")
        
        permission_results.append({
            "type": obj_type,
            "name": display_name,
            "permission": permission_level,
            "status": "success",
            "error": None,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        return True
    except Exception as e:
        error_msg = str(e)
        permission_results.append({
            "type": obj_type,
            "name": display_name,
            "permission": permission_level,
            "status": "failed",
            "error": error_msg,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        if VERBOSE:
            print(f"  ❌ Error granting {permission_level} to {obj_type} {display_name}: {error_msg}")
        return False

def ensure_group_exists():
    """Ensure the global read-only group exists, creating it if necessary."""
    try:
        # Check if group exists
        existing_groups = w.groups.list()
        group_exists = any(group.display_name == GROUP_NAME for group in existing_groups)
        
        if not group_exists:
            if DRY_RUN:
                print(f"Would create group: {GROUP_NAME}")
            else:
                w.groups.create(display_name=GROUP_NAME)
                print(f"Created group: {GROUP_NAME}")
                # Wait for group propagation
                print(f"Waiting 120 seconds for group propagation...")
                time.sleep(120)
        else:
            print(f"Group {GROUP_NAME} already exists")
        
        return True
    except Exception as e:
        print(f"❌ Error creating/verifying group {GROUP_NAME}: {str(e)}")
        return False

def process_workspace_permissions():
    """Process workspace permissions to apply read-only access to objects."""
    print("Processing workspace permissions...")
    workspace_objects = {} # Dictionary to store objects by type
    # Define object types, list functions, ID/name attributes, and permissions
    # Note: Some might require specific APIs or might not be listable/permissible this way.
    object_types_to_process = {
        "clusters": {"list_func": w.clusters.list, "id_attr": "cluster_id", "name_attr": "cluster_name", "permission": "CAN_ATTACH_TO"},
        "jobs": {"list_func": w.jobs.list, "id_attr": "job_id", "name_attr": "settings.name", "permission": "CAN_VIEW"},
        "sql/warehouses": {"list_func": w.warehouses.list, "id_attr": "id", "name_attr": "name", "permission": "CAN_VIEW"},
        "pools": {"list_func": w.instance_pools.list, "id_attr": "instance_pool_id", "name_attr": "instance_pool_name", "permission": "CAN_ATTACH_TO"},
        "dashboards": {"list_func": w.lakeview.list, "id_attr": "dashboard_id", "name_attr": "display_name", "permission": "CAN_RUN"}, # Lakeview dashboards
        "alerts": {"list_func": w.alerts.list, "id_attr": "id", "name_attr": "name", "permission": "CAN_RUN"},
        # Removed: experiments, registered_models, serving_endpoints due to Permissions API incompatibility.
        # Removed: directories (root) as Permissions API does not support it.
    }

    # --- Process other individual object types ---

    # Add queries separately due to potential listing errors
    try:
        workspace_objects["sql/queries"] = list(w.queries.list())
        queries = workspace_objects["sql/queries"]
        num_queries = len(queries)
        if num_queries > 0:
            query_success_count = 0
            query_failed_count = 0

            # Get a warehouse for execution
            warehouses = list(w.warehouses.list())
            if not warehouses:
                print(f"🔎 SQL Queries:\n    ❌ Skipped {num_queries} grants: No SQL warehouse available for execution.")
            else:
                warehouse_id = warehouses[0].id
                for q in queries:
                    query_id = q.id
                    query_name = q.display_name or f"Query {query_id}" # Use display_name or fallback to ID
                    sql_query = f"GRANT CAN VIEW ON QUERY {query_id} TO `{GROUP_NAME}`"
                    grant_status = "failed"
                    error_msg = None
                    try:
                        if DRY_RUN:
                            if VERBOSE:
                                print(f"  DRY RUN: Would execute: {sql_query}")
                            grant_status = "dry_run_success"
                            query_success_count += 1
                        else:
                            w.statement_execution.execute_statement(
                                warehouse_id=warehouse_id,
                                statement=sql_query,
                                wait_timeout=None,
                            )
                            grant_status = "success"
                            query_success_count += 1
                            if VERBOSE:
                                print(f"    ✅ Granted CAN_VIEW on query {query_name} ({query_id})")
                    except Exception as e:
                        error_msg = str(e)
                        query_failed_count += 1
                        if VERBOSE:
                             print(f"    ❌ Error granting CAN_VIEW on query {query_name} ({query_id}): {error_msg}")

                    permission_results.append({
                        "type": "sql_query_grant", # Distinct type for summary
                        "name": query_name,
                        "permission": "CAN_VIEW",
                        "status": grant_status,
                        "error": error_msg,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })

            if not VERBOSE and num_queries > 0:
                status_icon = "✅" if query_failed_count == 0 else "❌"
                dry_run_msg = " (DRY RUN)" if DRY_RUN and query_success_count > 0 else ""
                if not warehouses:
                    print(f"🔎 SQL Queries:\n    ❌ Skipped {num_queries} grants: No SQL warehouse available for execution.")
                elif query_failed_count == 0:
                    print(f"🔎 SQL Queries:\n    {status_icon} Applied CAN_VIEW via GRANT to {query_success_count}/{num_queries} queries successfully{dry_run_msg}.")
                else:
                    print(f"🔎 SQL Queries:\n    {status_icon} Applied CAN_VIEW via GRANT to {query_success_count}/{num_queries} queries.{dry_run_msg} {query_failed_count} failed.")
                print() # Add whitespace after summary
        else:
             if not VERBOSE:
                 print(f"🔎 SQL Queries:\n    ✅ No objects found.")
                 print() # Add whitespace after summary
    except Exception as e:
        # Log error if listing itself failed, only in non-verbose mode
        if not VERBOSE:
             print(f"🔎 SQL Queries:\n    ⚠️ Error listing queries, skipping: {str(e)}")

    # Add pipelines separately due to potential listing errors
    try:
        workspace_objects["pipelines"] = list(w.pipelines.list_pipelines())
        object_types_to_process["pipelines"] = {"list_func": None, "id_attr": "pipeline_id", "name_attr": "name", "permission": "CAN_VIEW"} # Use stored list
    except Exception as e:
        print(f"  ⚠️ Error listing pipelines, skipping: {str(e)}")

    # Populate workspace_objects for other types
    for obj_type, config in object_types_to_process.items():
        # Skip listing for types handled separately (queries, pipelines)
        if obj_type in ["sql/queries", "pipelines"]:
            continue

        if config.get("list_func"): # Check if list_func exists
            try:
                workspace_objects[obj_type] = list(config["list_func"]())
            except Exception as e:
                if not VERBOSE:
                     # Capitalize obj_type for display
                     obj_type_display = ' '.join(word.capitalize() for word in obj_type.replace("sql/", "SQL ").split('_'))
                     print(f"🔎 {obj_type_display}:\n    ⚠️ Error listing objects, skipping: {str(e)}")
                # Remove from processing if listing failed
                if obj_type in workspace_objects: del workspace_objects[obj_type]
                if obj_type in object_types_to_process: del object_types_to_process[obj_type]

    # Process permissions per object type
    total_success_count = 0
    total_failed_count = 0

    for obj_type, config in object_types_to_process.items():
        # Skip queries here as they are handled separately above via SQL GRANTs
        if obj_type == "sql/queries":
            continue

        objects = workspace_objects.get(obj_type, [])
        if not objects:
            if not VERBOSE: # Only print summary if not verbose and objects were expected but not found/listed
                 # Capitalize obj_type for display
                 obj_type_display = ' '.join(word.capitalize() for word in obj_type.replace("sql/", "SQL ").split('_'))
                 print(f"🔎 {obj_type_display}:\n    ✅ No objects found or listing failed.")
                 print() # Add whitespace after summary
            continue # Skip if no objects or listing failed previously

        type_success_count = 0
        type_failed_count = 0
        permission = config["permission"]

        if not VERBOSE:
            pass # Removed intermediate print statement

        for obj in objects:
            # Handle potential nested attributes for name_attr (like job.settings.name)
            name = obj
            for attr in config["name_attr"].split('.'):
                name = getattr(name, attr, '[Name Not Found]')

            obj_id = str(getattr(obj, config["id_attr"]))

            # Verbose logging happens inside apply_permission
            if apply_permission(obj_type, obj_id, permission, name):
                type_success_count += 1
            else:
                type_failed_count += 1

        if not VERBOSE:
            status_icon = "✅" if type_failed_count == 0 else "❌"
            dry_run_msg = " (DRY RUN)" if DRY_RUN and (type_success_count > 0 or type_failed_count > 0) else "" # Show dry run if any attempt was made
            # Capitalize obj_type for display
            obj_type_display = ' '.join(word.capitalize() for word in obj_type.replace("sql/", "SQL ").split('_'))
            if type_failed_count == 0:
                # Handle case where len(objects) might be 0 if listing failed but wasn't caught above
                total_objects = len(objects) if objects else type_success_count + type_failed_count
                print(f"🔎 {obj_type_display}:\n    {status_icon} Applied {permission} to {type_success_count}/{total_objects} objects successfully{dry_run_msg}.")
            else:
                # Handle case where len(objects) might be 0 if listing failed but wasn't caught above
                total_objects = len(objects) if objects else type_success_count + type_failed_count
                print(f"🔎 {obj_type_display}:\n    {status_icon} Applied {permission} to {type_success_count}/{total_objects} objects.{dry_run_msg} {type_failed_count} failed.")
            print() # Add whitespace after summary

        total_success_count += type_success_count
        total_failed_count += type_failed_count

    # Final summary print moved to display_summary
    # Return values are not strictly needed as results are in permission_results
    # return total_success_count, total_failed_count # Keep for compatibility if needed elsewhere

def process_unity_catalog_permissions():
    """Process Unity Catalog permissions to apply read-only access to catalogs."""
    print("Processing Unity Catalog permissions...")
    
    # Get all catalogs - convert the generator to a list first
    try:
        catalogs = list(w.catalogs.list())
        num_catalogs = len(catalogs)
        if len(catalogs) == 0:
            if not VERBOSE:
                print(f"🔎 Unity Catalog (All Catalogs):\n    ✅ No catalogs found.")
                print() # Add whitespace after summary
            return # Nothing to do

        print(f"  Found {num_catalogs} catalogs.")
        permissions_to_grant_uc = [
            "USE CATALOG", "USE SCHEMA", "SELECT", "READ VOLUME", "BROWSE"
        ]
        # Dictionary to track success/failure per permission type for non-verbose summary
        uc_summary = {p: {"success": 0, "failed": 0, "total": num_catalogs} for p in permissions_to_grant_uc}
        uc_overall_failed_listing = False

        for cat in catalogs:
            catalog_name = cat.name
            if VERBOSE:
                 print(f"  Processing catalog: {catalog_name}")

            # Define permissions to grant for each catalog
            for permission in permissions_to_grant_uc:
                try:
                    if DRY_RUN:
                        if VERBOSE:
                            print(f"    Would grant {permission} on catalog {catalog_name}")
                        uc_summary[permission]["success"] += 1
                        # Log dry run success in main results list
                        permission_results.append({
                            "type": "unity_catalog", "name": catalog_name,
                            "permission": permission, "status": "dry_run_success",
                            "error": None, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                    else:
                        # Get the first warehouse for SQL execution
                        warehouses = list(w.warehouses.list())
                        if not warehouses:
                            print("    ❌ No warehouse available for SQL execution")
                            # Mark failure for this permission type for summary
                            uc_summary[permission]["failed"] += 1
                            # Log specific failure
                            permission_results.append({
                                "type": "unity_catalog", "name": catalog_name,
                                "permission": permission, "status": "failed",
                                "error": "No warehouse available for SQL execution",
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            })
                            continue

                        sql_query = f"""
                        GRANT {permission} ON CATALOG `{catalog_name}` TO `{GROUP_NAME}`
                        """
                        grant_success = False
                        try:
                            w.statement_execution.execute_statement(
                                warehouse_id=warehouses[0].id,
                                statement=sql_query,
                                wait_timeout=None,
                            )
                            grant_success = True
                            if VERBOSE:
                                print(f"    ✅ Granted {permission} on catalog {catalog_name}")
                            
                            # Update counts
                            uc_summary[permission]["success"] += 1

                            # Add to permission results for summary
                            permission_results.append({
                                "type": "unity_catalog",
                                "name": catalog_name,
                                "permission": permission,
                                "status": "success",
                                "error": None,
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            })
                        except Exception as e:
                            error_msg = str(e)
                            if VERBOSE: # Only print individual errors if verbose
                                print(f"    ❌ Error granting {permission} to catalog {catalog_name}: {error_msg}")
                            uc_summary[permission]["failed"] += 1

                            # Add to permission results for summary
                            permission_results.append({
                                "type": "unity_catalog",
                                "name": catalog_name,
                                "permission": permission,
                                "status": "failed",
                                "error": error_msg,
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            })
                except Exception as e:
                    error_msg = str(e)
                    # This error is less common, likely during setup/warehouse check
                    print(f"    ❌ Error processing permission {permission} for catalog {catalog_name}: {error_msg}")
                    uc_summary[permission]["failed"] += 1 # Count failure against this permission type
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Error listing catalogs: {error_msg}")
        uc_overall_failed_listing = True # Indicate catalog listing failed

    # Print non-verbose summary if VERBOSE is False
    if not VERBOSE:
        print(f"🔎 Unity Catalog (All Catalogs):")
        if uc_overall_failed_listing:
             print("    ❌ Failed to list catalogs.")
        elif num_catalogs == 0:
             print("    ✅ No catalogs found.")
        else:
            for permission, counts in uc_summary.items():
                status_icon = "✅" if counts["failed"] == 0 else "❌"
                dry_run_msg = " (DRY RUN)" if DRY_RUN and (counts["success"] > 0 or counts["failed"] > 0) else ""
                if counts["failed"] == 0:
                     print(f"    {status_icon} {permission}: Applied to {counts['success']}/{num_catalogs} catalogs successfully{dry_run_msg}.")
                else:
                     print(f"    {status_icon} {permission}: Applied to {counts['success']}/{num_catalogs} catalogs.{dry_run_msg} {counts['failed']} failed.")
                print() # Add whitespace after summary

    # No return needed, results stored in permission_results
    # return uc_summary # Or return nothing

def display_summary():
    """Display a summary of permission grants."""
    if not permission_results:
        print("No permission operations performed.")
        return
    
    df = pd.DataFrame(permission_results)
    
    # Show summary by object type and status
    summary = df.groupby(['type', 'status']).size().unstack(fill_value=0)
    
    # Define potential status columns including dry run
    all_status_columns = ['failed', 'success', 'dry_run_success']
    final_summary_columns = ['Object Type']

    # Ensure both success and failed columns exist
    for col in all_status_columns:
        if col not in summary.columns:
            summary[col] = 0
        # Add to final list if it exists (has non-zero sum) or is 'failed'/'success'
        if col in summary.columns and (summary[col].sum() > 0 or col in ['failed', 'success']):
             # Capitalize for display
             final_summary_columns.append(col.replace('_', ' ').title())

    # Rename and reorder columns for clarity
    summary = summary.reset_index()
    # Filter columns based on what actually occurred or the essential ones
    display_cols_ordered = ['type'] + [col for col in all_status_columns if col.replace('_', ' ').title() in final_summary_columns]
    summary = summary[display_cols_ordered]
    summary.columns = final_summary_columns # Rename columns
    
    print()
    print("=" * 80)
    print("PERMISSION GRANT SUMMARY")
    print(f"Run at: {datetime.now().strftime('%B %d, %Y at %I:%M:%S %p')}")
    print("Parameters:")
    print(f"  - Group Name: {GROUP_NAME}")
    print(f"  - Dry Run: {DRY_RUN}")
    print(f"  - Verbose: {VERBOSE}")
    print(f"    (Verbose=True logs each operation; False logs per-type summaries)")
    print("=" * 80)
    print()
    
    # Display the summary table
    print(summary.to_string(index=False))
    print()
    print("Note: For workspace objects, '0 found' might indicate no objects exist or the script runner lacks list permissions.")
    
    # Calculate totals directly from the permission_results DataFrame
    df = pd.DataFrame(permission_results)
    total_successful = df[df['status'] == 'success'].shape[0]
    total_failed = df[df['status'] == 'failed'].shape[0]
    total_dry_run = df[df['status'] == 'dry_run_success'].shape[0]
    
    # Check if all operations were successful
    if DRY_RUN:
        total_ops = total_dry_run
        if total_ops > 0:
             print(f"✅ DRY RUN: Would have attempted {total_ops} permission assignments.")
        else:
             print("✅ DRY RUN: No permission assignments were attempted (check logs for listing errors).")
    else:
        total_ops = total_failed + total_successful
        if total_ops == 0:
             print("⚠️ No permission assignments were completed (check logs for listing errors).")
        elif total_failed == 0:
            print(f"✅ All {total_ops} permission assignments completed successfully!")
        elif total_successful == 0:
            print(f"❌ All {total_ops} permission assignments failed!")
        else:
            print(f"⚠️ {total_successful} of {total_ops} permission assignments successful. {total_failed} failed.")
    
    # Show details of failed operations
    if total_failed > 0:
        print()
        print("Failed operations:")
        failed_df = df[df['status'] == 'failed'][['type', 'name', 'permission', 'error', 'timestamp']]
        # Use pandas option to prevent truncation
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
             print(failed_df.to_string(index=False))

def confirm_action():
    """Ask for confirmation before proceeding with real changes."""
    if DRY_RUN:
        return True
    
    print()
    print("🚨 WARNING 🚨")
    print("You are about to apply global read-only permissions to ALL objects in your workspace.")
    print("This will affect ALL clusters, jobs, notebooks, etc.")
    print("The group name will be: " + GROUP_NAME)
    print()
    print("Type 'yes' to continue or anything else to abort:")
    
    response = input()
    return response.lower() == 'yes'

def main():
    """Main execution function."""
    print("🔑 Global Read-Only Access Setup 🔑")
    print(f"Mode: {'DRY RUN - No changes will be applied' if DRY_RUN else 'LIVE - Changes will be applied'}")
    
    if not confirm_action():
        print("Operation aborted by user.")
        return
    
    # Create group if it doesn't exist
    if not ensure_group_exists():
        print("Cannot proceed without creating the group. Exiting.")
        return
    
    # Process workspace permissions
    process_workspace_permissions()
    
    # Process Unity Catalog permissions
    process_unity_catalog_permissions()
    
    # Display summary
    display_summary()

if __name__ == "__main__":
    main()