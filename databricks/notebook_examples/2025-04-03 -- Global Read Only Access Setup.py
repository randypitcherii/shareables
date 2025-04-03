#!/usr/bin/env python
# coding: utf-8

# # Global Read Only Access Setup
# 
# This script creates a global read only group that is granted read-only access to all workspace and Unity Catalog objects.

import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
import databricks.sdk.service.catalog as catalog

# Parameters - Adjust these as needed
GROUP_NAME = "GLOBAL_READ_ONLY_GROUP"
DRY_RUN = False  # Set to False to apply changes
VERBOSE = False  # Set to True for detailed output

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
            print(f"  Would grant {permission_level} on {obj_type} {display_name}")
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
    permissions_to_grant = []
    
    # Skip workspace directory permission as it's not supported in the API
    # permissions_to_grant.append(("directories", "/", "CAN_READ", "/"))
    
    # Grant attach access to all clusters
    for cluster in w.clusters.list():
        permissions_to_grant.append(("clusters", cluster.cluster_id, "CAN_ATTACH_TO", cluster.cluster_name))
    
    # Grant view access to all jobs
    for job in w.jobs.list():
        permissions_to_grant.append(("jobs", str(job.job_id), "CAN_VIEW", job.settings.name))
    
    # Skip experiments - different API is needed for experiments
    
    # Grant view access to all SQL warehouses
    for warehouse in w.warehouses.list():
        permissions_to_grant.append(("sql/warehouses", warehouse.id, "CAN_VIEW", warehouse.name))
    
    # Grant view access to all queries
    try:
        for query in w.queries.list():
            permissions_to_grant.append(("sql/queries", str(query.id), "CAN_VIEW", query.name))
    except Exception as e:
        if VERBOSE:
            print(f"  ⚠️ Error listing queries: {str(e)}")
    
    # Grant view access to all Delta Live Tables pipelines
    try:
        for pipeline in w.pipelines.list():
            permissions_to_grant.append(("pipelines", pipeline.pipeline_id, "CAN_VIEW", pipeline.name))
    except Exception as e:
        if VERBOSE:
            print(f"  ⚠️ Error listing pipelines: {str(e)}")
    
    # Skip serving endpoints - they use a different permissions model
    
    # Apply all permissions
    success_count = 0
    failed_count = 0
    
    for obj_type, obj_id, permission, name in permissions_to_grant:
        if apply_permission(obj_type, obj_id, permission, name):
            success_count += 1
        else:
            failed_count += 1
    
    print(f"Finished processing workspace permissions: {success_count} successful, {failed_count} failed.")
    return success_count, failed_count

def process_unity_catalog_permissions():
    """Process Unity Catalog permissions to apply read-only access to catalogs."""
    print("Processing Unity Catalog permissions...")
    
    # Track statistics
    uc_results = {
        "total_catalogs": 0,
        "total_permissions": 0,
        "use_catalog": 0,
        "use_schema": 0,
        "select": 0,
        "read_volume": 0,
        "browse": 0,
        "failed": 0
    }
    
    # Get all catalogs - convert the generator to a list first
    try:
        catalogs = list(w.catalogs.list())
        uc_results["total_catalogs"] = len(catalogs)
        
        if len(catalogs) == 0:
            print("  No catalogs found in Unity Catalog")
            return uc_results
        
        for cat in catalogs:
            catalog_name = cat.name
            print(f"  Processing catalog: {catalog_name}")
            
            # Define permissions to grant for each catalog
            permissions = [
                "USE CATALOG", "USE SCHEMA", "SELECT", "READ VOLUME", "BROWSE"
            ]
            
            for permission in permissions:
                try:
                    if DRY_RUN:
                        if VERBOSE:
                            print(f"    Would grant {permission} on catalog {catalog_name}")
                        uc_results[permission.lower().replace(" ", "_")] += 1
                        uc_results["total_permissions"] += 1
                    else:
                        # Get the first warehouse for SQL execution
                        warehouses = list(w.warehouses.list())
                        if not warehouses:
                            print("    ❌ No warehouse available for SQL execution")
                            uc_results["failed"] += 1
                            continue
                            
                        sql_query = f"""
                        GRANT {permission} ON CATALOG `{catalog_name}` TO `{GROUP_NAME}`
                        """
                        try:
                            w.statement_execution.execute_statement(
                                warehouse_id=warehouses[0].id,
                                statement=sql_query,
                                wait_timeout=None,
                            )
                            if VERBOSE:
                                print(f"    ✅ Granted {permission} on catalog {catalog_name}")
                            
                            # Update counts
                            uc_results[permission.lower().replace(" ", "_")] += 1
                            uc_results["total_permissions"] += 1
                            
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
                            print(f"    ❌ Error granting {permission} to catalog {catalog_name}: {error_msg}")
                            uc_results["failed"] += 1
                            
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
                    print(f"    ❌ Error processing permission {permission} for catalog {catalog_name}: {error_msg}")
                    uc_results["failed"] += 1
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Error listing catalogs: {error_msg}")
        uc_results["failed"] += 1
    
    # Print summary with correct status indicators
    success = uc_results["failed"] == 0
    status_icon = "✅" if success else "❌"
    
    print(f"  {status_icon} Processed {uc_results['total_catalogs']} catalogs in Unity Catalog")
    print()
    print("  Summary of Unity Catalog grants:")
    print(f"  {status_icon} Total catalogs: {uc_results['total_catalogs']}")
    print(f"  {status_icon} Total permissions granted: {uc_results['total_permissions']}")
    print(f"  {status_icon} Failed operations: {uc_results['failed']}")
    print(f"  {status_icon} USE CATALOG: {uc_results['use_catalog']} grants")
    print(f"  {status_icon} USE SCHEMA: {uc_results['use_schema']} grants")
    print(f"  {status_icon} SELECT: {uc_results['select']} grants")
    print(f"  {status_icon} READ VOLUME: {uc_results['read_volume']} grants")
    print(f"  {status_icon} BROWSE: {uc_results['browse']} grants")
    
    return uc_results

def display_summary():
    """Display a summary of permission grants."""
    if not permission_results:
        print("No permission operations performed.")
        return
    
    df = pd.DataFrame(permission_results)
    
    # Show summary by object type and status
    summary = df.groupby(['type', 'status']).size().unstack(fill_value=0)
    
    # Ensure both success and failed columns exist
    if 'success' not in summary.columns:
        summary['success'] = 0
    if 'failed' not in summary.columns:
        summary['failed'] = 0
        
    # Rename and reorder columns for clarity
    summary = summary[['failed', 'success']].reset_index()
    summary.columns = ['Object Type', 'Failed', 'Successful']
    
    print()
    print("=" * 80)
    print("PERMISSION GRANT SUMMARY")
    print(f"Run at: {datetime.now().strftime('%B %d, %Y at %I:%M:%S %p')}")
    print("Parameters:")
    print(f"  - Group Name: {GROUP_NAME}")
    print(f"  - Dry Run: {DRY_RUN}")
    print(f"  - Verbose: {VERBOSE}")
    print("=" * 80)
    print()
    
    # Display the summary table
    print(summary.to_string(index=False))
    print()
    
    # Check if all operations were successful
    total_failed = summary['Failed'].sum()
    total_successful = summary['Successful'].sum()
    total_ops = total_failed + total_successful
    
    if total_failed == 0:
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
    ws_success, ws_failed = process_workspace_permissions()
    
    # Process Unity Catalog permissions
    uc_results = process_unity_catalog_permissions()
    
    # Display summary
    display_summary()

if __name__ == "__main__":
    main()