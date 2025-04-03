#!/usr/bin/env python
"""
🔍 Databricks Environment Validator
Checks if your local Databricks environment is properly configured
"""
from importlib.metadata import version
import sys
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession

def get_error_message(e: Exception) -> str:
    """Extract the main error message without the stack trace"""
    return str(e).split('\n')[0]

def validate_databricks_sdk():
    """Validates Databricks SDK connectivity"""
    print("\n🔍 Checking Databricks SDK...")
    sdk_success = True
    
    try:
        # Initialize the Databricks client
        client = WorkspaceClient()
        print("  ✅ Successfully connected to Databricks!")
        
        # Get connection details
        current_user = client.current_user.me()
        workspace_host = client.config.host
        
        try:
            workspace = client.workspace_info()
            workspace_id = workspace.workspace_id
            print(f"  ✅ Connected as: {current_user.user_name}")
            print(f"  ✅ Workspace: {workspace.deployment_name} ({workspace_host})")
            print(f"  ✅ Workspace ID: {workspace_id}")
        except Exception:
            print(f"  ✅ Connected as: {current_user.user_name}")
            print(f"  ✅ Workspace: {workspace_host}")
        
        # Test access to resources
        print("  📊 Resource Access:")
        resources_ok = True
        try:
            clusters = list(client.clusters.list())
            print(f"    ✅ Clusters: {len(clusters)}")
        except Exception as e:
            print(f"    ❌ Clusters: {get_error_message(e)}")
            resources_ok = False
            
        try:
            jobs = list(client.jobs.list())
            print(f"    ✅ Jobs: {len(jobs)}")
        except Exception as e:
            print(f"    ❌ Jobs: {get_error_message(e)}")
            resources_ok = False
            
        try:
            warehouses = list(client.warehouses.list())
            print(f"    ✅ SQL Warehouses: {len(warehouses)}")
        except Exception as e:
            print(f"    ❌ SQL Warehouses: {get_error_message(e)}")
            resources_ok = False
            
        try:
            repos = list(client.repos.list())
            print(f"    ✅ Repos: {len(repos)}")
        except Exception as e:
            print(f"    ❌ Repos: {get_error_message(e)}")
            resources_ok = False
            
        if not resources_ok:
            sdk_success = False
            
    except Exception as e:
        print(f"  ❌ SDK Connection Failed: {get_error_message(e)}")
        sdk_success = False
        
    return sdk_success

def validate_databricks_connect():
    """Validates Databricks Connect (Spark) connectivity"""
    print("\n🔍 Checking Databricks Connect...")
    connect_success = True
    
    try:
        spark = DatabricksSession.builder.getOrCreate()
        print("  ✅ Successfully connected to Spark!")
        
        # Get user info
        try:
            user_info = spark.sql("SELECT current_user() as current_user").collect()[0]
            print(f"  ✅ Current User: {user_info.current_user}")
        except Exception as e:
            print(f"  ❌ Could not get user info: {get_error_message(e)}")
            connect_success = False
        
        # Check compute info
        try:
            cluster_name = spark.conf.get('spark.databricks.clusterUsageTags.clusterName')
            if cluster_name:
                print(f"  ✅ Using Cluster: {cluster_name}")
            else:
                print("  ℹ️  No compute name found, assuming serverless compute")
        except Exception:
            print("  ℹ️  No compute name found, assuming serverless compute")
            
        # Test database access
        try:
            dbs = spark.sql("SHOW DATABASES").collect()
            print(f"  ✅ Found {len(dbs)} databases")
        except Exception as e:
            print(f"  ❌ Cannot list databases: {get_error_message(e)}")
            connect_success = False
            
    except Exception as e:
        print(f"  ❌ Spark Connection Failed: {get_error_message(e)}")
        connect_success = False
        
    return connect_success

def main():
    print("Databricks Environment Validation")
    print("=================================")
    print(f"  ✅ Python Version: {sys.version.split()[0]}")
    print(f"  ✅ Virtual Env: {sys.prefix}")
    print(f"  ✅ Databricks SDK Version: {version('databricks-sdk')}")
    print(f"  ✅ Databricks Connect Version: {version('databricks-connect')}")
    print()
    
    w = WorkspaceClient()
    sdk_success = True
    connect_success = True
    
    try:
        workspace_host = w.config.host
        print("Databricks SDK:")
        print(f"  ✅ Successfully connected as {w.current_user.me().user_name}")
        print(f"  ✅ Workspace: {workspace_host}")
        
        # Test access to resources
        print("  📊 Resource Access:")
        resources_ok = True
        try:
            clusters = list(w.clusters.list())
            print(f"    ✅ Clusters: {len(clusters)}")
        except Exception as e:
            print(f"    ❌ Clusters: {get_error_message(e)}")
            resources_ok = False
            
        try:
            jobs = list(w.jobs.list())
            print(f"    ✅ Jobs: {len(jobs)}")
        except Exception as e:
            print(f"    ❌ Jobs: {get_error_message(e)}")
            resources_ok = False
            
        try:
            warehouses = list(w.warehouses.list())
            print(f"    ✅ SQL Warehouses: {len(warehouses)}")
        except Exception as e:
            print(f"    ❌ SQL Warehouses: {get_error_message(e)}")
            resources_ok = False
            
        try:
            repos = list(w.repos.list())
            print(f"    ✅ Repos: {len(repos)}")
        except Exception as e:
            print(f"    ❌ Repos: {get_error_message(e)}")
            resources_ok = False
            
        if not resources_ok:
            sdk_success = False
            
    except Exception as e:
        print(f"  ❌ SDK Connection Failed: {get_error_message(e)}")
        sdk_success = False
        
    connect_ok = validate_databricks_connect()
    
    print("\n📋 Summary:")
    print(f"  {'✅' if sdk_success else '❌'} Databricks SDK")
    print(f"  {'✅' if connect_ok else '❌'} Databricks Connect")

if __name__ == "__main__":
    main() 