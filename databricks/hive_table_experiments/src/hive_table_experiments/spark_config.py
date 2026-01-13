"""Spark configuration for AWS Glue as hive_metastore"""

from typing import Dict


def get_glue_spark_config(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    glue_region: str = "us-east-1",
) -> Dict[str, str]:
    """
    Generate Spark configuration for using AWS Glue as hive_metastore.

    This configures Databricks to use Glue Data Catalog as the Hive metastore
    instead of the default hive_metastore or Unity Catalog.

    Args:
        aws_access_key_id: AWS access key for IAM user
        aws_secret_access_key: AWS secret key for IAM user
        glue_region: AWS region where Glue catalog resides

    Returns:
        Dictionary of Spark configuration properties
    """
    return {
        # Enable Glue as Hive metastore
        "spark.databricks.hive.metastore.glueCatalog.enabled": "true",

        # Glue catalog region
        "spark.hadoop.aws.glue.catalog.region": glue_region,

        # AWS credentials for accessing Glue and S3
        "spark.hadoop.fs.s3a.access.key": aws_access_key_id,
        "spark.hadoop.fs.s3a.secret.key": aws_secret_access_key,

        # Enable recursive directory scanning (for recursive subfolder scenarios)
        "mapreduce.input.fileinputformat.input.dir.recursive": "true",

        # Disable Databricks from auto-updating Glue metadata
        "spark.databricks.delta.catalog.update.enabled": "false",

        # Hive settings for better compatibility
        "spark.sql.hive.convertMetastoreParquet": "false",
    }


def get_spark_config_from_secrets(spark) -> Dict[str, str]:
    """
    Retrieve AWS credentials from Databricks secrets and generate Spark config.

    Expects secrets in scope 'your-project-scope':
    - aws_access_key_id
    - aws_secret_access_key

    Args:
        spark: Active Spark session (for dbutils access)

    Returns:
        Dictionary of Spark configuration properties
    """
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

        access_key = dbutils.secrets.get(scope="your-project-scope", key="aws_access_key_id")
        secret_key = dbutils.secrets.get(scope="your-project-scope", key="aws_secret_access_key")

        return get_glue_spark_config(access_key, secret_key)
    except Exception as e:
        raise RuntimeError(
            f"Failed to retrieve credentials from Databricks secrets: {e}. "
            "Ensure secrets scope 'your-project-scope' exists with keys:"
            "'aws_access_key_id' and 'aws_secret_access_key'"
        )
