def model(dbt, session):
    dbt.config(
        materialized="table",
        submission_method="serverless_cluster",
        tags=["r22_python_model"],
    )
    events = dbt.ref("events")
    return events.groupBy("customer_id").agg({"amount": "sum"}).withColumnRenamed(
        "sum(amount)", "total_amount"
    )
