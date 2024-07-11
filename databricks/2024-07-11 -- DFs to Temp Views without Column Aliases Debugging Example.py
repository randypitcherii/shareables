# Databricks notebook source
from pyspark.sql import functions as F

# Sample data about planets in the solar system
data = [
    ("Mercury", "Rocky"),
    ("Venus", "Rocky"),
    ("Earth", "Rocky"),
    ("Mars", "Rocky"),
    ("Jupiter", "Gas Giant"),
    ("Saturn", "Gas Giant"),
    ("Saturn", "Gas Giant"), # duplicate
    ("Uranus", "Gas Giant"),
    ("Neptune", "Gas Giant")
]

# Creating DataFrame
planets_df = spark.createDataFrame(data, ["Planet", "Type"])

# Aggregate DataFrame to get the count of planets by their type and count distinct
aggregate_df_without_alias = planets_df.groupBy("Type").agg(
    F.count("Planet"), 
    F.countDistinct("Planet")
) 
aggregate_df_with_alias    = planets_df.groupBy("Type").agg(
    F.count("Planet").alias("num_planets"), 
    F.countDistinct("Planet").alias("unique_planets")
)

# Register the DataFrame as a temporary view
aggregate_df_without_alias.createOrReplaceTempView("planet_counts_without_alias")
aggregate_df_with_alias.createOrReplaceTempView("planet_counts_with_alias")

display(spark.sql('select * from planet_counts_without_alias'))
display(spark.sql('select * from planet_counts_with_alias'))
