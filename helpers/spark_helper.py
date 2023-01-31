"""
Module containing helper functions for use with Apache Spark
"""

import pyspark
from pyspark.sql import SparkSession
from delta import *


def start_spark(app_name='trello_delta_app'):
    """
    Start a Spark session
    :param app_name: Name of Spark app
    :return: Spark session object
    """

    # get Spark session factory
    builder = pyspark.sql.SparkSession.builder.appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")\
        .config("spark.ui.showConsoleProgress", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_context = spark.sparkContext

    return spark, spark_context
