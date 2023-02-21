"""
This script will fetch all the cards from silver unique cards table and 
- filter the table for specific user
- filter data for specific timeframe passed in the script
- save it to user Delta Lake Gold table
"""

import os
import sys
from datetime import datetime, timedelta
import argparse
from loguru import logger
from pyspark.sql.functions import col, from_utc_timestamp, to_date
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import spark_helper as sh, common_functions as cf, trello_functions as tf
from config import delta_table_conf as dc

def get_user_cards(spark, start_date, end_date):
    """
    Fetch all the cards data for the given user within the given date range 
    and save it into a Delta Lake Gold table
    :param spark: Spark session
    :param start_date: start date
    :param end_date: end date
    :return none
    """
    path = dc.akkul_cards_gold_path
    try:
        # Fetch the cards data from the refined cards silver delta table
        refined_cards_data = spark.read.format(
            "delta").load(dc.cards_unique_silver_table_path)
        logger.info(
            f'Fetched refined card data from silver delta table')      

        # Convert from_date to a datetime object
        start_date = datetime.strptime(start_date, '%Y-%m-%d')

        # Convert "dateLastActivity" column to date format
        refined_cards_data = refined_cards_data.withColumn("dateLastActivity", to_date(from_utc_timestamp("dateLastActivity", "UTC")))

        # Filter the DataFrame for the given user and date range
        filtered_cards_data = refined_cards_data.filter((col("card_members").like("%akkuldn%")) & (col("dateLastActivity") >= start_date) & (col("dateLastActivity") <= end_date))
        logger.info(f'Fetched user card data within the date range')  

        #Select the required columns
        filtered_cards_data = filtered_cards_data.select(
            "id", "name", "LastUpdated", "board_name")

        #Write the data to Delta Lake Gold table
        filtered_cards_data.write.format('delta').mode(
            "overwrite").option("mergeSchema", 'true').save(path)    

        logger.success(
            f'\n Saved user card data for the given data range')
        logger.info(
            f'Saved user card data to {path} table')    

    except Exception as error:
        logger.exception(
            f'Exception while fetching cards data')
        raise error


def perform_user_cards_aggregation_to_gold(start_date, end_date):
    """
    Run the steps to get all the data of cards within the provided dates and place it Delta Lake Gold table
    """
    logger.info("Starting the job to fetch all the cards for the given dates")
    try:
        # Get SparkSession and SparkContext
        spark, context = sh.start_spark()

        # Fetch all cards for the given date range
        get_user_cards(spark, start_date, end_date)
        logger.success('Completed get user cards by date range job')

    except Exception as error:
        logger.exception(
            f'Failure in job to fetch user card data between the dates {start_date} - {end_date}')
        raise error

# --------START OF SCRIPT
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='Create gold delta table')
    PARSER.add_argument('--start_date', type=str, required=True, help='Start date in the format YYYY-MM-DD')
    PARSER.add_argument('--end_date', type=str, required=True, help='End date in the format YYYY-MM-DD')
    ARGS = PARSER.parse_args()
    perform_user_cards_aggregation_to_gold(ARGS.start_date, ARGS.end_date)