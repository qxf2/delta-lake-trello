"""
This script will fetch all the cards of a trello board that according to the below criteria:
- present with in timeframe
- Fetch data for Drishya as username
Save the data to Delta Lake Gold table

"""

import os
import sys
from datetime import datetime, timedelta
import argparse
from loguru import logger
from pyspark.sql.functions import col
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import spark_helper as sh
from config import delta_table_conf as dc


def get_user_activity_cards(spark, start_date, end_date, nameofuser):
    """
    Fetch all the cards data of a trello board for the user and specified days and save it into a Delta Lake Gold table
    :param spark: Spark session
    :param start_date: trello fetching details start date
    :param end_date: trello fetching details end date
    :param nameofuser: the name of user that needs to filter/fetch
    :return none
    """
    path = dc.drishya_cards_gold_path
    try:
        # Fetch the cards data from the refined cards silver delta table
        refined_cards_data = spark.read.format(
            "delta").load(dc.cards_silver_table_path)
        logger.info(f'Fetched the refined cards data from silver table')

        # Convert dates to a datetime object
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Fetch all cards that are for the user within the specified dates 
        user_cards = refined_cards_data.filter((col("card_members").contains(nameofuser)) & (col("dateLastActivity") >= start_date) & (col("dateLastActivity") <= end_date))

        #Select the required columns
        user_only_cards = user_cards.select(
            "id", "name","card_members", "LastUpdated", "board_name")

        #Write the data to Delta Lake Gold table
        user_only_cards.write.format('delta').mode(
            "overwrite").option("mergeSchema", 'true').save(path)

        logger.success(
            f'\n Saved activity cards data for user {nameofuser}.')
        logger.info(
            f'Saved activity cards data for user {nameofuser} to {path} table')

    except Exception as error:
        logger.exception(
            f'Exception while fetching no cards for {nameofuser}')
        raise error


def fetch_drishya_aggregation_to_gold(start_date, end_date,name_user):
    """
    Run the steps to get all the data of cards that 
    are with name drishya and place it Delta Lake Gold table
    """
    logger.info(f'Starting the job to fetch {name_user} activity cards')
    try:
        # Get SparkSession and SparkContext
        spark, context = sh.start_spark()

        logger.info(
            f'Fetched sprint board info for {start_date} between {end_date}')

        # Fetch cards that required for the user within the specified dates
        get_user_activity_cards(spark, start_date, end_date, name_user)
        logger.success(f'Completed the {name_user} activity cards job')

    except Exception as error:
        logger.exception(
            f'Failure in job to fetch {name_user} cards data for board')
        raise error


# --------START OF SCRIPT
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='Create gold delta table for User')
    PARSER.add_argument('--start_date',type=str,required=True,help="Start Date format YYYY-MM-DD")
    PARSER.add_argument('--end_date',type=str,required=True,help="Start Date format YYYY-MM-DD")
    
    ARGS = PARSER.parse_args()
    fetch_drishya_aggregation_to_gold(ARGS.start_date,ARGS.end_date,name_user="Drishya")