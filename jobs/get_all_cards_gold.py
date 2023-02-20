"""
This script will fetch all the cards of a trello board that according to the below criteria:
- Get the data for a defined data range
Save the data to Delta Lake Gold table
This script is scheduled to run twice a week
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

def get_cards(spark, start_date, end_date):
    """
    Fetch all the cards data of a user within the mentioned date range
    :param spark: Spark session
    :param board_id: trello board id
    :param board_name: trello board name
    :param no_of_days: number of days the cards are not active
    :return none
    """
    path = dc.ajitava_cards_gold_path
    try:
        # Fetch the cards data from the refined cards silver delta table
        refined_cards_data = spark.read.format(
            "delta").load(dc.cards_unique_silver_table_path)
        logger.info(f'Fetched the refined cards data from silver table')

        user_card_data = refined_cards_data.filter((col("card_members").like("Ajitava Deb")))
        logger.info(f'Got the user cards based on specific user')

        # Get the dates
        start_date = datetime.strptime(start_date, "%d-%m-%Y")
        end_date = datetime.strptime(end_date, "%d-%m-%Y")

        user_date_cards = user_card_data.where((col('dateLastActivity') == start_date) & (col('dateLastActivity') == end_date))
        logger.info(f'Fetched user cards for specific duration')

        #Select the required columns
        user_date_cards = user_date_cards.select(
            "id", "name", "card_members", "LastUpdated", "board_name")

        #Write the data to Delta Lake Gold table
        user_date_cards.write.format('delta').mode(
            "overwrite").option("mergeSchema", 'true').save(path)

        logger.success(
            f'\n Saved user data for specified duration')
        logger.info(
            f'Saved user data to {path} table')

    except Exception as error:
        logger.exception(
            f'Exception while fetching no activity doing cards for {board_id} {board_name}')
        raise error


def perform_user_cards_aggregation_to_gold(start_date,end_date):
    """
    Run the steps to get all the data of cards for the users 
    and place it Delta Lake Gold table
    """
    logger.info("Starting the job to fetch user cards")
    try:
        # Get SparkSession and SparkContext
        spark, context = sh.start_spark()

        # Fetch cards that have no activity from past 7 days
        get_cards(spark, start_date, end_date)
        logger.success('Completed the user activity cards job')

    except Exception as error:
        logger.exception(
            f'Failure in job to fetch user cards data')
        raise error


# --------START OF SCRIPT
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='Create gold delta table')
    PARSER.add_argument('--start_date', metavar='string', required=True,
                        help='Start in date format of MM-DD-YYYY')
    PARSER.add_argument('--end_date', metavar='string', required=True,
                        help='End in date format of MM-DD-YYYY')    
    ARGS = PARSER.parse_args()
    perform_user_cards_aggregation_to_gold(ARGS.start_date,ARGS.end_date)

