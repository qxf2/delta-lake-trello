"""
This script will fetch all the cards of a trello board that according to the below criteria:
- present in doing list
- does not have any activity from past specified days
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
from helpers import spark_helper as sh, common_functions as cf, trello_functions as tf
from config import delta_table_conf as dc

def get_noactivity_cards(spark, board_id, board_name, no_of_days):
    """
    Fetch all the cards data of a trello board that are in doing list and
    not active from past specified days and save it into a Delta Lake Gold table
    :param spark: Spark session
    :param board_id: trello board id
    :param board_name: trello board name
    :param no_of_days: number of days the cards are not active
    :return none
    """
    path = dc.noactive_cards_gold_path
    try:
        # Fetch the cards data from the refined cards silver delta table
        refined_cards_data = spark.read.format(
            "delta").load(dc.cards_silver_table_path)
        logger.info(f'Fetched the refined cards data from silver table')

        # Get the cards from the doing list
        doing_list_id = tf.extract_list_id(board_id, "doing")
        refined_doing_cards_data = refined_cards_data.where(
            col("idList") == doing_list_id)

        # Fetch all cards that are not active from provided number of days
        noactive_doing_cards = refined_doing_cards_data.where(
            col('dateLastActivity') < datetime.now() - timedelta(days=no_of_days))
        logger.info('Fetched all the doing cards that have no activity')

        #Select the required columns
        noactive_doing_cards = noactive_doing_cards.select(
            "id", "name", "card_members", "LastUpdated", "board_name")

        #Write the data to Delta Lake Gold table
        noactive_doing_cards.write.format('delta').mode(
            "overwrite").option("mergeSchema", 'true').save(path)

        logger.success(
            f'\n Saved no activity doing cards data for board {board_id} {board_name}')
        logger.info(
            f'Saved no activity doing cards data for board {board_id} {board_name} to {path} table')

    except Exception as error:
        logger.exception(
            f'Exception while fetching no activity doing cards for {board_id} {board_name}')
        raise error


def perform_noactive_cards_aggregation_to_gold(no_of_days):
    """
    Run the steps to get all the data of cards that 
    are not active and place it Delta Lake Gold table
    """
    logger.info("Starting the job to fetch no activity cards")
    try:
        # Get SparkSession and SparkContext
        spark, context = sh.start_spark()

        # Get current sprint board info
        board_id, board_name = cf.get_current_sprint_board()
        logger.info(
            f'Fetched current sprint board info for {board_id} {board_name}')

        # Fetch cards that have no activity from past 7 days
        get_noactivity_cards(spark, board_id, board_name, no_of_days)
        logger.success('Completed the no activity cards job')

    except Exception as error:
        logger.exception(
            f'Failure in job to fetch noactivity cards data for board {board_id} {board_name}')
        raise error


# --------START OF SCRIPT
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='Create gold delta table')
    PARSER.add_argument('--days', metavar='number', required=True,
                        help='Number of days')
    ARGS = PARSER.parse_args()
    perform_noactive_cards_aggregation_to_gold(int(ARGS.days))
