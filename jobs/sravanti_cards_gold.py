"""
This script will fetch all the cards that have a particular person as the member. 
It can be queried based on date
"""

import os
import sys
from datetime import datetime as dt
import argparse
from loguru import logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import delta_table_conf as dc
from helpers import spark_helper as sh

def get_memeber_cards(spark,from_date,to_date):
    """
    Fetch all the cards data of a particular trello board member, queried based on date range
    and save it into a Delta Lake Gold table
    :param spark: Spark session
    :param from_date: Start date 
    :param to_date: End date
    :return none
    """
    path = dc.sravanti_cards_gold_path
    try:
        # Fetch the cards data from the refined unique cards silver delta table
        refined_unique_cards_data = spark.read.format(
            "delta").load(dc.cards_unique_silver_table_path)
        logger.info(f'Fetched the refined unique cards data from silver table')

        #Filter by member name
        aggregate_cards_data = refined_unique_cards_data.filter(
            'card_members LIKE "%Sravanti%"')

        #Filter by dates
        aggregate_cards_data = aggregate_cards_data.filter(aggregate_cards_data.dateLastActivity > 
                    dt.strptime(from_date, '%Y-%m-%d')).filter(aggregate_cards_data.dateLastActivity < 
                    dt.strptime(to_date, '%Y-%m-%d'))

        #Select the required columns
        aggregate_cards_data = aggregate_cards_data.select("id", "name","LastUpdated", "board_name", "card_members")

        #Write to Delta Lake Gold table
        aggregate_cards_data.write.format('delta').mode(
            "overwrite").option("mergeSchema", 'true').save(path)

        logger.success(f'Saved cards in the gold table in {path}')

    except Exception as error:
        logger.exception(
            f'Exception while fetching member cards data')
        raise error


def perform_member_data_aggregation_to_gold(from_date,to_date):
    """
    Run the steps to get member aggregated data
    and place it in Delta Lake Gold table
    """
    logger.info("Starting the job to fetch aggregated member data")
    try:
        # Get SparkSession and SparkContext
        spark, context = sh.start_spark()

        # Fetch cards that have no activity from past 7 days
        get_memeber_cards(spark, from_date, to_date)
        logger.success('Completed the job')

    except Exception as error:
        logger.exception(
            f'Failure in job to fetch aggregated member data')
        raise error


# --------START OF SCRIPT
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='Create gold delta table for member cards')
    PARSER.add_argument('--from_date', metavar='date', required=True,
                        help='from date; should be in the format YYYY-MM-DD (Eg:2023-01-02)')
    PARSER.add_argument('--to_date', metavar='date', required=True,
                        help='to_date date; should be in the format YYYY-MM-DD (Eg:2023-02-02)')
    ARGS = PARSER.parse_args()

    perform_member_data_aggregation_to_gold(ARGS.from_date, ARGS.to_date)
