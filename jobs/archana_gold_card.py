"""
Script to
 - fetch all the cards that a member has worked on within the given duration
 - save this data to the member's Delta Lake Gold table
"""
import os
import sys
from datetime import datetime
import argparse
from loguru import logger
from pyspark.sql.functions import col
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import spark_helper as sh
from config import delta_table_conf as dc

def store_member_cards_data_gold(member_cards_data, delta_table_path = dc.archana_cards_gold_path):
    """
    Store the member's cards data for the passed duration, into a Delta Lake Gold table
    :param member_cads_data: PySpark SQL DataFrame
    :param delta_table_path: str
    :return: none
    """
    try:
        # Write the data to members Delta Lake Gold table
        member_cards_data.write.format('delta').mode(
            "overwrite").option("mergeSchema", 'true').save(delta_table_path)
        logger.success(
            '\n Saved members cards data for the passed duration')
        logger.info(
            f'Saved members cards data for the passed duration to {delta_table_path} table')
    except Exception as error:
        logger.exception(
            'Exception while fetching cards data for the passed duration')
        raise error

def get_member_cards(start_date, end_date, member_name = "Archana"):
    """
    Fetch member's Trello cards data for the passed duration
    :param start_date: str
    :param end_date: str
    :param member_name: str
    :return: PySpark SQL DataFrame
    """
    try:
        # Initiation
        spark, _ = sh.start_spark()

        # Fetch the cards data from the unique cards silver delta table into a spark dataframe
        unique_cards_data = spark.read.format(
            "delta").load(dc.cards_unique_silver_table_path)
        logger.info('Fetched the unique cards data from silver table')

        # Extract that particular member cards
        member_cards_data = unique_cards_data.where(col("card_members").contains(member_name))
        logger.info('Extracted cards mapped with the members name')

        # Retrieve member cards that fall within the duration passed
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        member_duration_cards = member_cards_data.where(
            (col('dateLastActivity') >= start_date) &
            (col('dateLastActivity') <= end_date))
        logger.info('Filtered member cards within the passed duration')

        # Select the required columns
        member_duration_cards = member_duration_cards.select(
            "id", "name", "card_members", "LastUpdated", "board_name")
        logger.info('Extracted required fields from the data to be stored')
        return member_duration_cards
    except Exception as error:
        logger.exception(
            'Exception while fetching members cards data')
        raise error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Pass duration to create member gold delta table')
    parser.add_argument('--start_date', metavar='string', required=True,
                        help='start date in %Y-%m-%d format eg: 2020-04-01')
    parser.add_argument('--end_date', metavar='string', required=True,
                        help='end date in %Y-%m-%d format eg: 2020-04-01')
    args = parser.parse_args()
    # By defaults runs for <Archana>. Pass member name to run for different member.
    cards_data = get_member_cards(args.start_date, args.end_date)
    # By default stores results into <archana_gold_table>.
    # Pass appropriate file path for different member.
    store_member_cards_data_gold(cards_data)