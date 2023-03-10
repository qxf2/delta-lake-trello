"""
This script will fetch all the cards of a Trello board according to the defined criteria:
This script fetches all the Trello cards that I have worked on over a period of time 
and place it in a Gold delta table
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

def get_member_cards(start_date,end_date):
    """
    Fetch all the cards data between provided date range and
    save it into a Delta Lake Gold table
    :param spark: Spark session
    :return none
    """
    path = dc.raghava_cards_gold_path
    try:
        spark, context = sh.start_spark()

        # Fetch the cards data from the refined cards silver delta table
        refined_cards_data = spark.read.format(
            "delta").load(dc.cards_unique_silver_table_path)
        logger.info(f'Fetched the refined cards data from silver table')

        # trim date using datetime
        start_date = datetime.strptime(start_date,'%Y-%m-%d')
        end_date = datetime.strptime(end_date,'%Y-%m-%d')
        # filter the data based on name, start date and end date
        filtered_data = refined_cards_data.filter(col("card_members").contains("raghava.nelabhotla") & (col("dateLastActivity") >= start_date) & (col("dateLastActivity") <= end_date))

        #Select the required columns
        member_cards_data = filtered_data.select(
            "id", "name", "card_members", "LastUpdated", "board_name")

        #Write the data to Delta Lake Gold table
        member_cards_data.write.format('delta').mode(
            "overwrite").option("mergeSchema", 'true').save(path)

        logger.success(
            f'\n Saved raghava cards data to {path}')
        logger.info(
            f'Saved raghava cards data for boardto {path} table')

    except Exception as error:
        logger.exception(
            f'Exception while fetching cards for')
        raise error

# --------START OF SCRIPT
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='Create gold delta table')
    PARSER.add_argument('--start_date', metavar='starting date', required=True,
                        help='start date')
    PARSER.add_argument('--end_date', metavar='end date', required=True,
                         help='end date')
    ARGS = PARSER.parse_args()
    get_member_cards(ARGS.start_date, ARGS.end_date)
