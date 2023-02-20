"""
This script will fetch all the cards from silver unique cards table and 
- filter the table for specific user
- filter data for specific timeframe passed in the script
- save it to user Delta Lake Gold table
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

def get_user_activity_cards(from_date, to_date, path, user_name):
    """
    Fetch all the cards that Avinash has worked within the provided timeframe
    """
    logger.info("Starting the job to fetch cards worked by user for provided time frame")
    
    try:
        # Get SparkSession and SparkContext
        spark, context = sh.start_spark()

        # Fetch the cards data from the unique cards silver table
        refined_unique_cards_data = spark.read.format(
            "delta").load(dc.cards_unique_silver_table_path)
        logger.info(f'Fetched the unique cards data from silver unique cards table')

        # Convert the from_date and to_date argument passed to a datetime function
        from_date = datetime.strptime(from_date, '%Y-%m-%d')
        to_date = datetime.strptime(to_date, '%Y-%m-%d')
        
        # Filter card data for avinash and for date range provided
        filtered_unique_cards_data = refined_unique_cards_data.filter(col("card_members").contains(user_name) & (col("dateLastActivity") >= from_date) & (col("dateLastActivity") <= to_date))

        # Select the required columns
        filtered_unique_cards_data = filtered_unique_cards_data.select(
            "id", "name", "LastUpdated", "board_name","card_members")

        # Write the data to Delta Lake Gold table
        filtered_unique_cards_data.write.format('delta').mode(
            "overwrite").option("mergeSchema", 'true').save(path)

        logger.success(
            f'\n Saved Avinash cards data to path: {path}')
        logger.info(
            f'Saved Avinash cards data to path: {path}')

    except Exception as error:
        logger.exception(
            f'Exception while fetching data for the user')
        raise error



# --------START OF SCRIPT
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='Fetch Individual card details')
    PARSER.add_argument('--from_date', metavar='date in YYYY-MM-DD format', required=True,
                         help='Start date from which you need the tickets you worked')
    PARSER.add_argument('--to_date', metavar='date in YYYY-MM-DD format', required=True,
                         help='End date to which you need the tickets you worked')
    ARGS = PARSER.parse_args()

    path = dc.avinash_cards_gold_path
    get_user_activity_cards(ARGS.from_date, ARGS.to_date, path, user_name="avinash")
