"""
This script will:
- fetch all cards from silver unique cards table, 
- filter data for specific user over specified time frame and 
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

def get_user_cards_and_save_to_gold_table(start_date, end_date, user_name):
    """
    Fetch the user specific data for specified period and save it at specified gold table 
    """
    logger.info("Starting the job to fetch the user cards worked for specified time period")
    
    try:
        # Set the gold table path
        path = dc.rohan_cards_gold_path

        # Get SparkSession and Context
        spark, context = sh.start_spark()

        # Convert the start_date and end_date argument 
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Fetch the cards data from the unique cards silver table
        unique_silver_cards_data = spark.read.format("delta").load(dc.cards_unique_silver_table_path)
        logger.info(f'Fetched the cards data from unique silver cards table')
        
        # Filter user card data for specified time period
        filtered_users_unique_cards_data = unique_silver_cards_data.filter(col("card_members").contains(user_name) 
            & (col("dateLastActivity") >= start_date) & (col("dateLastActivity") <= end_date))

        # Select columns
        filtered_users_unique_cards_data = filtered_users_unique_cards_data.select("id", "name", "LastUpdated", "board_name", "card_members")

        # Write the data to Delta Lake Gold table
        filtered_users_unique_cards_data.write.format('delta').mode("overwrite").option("mergeSchema", 'true').save(path)

        logger.success(
            f'Saved {user_name} cards data to path: {path}')
    

    except Exception as error:
        logger.exception(
            f'Exception while fetching data for the user: {user_name}')
        raise error


# ---START OF SCRIPT----
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='Create Individual gold card table')
    PARSER.add_argument('--start_date', metavar='Please pass date in YYYY-MM-DD format', required=True,
                         help='Start date for filtering tickets')
    PARSER.add_argument('--end_date', metavar='Please pass date in YYYY-MM-DD format', required=True,
                         help='End date for filtering tickets')
    
    ARGS = PARSER.parse_args()

    get_user_cards_and_save_to_gold_table(ARGS.start_date, ARGS.end_date, user_name="rohandudam")