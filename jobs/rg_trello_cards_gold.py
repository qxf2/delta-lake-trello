"""
This script will fetch all the trello cards of Raji Gali
Save the data to Delta Lake Gold table raji_cards_gold
"""

import os
import sys
from datetime import datetime, timedelta
import argparse
from loguru import logger
from pyspark.sql.functions import *
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import spark_helper as sh, common_functions as cf, trello_functions as tf
from config import delta_table_conf as dc


def user_cards_to_gold(startDate,endDate,user="rajigali"):
    """
    Run the steps to get all the data of cards that 
    belongs to user and place it Delta Lake Gold table
    """
    logger.info("Starting the job to fetch user cards")
    try:
        # Get SparkSession and SparkContext
        spark, context = sh.start_spark()
        path = dc.raji_cards_gold_path
        startDate = datetime.strptime(startDate, '%Y-%m-%d')
        endDate = datetime.strptime(endDate, '%Y-%m-%d')

        # Fetch the cards data from the refined cards silver delta table
        refined_cards_data = spark.read.format(
            "delta").load(dc.cards_unique_silver_table_path)
        logger.info(f'Fetched the refined cards data from silver table')

        #convert trello last activity date format is in UTC to python date time
        refined_cards_data = refined_cards_data.withColumn("dateLastActivity", to_date(from_utc_timestamp("dateLastActivity", "UTC")))

        # Apply the filters for the start date , end date & the member to fetch
        user_cards_data = refined_cards_data.filter((col("card_members").like(f"%{user}%")) & \
                            (col("dateLastActivity") >= startDate) & (col("dateLastActivity") <= endDate)) \
                            .select("id", "name", "LastUpdated", "board_name")
        
        #Write the data to Delta Lake Gold table
        user_cards_data.write.format('delta').mode("overwrite").option("mergeSchema", 'true').save(path)  

        logger.success(f'Completed fetching and saving given user={user} activity cards')

    except Exception as error:
        logger.exception(
            f'Failure in job to fetch the member cards data')
        raise error
        

# --------START OF SCRIPT
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='Create gold delta table for the member: Raji Gali')
    PARSER.add_argument('--startDate',metavar=str,required=True,help="Start Date %Y-%m-%d")
    PARSER.add_argument('--endDate',metavar=str,required=True,help="End Date %Y-%m-%d")

    ARGS = PARSER.parse_args()
    user_cards_to_gold(ARGS.startDate,ARGS.endDate,"rajigali")

   