"""
This job will run the steps which will extract raw members data of a trello board and place them
in a Delta Lake Bronze table
It is run once
"""

import os
import sys
from loguru import logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from operations import bronze_layer_operations as bl
from helpers import common_functions as cf, spark_helper as sh

def perform_members_ingestion_to_bronze():
    """
    Run the steps to extract raw members data and place it Delta Lake Bronze table
    """
    logger.info("Starting the members ingestion job")
    try:
        # Get SparkSession
        spark, spark_context = sh.start_spark()

        # Get current sprint board info
        board_id, board_name = cf.get_current_sprint_board()

        logger.info(
            f'Fetched current sprint board info for {board_id} {board_name}')

        # Extract raw members data
        bl.ingest_raw_members_data_bronze(
            spark, spark_context, board_id, board_name)
        logger.info(
            f'Ingestion of raw members data completed for board {board_id} {board_name}')
        logger.success('Completed the members ingestion job')

    except Exception as error:
        logger.exception(
            f'Failure in job to fetch raw members data for board {board_id} {board_name}')
        raise error


# --------START OF SCRIPT
if __name__ == "__main__":
    perform_members_ingestion_to_bronze()
