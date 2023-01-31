"""
This job will run the steps which will extract raw cards data of a trello board and place them
in a Delta Lake Bronze table
It is scheduled to run daily
"""

import os
import sys
from loguru import logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from operations import bronze_layer_operations as bl
from helpers import common_functions as cf, spark_helper as sh

def perform_cards_ingestion_to_bronze():
    """
    Run the steps to extract raw cards data and place it Delta Lake Bronze table
    """
    # logger.add(lc.cards_bronze_table_path)
    logger.info("Starting the cards ingestion job")
    try:
        # Get SparkSession
        spark, spark_context = sh.start_spark()

        # Get current sprint board info
        board_id, board_name = cf.get_current_sprint_board()

        logger.info(
            f'Fetched current sprint board info for {board_id} {board_name}')

        # Extract raw cards data
        bl.ingest_raw_cards_data_bronze(
            spark, spark_context, board_id, board_name)
        logger.info(
            f'Extraction of raw cards data completed for board {board_id} {board_name}')
        logger.success("Completed the cards ingestion job")

    except Exception as error:
        logger.exception(
            f'Failure in job to fetch raw cards data for board {board_id} {board_name}')
        raise error


# --------START OF SCRIPT
if __name__ == "__main__":
    perform_cards_ingestion_to_bronze()
