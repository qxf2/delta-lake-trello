"""
This job will run the steps which will refine cards data of a trello board and place them
in a Delta Lake Silver table
It is scheduled to run
"""

import os
import sys
from loguru import logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from operations import silver_layer_operations as sl
from helpers import common_functions as cf, spark_helper as sh

def perform_cards_refinement_to_silver():
    """
    Run the steps to perform refinement of cards data and place it Delta Lake Silver table
    """
    logger.info("Starting the cards refinement job")
    try:
        # Get SparkSession
        spark, spark_context = sh.start_spark()

        # Get current sprint board info
        board_id, board_name = cf.get_current_sprint_board()
        logger.info(
            f'Fetched current sprint board info for {board_id} {board_name}')

        # Extract raw cards data
        sl.refine_current_board_cards_silver(spark, board_id, board_name)
        logger.info(
            f'Refinement of cards data completed for board {board_id} {board_name}')
        logger.success('Completed the cards refinement job')

    except Exception as error:
        logger.exception(
            f'Failure in job to refine cards data for board {board_id} {board_name}')
        raise error


# --------START OF SCRIPT
if __name__ == "__main__":
    perform_cards_refinement_to_silver()
