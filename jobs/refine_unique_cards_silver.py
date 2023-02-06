"""
This job will run the steps which will deduplicate the refined cards data and place them
in a Delta Lake Silver table
"""

import os
import sys
from loguru import logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from operations import silver_layer_operations as sl
from helpers import spark_helper as sh

def remove_duplicate_cards_refinement_to_silver():
    """
    Run the steps to perform deduplication of cards data and place it Delta Lake Silver table
    """
    logger.info("Starting the cards refinement job")
    try:
        # Get SparkSession
        spark, spark_context = sh.start_spark()

        # Extract raw cards data
        sl.get_unique_cards_silver(spark)
        logger.success('Completed the cards refinement job')

    except Exception as error:
        logger.exception(
            f'Failure in job to refine cards data')
        raise error


# --------START OF SCRIPT
if __name__ == "__main__":
    remove_duplicate_cards_refinement_to_silver()
