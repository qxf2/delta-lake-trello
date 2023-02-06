"""
This script contains the functions that help in refinement operations
"""

import os
import sys
from loguru import logger
from delta.tables import DeltaTable
from pyspark.sql.functions import col, date_format, lit, row_number
from pyspark.sql.window import Window
# add project root to sys path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import delta_table_conf as dc

def refine_current_board_cards_silver(spark, board_id, board_name):
    """
    Refine cards data of a trello board and save it into a Delta Lake Silver table
    :param board_id: Trello board id
    :return none
    """
    path = dc.cards_silver_table_path
    try:
        # Fetch raw cards data from bronze delta table that have been recently added
        raw_cards_data = spark.read.format(
            "delta").load(dc.cards_bronze_table_path)
        logger.info(f'Fetched the raw cards data')

        raw_mem_data = spark.read.format(
            "delta").load(dc.members_bronze_table_path)
        logger.info(f'Fetched the raw members data')
              
        # Filter data based on current sprint board
        refined_cards_data = raw_cards_data.filter(
            raw_cards_data.idBoard == board_id)

        # Create temporary views for members and cards data to run sql queries
        refined_cards_data.createOrReplaceTempView("trello_cards")
        raw_mem_data.createOrReplaceTempView("trello_members")

        refined_cards_data = spark.sql("select tc.*, array_join(collect_list(tm.fullName), ', ') as card_members from trello_cards tc \
                                        left outer join trello_members tm where array_contains (tc.idMembers, tm.id) \
                                        group by tc.id, tc.closed, tc.dateLastActivity, tc.due, tc.idBoard, tc.idList, \
                                        tc.idMembers, tc.name, tc.desc, tc.shortLink, tc.shortUrl, tc.url")

        # Change the format of dateLastActivity for readability
        refined_cards_data = refined_cards_data.withColumn(
            'LastUpdated', date_format('dateLastActivity', "d MMM"))

        # Add board_name to the list of columns
        refined_cards_data = refined_cards_data.withColumn(
            'board_name', lit(board_name))

        logger.info(
            "Completed refining the data, writing cleaned and conformed data as a Silver table in Delta Lake")

        deltaTable = DeltaTable.forPath(spark, path)
        deltaTable.alias("target").merge(
            source=refined_cards_data.alias("source"),
            condition="target.id = source.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.success(
            f'\n Refined data and created cards Silver Delta Table successfully for board {board_id} {board_name}')

    except Exception as error:
        logger.exception(
            f'Exception while creating Silver Delta Table for board {board_id} {board_name}')
        raise error

def get_unique_cards_silver(spark):
    """
    Remove the duplicate cards of silver table and save it into a Delta Lake Silver table
    :param spark: Spark session object
    """
    unique_cards_path = dc.cards_unique_silver_table_path

    try:
        # Fetch refined cards data from Silver table
        refined_cards_data = spark.read.format(
            "delta").load(dc.cards_silver_table_path)
        logger.info(f'Fetched the refined silver cards data')

        #Remove duplicate cards (Cards that might have been worked across various trello boards, 
        #the one with latest timestamp is picked)
        win = Window.partitionBy("name").orderBy(col("dateLastActivity").desc())
        deduplicated_cards = refined_cards_data.withColumn(
            "row", row_number().over(win)).filter(col("row") == 1).drop("row")

        logger.info(
            "Completed deduplicating the data, writing it as a Silver table in Delta Lake")

        #Write it to another Delta Lake Silver table
        deduplicated_cards.write.format('delta').mode(
            "overwrite").option("mergeSchema", 'true').save(unique_cards_path)

        logger.info("Completed writing the data to Silver table")

    except Exception as error:
        logger.exception(
            f'Exception while creating Silver Delta Table')
        raise error