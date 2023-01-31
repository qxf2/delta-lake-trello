"""
This script contains the functions that help in refinement operations
"""

import os
import sys
from loguru import logger
from delta.tables import DeltaTable
from pyspark.sql.functions import col, date_format, lit
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
    deltaTable = DeltaTable.forPath(spark, path)

    try:
        # Fetch raw cards data from bronze delta table that have been recently added
        raw_cards_data = spark.read.format(
            "delta").load(dc.cards_bronze_table_path)
        logger.info(f'Fetched the raw cards data')

        raw_mem_data = spark.read.format(
            "delta").load(dc.members_bronze_table_path)
        logger.info(f'Fetched the raw members data')

        print("THe board id is ", board_id)
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

        refined_cards_data.write.format('delta').mode(
            "append").option("mergeSchema", 'true').save(path)
        # deltaTable.alias("target").merge(
        #     source=refined_cards_data.alias("source"),
        #     condition="target.id = source.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.success(
            f'\n Refined data and created cards Silver Delta Table successfully for board {board_id} {board_name}')

    except Exception as error:
        logger.exception(
            f'Exception while creating Silver Delta Table for board {board_id} {board_name}')
        raise error
