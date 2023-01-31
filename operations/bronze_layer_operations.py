"""
This script contains the functions that help in ingestion operations
* Fetching the raw trello cards data
* Fetching the raw trello members data
"""

import os
import sys
import json
from loguru import logger
from delta import *
from delta.tables import DeltaTable
# add project root to sys path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import delta_table_conf as dc
from helpers import trello_functions as tf

def ingest_raw_cards_data_bronze(spark, spark_context, board_id, board_name):
    """
    Fetch raw cards data of a trello board and save it into a Delta Lake Bronze table
    :param spark: Spark session
    :param spark_context: 
    :param board_id: Trello board id
    :param board_name: Trello board name
    :return none
    """
    path = dc.cards_bronze_table_path
    try:
        all_cards = tf.get_all_cards(board_id)
        logger.success(
            f"Fetched all the cards of the board {board_id}, {board_name}")
        deltaTable = DeltaTable.forPath(spark, path)
        for each_card in all_cards:
            card_obj = tf.fetch_card_details(each_card)
            jsonRDD = spark_context.parallelize([json.dumps(card_obj)])
            data_df = spark.read.json(jsonRDD, multiLine=True)
            data_df = data_df.select("id", "closed", "dateLastActivity", "due", "idBoard",
                                     "idList", "desc", "idMembers", "name", "shortLink", "shortUrl", "url")
            deltaTable.alias("target").merge(
                source=data_df.alias("source"),
                condition="target.id = source.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.success(
            f'Successfully added the cards data to delta bronze table for the board {board_id}, {board_name}')
    except Exception as error:
        logger.exception(
            f'Exception while ingesting cards data for the board {board_id}, {board_name}')
        raise error


def ingest_raw_members_data_bronze(spark, spark_context, board_id, board_name):
    """
    Fetch raw members data of a trello board and save it into a Delta Lake Bronze table
    data ingested only for the following selected columns: 
    id, fullName, initials, membersType, url, username, status, email, idOrganizations
    :param board_id: Trello board id
    :param board_name: Trello board name
    :return none
    """
    path = dc.members_bronze_table_path
    try:
        all_members = tf.get_all_members(board_id)
        logger.success(
            f'Fetched all the members of the board {board_id}, {board_name}')
        for each_mem in all_members:
            mem_obj = tf.fetch_member_details(each_mem)
            jsonRDD = spark_context.parallelize([json.dumps(mem_obj)])
            data_df = spark.read.json(jsonRDD, multiLine=True)
            # Select only the required columns
            data_df = data_df.select("id", "fullName", "initials", "memberType",
                                     "url", "username", "status", "email", "idOrganizations")
            data_df.write.format('delta').mode("append").option(
                "mergeSchema", 'true').save(path)
        logger.success(
            f'Successfully added the members data to delta bronze table for the board {board_id}, {board_name}')
    except Exception as error:
        logger.exception(
            f'Exception while ingesting members data for the board {board_id}, {board_name}')
        raise error
