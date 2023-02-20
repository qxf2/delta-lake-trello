"""
What does the script do:
    - Get all the cards a member has worked on
    - Filter the cars based on start & end date CLI params passed
    - Create a Gold table with the filtered cards
"""

import argparse
from enum import Enum
from datetime import datetime
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from pyspark.sql.functions import col
from helpers import spark_helper 
from config import delta_table_conf as conf

class Member(Enum):
    Shiva = "shivahari p"

class SilverTable(Enum):
    All = conf.cards_unique_silver_table_path

class GoldTable(Enum):
    Shiva = conf.shiva_cards_gold_path

class Aggregator():
    "Create Gold Delta Lake tables based on specific use case"
    def __init__(self, member:str, silver_table:str, gold_table:str, start_date:datetime=None, end_date:datetime=None):
        self.logger = logger
        self.spark, _ =  spark_helper.start_spark()
        self.member = member
        self.gold_table = gold_table
        self.silver_table = silver_table
        self.start_date = start_date
        self.end_date = end_date

    @property
    def member(self):
        return self._member

    @member.setter
    def member(self, value):
        self.logger.info(f"Setting {value} as member")
        self._member = value

    @property
    def gold_table(self):
        return self._gold_table

    @gold_table.setter
    def gold_table(self, value):
        self.logger.info(f"Setting {value} as Output Gold table location for {self.member}")
        self._gold_table = value

    @property
    def silver_table(self):
        return self._silver_table

    @silver_table.setter
    def silver_table(self, value):
        self.logger.info(f"Setting {value} as Input Silver table location for {self.member}")
        self._silver_table = value

    @property
    def start_date(self):
        return self._start_date

    @start_date.setter
    def start_date(self, value):
        if value:
            self.logger.info(f"Setting {value} as start_date")
            self._start_date = datetime.strptime(value, "%Y-%m-%d")
        else:
            self._start_date = None
            self.logger.warning(f"Start date is passed as None")

    @property
    def end_date(self):
        return self._end_date

    @end_date.setter
    def end_date(self, value):
        if value:
            self.logger.info(f"Setting {value} as end_date")
            self._end_date = datetime.strptime(value, "%Y-%m-%d")
        else:
            self._end_date = None
            self.logger.warning(f"End date is passed as None")
    
    def get_cards(self):
        """
        Get all cards based on:
            :self.member: Member contributed to the Trello card
            :self.start_date: Date from which the member started contributing
            :self.edn_date: Date until which the member contributed
        return:
            :cards: Trello cards Data Frame
        """
        try:
            self.logger.info(f"Gettings cards for member {self.member}")
            cards = self.spark.read.format("delta").load(self.silver_table)
            cards = cards.where(col("card_members").contains(self.member))
            if cards:
                self.logger.success(f"Successfully fetched cards for {self.member}")
            else:
                raise f"Unable to fetch cards for {self.member}"

            if self.start_date and self.end_date:
                cards = cards.where((col("dateLastActivity") >= self.start_date) & (col("dateLastActivity") <= self.end_date))
            
            return cards
        except Exception as err:
            self.logger.error(f"Unable to get cards due to {err}")

    def build_gold_table(self, cards):
        """
        Create a Gold table
        param:
            :cards: Trello cards Data Frame
        """
        try:
            cards.write.format("delta").mode("overwrite").option("mergeSchema","true").save(self.gold_table)
            self.logger.success(f"Successfully created Gold table for {self.member}")
        except Exception as err:
            self.logger.error(f"Unable to create Gold table due ti {err}")

if __name__ == "__main__":
 parser = argparse.ArgumentParser(description="Pass start & end date to create member gold delta table")
 parser.add_argument("--start_date",
                     metavar="string",
                     default=None,
                     help="start date in %Y-%m-%d format eg: 2021-12-31")
 parser.add_argument("--end_date",
                     metavar="string",
                     default=None,
                     help="end date in %Y-%m-%d format eg: 2022-12-31")
 args = parser.parse_args()
 aggregator = Aggregator(member = Member.Shiva.value,
                         silver_table=SilverTable.All.value,
                         gold_table=GoldTable.Shiva.value,
                         start_date=args.start_date,
                         end_date=args.end_date)
 cards = aggregator.get_cards()
 aggregator.build_gold_table(cards)
