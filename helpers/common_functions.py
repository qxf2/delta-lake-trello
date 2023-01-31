"""
This module contains common functions 
"""

import re
import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import helpers.trello_functions as tf

def clean_board_date(date: str):
    """
    Cleans up date format in the sprint board name
    """
    if date is None:
        return date
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    date_arr = date.split("-")
    day = date_arr[0]
    extracted_month = date_arr[1]
    if len(date_arr) < 3:
        year = date[-4:]
    else:
        year = date_arr[2]
    for month in months:
        if extracted_month.lower().startswith(month.lower()):
            return day + "-" + month + "-" + year
    return None


def get_current_sprint_board():
    """
    Gets the board id of the current sprint
    :return board id: id of the current sprint board
    :return board_name: name of the current sprint board
    """
    date_board_map = {}
    date_list = []

    try:
        all_boards = tf.get_boards_list()
        for board in all_boards:
            arr = re.findall(r"\((.*?)\)", board.name)
            if len(arr) > 0:
                date = arr[0]
                cleaned_date = clean_board_date(date)
                if cleaned_date is None:
                    continue
                date_list.append(cleaned_date)
                date_board_map[cleaned_date] = board
        sorted(date_list, key=lambda x: datetime.datetime.strptime(x, '%d-%b-%Y'))
        current_board_id = date_board_map[date_list[-1]].id
        current_board_name = date_board_map[date_list[-1]].name
    except Exception as error:
        raise Exception('Unable to get list of boards') from error

    return current_board_id, current_board_name
