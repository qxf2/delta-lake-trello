"""
Module containing helper functions for use with Trello
"""

from trello import TrelloClient
from config import trello_conf as tc


def get_trello_client():
    """
    return trello_client object
    :return trello_client: Trello client object
    """
    try:
        trello_client = TrelloClient(
            api_key=tc.TRELLO_API_KEY,
            api_secret=tc.TRELLO_API_SECRET,
            token=tc.TRELLO_TOKEN
        )
    except Exception:
        raise Exception('Unable to create Trello client')

    return trello_client


def get_boards_list():
    """
    Get the list of all boards
    :return list of boards
    """
    try:
        client = get_trello_client()
        all_boards = client.list_boards()
        #log.warn('Got the list of all boards')
    except Exception as error:
        raise Exception('Unable to get list of boards') from error

    return all_boards


def get_board(board_id):
    """
    Get board object
    :param board_id: board_id
    :return board: board object
    """
    try:
        client = get_trello_client()
        board = client.get_board(board_id)
    except Exception as error:
        raise Exception('Unable to create board object') from error
    return board


def get_list_details(board_id):
    """
    Get list from board
    :param board_id: id of the board
    :return list_details: list dict
    """
    try:
        client = get_trello_client()
        json_obj = client.fetch_json(
            '/boards/' + board_id + '/lists',
            query_params={'cards': 'none'})
    except Exception as error:
        raise Exception('Unable to fetch list details') from error
    return json_obj


def extract_list_id(board_id, list_name):
    """
    Extracts the id of the list from list details json object
    :param board_id: id of the board
    :param list_name: the name of the list
    :return list_id: id extracted from list details object
    """
    try:
        list_details = get_list_details(board_id)
        extracted_list_details = list_details['id' == list_name]
        list_id = extracted_list_details['id']
        #log.info('Extracted list id')
    except Exception as error:
        raise Exception('Unable to extract list id') from error
    return list_id


def get_all_cards(board_id):
    """
    Fetches all cards of given board
    :param board_id: id of the board
    :return all_cards: all the cards of the board
    """
    try:
        board = get_board(board_id)
        all_cards = board.get_cards()
        #log.info('Fetched details of all cards of the board')
    except Exception as error:
        raise Exception('Unable to fetch all cards') from error
    return all_cards


def get_all_members(board_id):
    """
    Fetches all members of given board
    :param board_id: id of the board
    :return all_members: all the members of the board
    """
    try:
        board = get_board(board_id)
        all_members = board.get_members()
    except Exception as error:
        raise Exception('Unable to fetch all members') from error
    return all_members


def get_board_name(board_id):
    """
    Fetches the name of the board based on id
    :param board_id: id of the board
    :return board_name: name of the board
    """
    try:
        board = get_board(board_id)
        board_name = board.name
    except Exception as error:
        raise Exception('Unable to fetch name of the board') from error
    return board_name


def fetch_member_details(mem_id):
    """
    Fetches all members details of given board
    :param mem_id: id of the member
    :return member details json
    """
    try:
        client = get_trello_client()
        json_obj = client.fetch_json(
            '/members/' + mem_id.id, query_params={'badges': False}
        )
    except Exception as error:
        raise Exception('Unable to fetch details of member') from error
    return json_obj


def fetch_card_details(card_id):
    """
    Fetch all attributes for the card
    :param card_id: id of the card
    :return card details json
    """
    try:
        client = get_trello_client()
        json_obj = client.fetch_json(
            '/cards/' + card_id.id,
            query_params={'badges': False}
        )
    except Exception as error:
        raise Exception('Unable to fetch details of card') from error
    return json_obj
