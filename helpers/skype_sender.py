"""
This script is used to send message to provided Skype channel
"""

import os
import sys
import requests
from loguru import logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import skype_conf as sc

def post_message_on_skype(message):
    """
    Posts a message on the set Skype channel
    :param message: text
    """
    try:
        headers = {"Content-Type": "application/json"}
        payload = {
            "msg": message,
            "channel": sc.SKYPE_CHANNEL,
            "API_KEY": sc.SKYPE_API_KEY,
        }

        response = requests.post(
            url=sc.SKYPE_URL,
            json=payload,
            headers=headers,
        )
        if response.status_code == 200:
            logger.info(f"Successfully sent the Skype message - {message}")
        else:
            logger.info("Failed to send Skype message", level="error")
    except Exception as err:
        raise Exception(f"Unable to post message to Skype channel, due to {err}")
