import json

import requests

from conf import config
from log import logger


def message(body):
    return json.dumps({"msgtype": "m.text", "body": body})


def send_message(msg):
    token = config["matrixconfig"]["accesstoken"]
    roomid = config["matrixconfig"]["roomid"]
    server_url = config["matrixconfig"]["server_url"]
    url = ("{}_matrix/client/r0/rooms/{}/send/m.room.message"
           "?access_token={}").format(server_url, roomid, token)
    data = message(msg)
    logger.info("sending: " + msg)
    r = requests.post(url, data=data)
