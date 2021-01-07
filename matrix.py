import json

import requests

import conf
from log import logger


CONFIG = conf.config["matrixconfig"]


def message(body):
    return json.dumps({"msgtype": "m.text", "body": body})


def send_message(msg):
    token = CONFIG["accesstoken"]
    roomid = CONFIG["roomid"]
    server_url = CONFIG["server_url"]
    url = "{}_matrix/client/r0/rooms/{}/send/m.room.message".format(
            server_url, roomid)
    headers = {"Authorization": "Bearer " + token}
    data = message(msg)
    logger.info("sending: " + msg)
    r = requests.post(url, data=data, headers=headers)
