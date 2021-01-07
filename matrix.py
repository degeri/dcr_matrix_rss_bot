import json
import time

import requests

import conf
from log import logger
from utils import json_compact


CONFIG = conf.config["matrixconfig"]


# SPEC: https://matrix.org/docs/spec/client_server/r0.6.0


def message(body, formatted_body=None):
    if formatted_body:
        msg = {"msgtype": "m.text",
               "body": body,
               "format": "org.matrix.custom.html",
               "formatted_body": formatted_body}
    else:
        msg = {"msgtype": "m.text", "body": body}
    return json_compact(msg)


def send_message(msg, formatted_msg=None):
    token = CONFIG["accesstoken"]
    roomid = CONFIG["roomid"]
    server_url = CONFIG["server_url"]
    txid = "m" + str(int(time.time()))
    url = "{}_matrix/client/r0/rooms/{}/send/m.room.message/{}".format(
            server_url, roomid, txid)
    headers = {"Authorization": "Bearer " + token}
    data = message(msg, formatted_msg)
    logger.info("sending: " + msg)
    r = requests.put(url, data=data, headers=headers)
    return r
