import json
import time

import requests

import conf
from log import logger
from utils import json_compact


CONFIG = conf.config["matrixconfig"]


# SPEC: https://matrix.org/docs/spec/client_server/r0.6.0


def message(body):
    return json_compact({"msgtype": "m.text", "body": body})


def send_message(msg):
    token = CONFIG["accesstoken"]
    roomid = CONFIG["roomid"]
    server_url = CONFIG["server_url"]
    txid = "m" + str(int(time.time()))
    url = "{}_matrix/client/r0/rooms/{}/send/m.room.message/{}".format(
            server_url, roomid, txid)
    headers = {"Authorization": "Bearer " + token}
    data = message(msg)
    logger.info("sending: " + msg)
    r = requests.put(url, data=data, headers=headers)
    return r
