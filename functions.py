from conf import *
import requests
import time
import feedparser
import os
import sqlite3
import json
from log import *
from collections import namedtuple
from datetime import datetime, timezone


ModAction = namedtuple("ModAction", [
    "id", "modname", "modname_pretty", "date", "date_pretty", "action"])


def pretty_date(ds):
    assert ds[22] == ":"
    # hack to make it parseable by strptime
    parseable = ds[:22] + ds[23:]
    dt = datetime.strptime(parseable, "%Y-%m-%dT%H:%M:%S%z")
    ds2 = dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat(" ") + " UTC"
    return ds2


def clean_name(name):
    return name.replace("/u/", "")


def mod_action_from_rss(entry):
    mid = entry["id"]
    modname = entry["authors"][0]["name"]
    modname_pretty = clean_name(modname)
    date = entry["updated"]
    date_pretty = pretty_date(date)
    action = entry["title_detail"]["value"]
    return ModAction(mid, modname, modname_pretty, date, date_pretty, action)


def reddit_mod_log():
    mod_log_db_name = config["redditmodlog"]["dbname"] + ".sqlite"

    mode = config["redditmodlog"]["mode"]
    if mode == "rss":
        mod_log_url = config["redditmodlog"]["rss_url"]
        feedobject = feedparser.parse(mod_log_url)
        mod_actions = map(mod_action_from_rss, feedobject.entries)
    else:
        raise Exception("unexpected mode: " + mode)

    postnow = True
    if not os.path.isfile(mod_log_db_name):
        postnow = False
        db_connection = sqlite3.connect(mod_log_db_name)
        db = db_connection.cursor()
        db.execute('CREATE TABLE "redditmodlog" ( `id` TEXT, `modname` TEXT, `updated` TEXT, `action` TEXT, PRIMARY KEY(`id`) )')
        db_connection.close()

    db_connection = sqlite3.connect(mod_log_db_name)
    db = db_connection.cursor()
    for ma in mod_actions:
        db.execute("SELECT * from redditmodlog WHERE id=?", (ma.id,))
        if not db.fetchall():
            db.execute("INSERT INTO redditmodlog VALUES (?,?,?,?)", (ma.id, ma.modname, ma.date, ma.action))
            db_connection.commit()
            if postnow:
                msg = json.dumps(ma.modname_pretty + " " + ma.date_pretty + "; reddit decred; " + ma.action)[1:-1]
                send_matrix_msg(msg)
                logger.info("Sending:" + msg)
    db_connection.close()


def send_matrix_msg(msg):
    token = config["matrixconfig"]["accesstoken"]
    roomid = config["matrixconfig"]["roomid"]
    server_url = config["matrixconfig"]["server_url"]
    data = '{"msgtype":"m.text", "body":"' + msg + '"}'
    r = requests.post(server_url + "_matrix/client/r0/rooms/" + roomid + "/send/m.room.message?access_token=" + token, data = data)
