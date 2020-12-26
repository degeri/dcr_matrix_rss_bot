from conf import *
import requests
import time
import feedparser
import sqlite3
import json
from log import *
from collections import namedtuple
from datetime import datetime, timezone
from time import mktime


ModAction = namedtuple("ModAction", [
    "id", "modname", "date", "platform", "place", "action"])


def minimal_username(name):
    return name.replace("/u/", "")


def drop_prefix(s, p):
    if s.startswith(p):
        return s.replace(p, "", 1)


REDDIT_ACTION_FIXES = {
    "approved"      : "approve",
    "banned"        : "ban",
    "distinguished" : "distinguish",
    "edited"        : "edit",
    "removed"       : "remove",
}


def replace_prefix(s, replacements):
    res = s
    for pref, repl in replacements.items():
        if s.startswith(pref):
            res = s.replace(pref, repl, 1)
            break

    return res


def mod_action_from_atom(entry):
    mid = entry["id"]
    modname = minimal_username(entry["authors"][0]["name"])
    stime = entry["updated_parsed"] # feedparser promises to return UTC
    date = datetime.utcfromtimestamp(mktime(stime))
    platform = "reddit"
    place = entry.tags[0]["term"]
    action = entry["title_detail"]["value"]
    action = drop_prefix(action, place + ": ")
    action = drop_prefix(action, modname + " ")
    action = replace_prefix(action, REDDIT_ACTION_FIXES)
    return ModAction(mid, modname, date, platform, place, action)


def mod_actions_from_atom(str_):
    feed = feedparser.parse(str_)
    return map(mod_action_from_atom, feed.entries)


def mod_action_from_json(obj):
    mid = obj["id"]
    modname = obj["mod"]
    created_unix = obj["created_utc"]
    date = datetime.utcfromtimestamp(created_unix)
    platform = "reddit"
    place = obj["subreddit"]
    action = obj["action"]
    # todo: use the extra stuff json offers
    return ModAction(mid, modname, date, platform, place, action)


def mod_actions_from_json(str_):
    feed = json.loads(str_)
    assert feed["kind"] == "Listing"
    children = feed["data"]["children"]
    mod_actions = []
    for c in children:
        if not c["kind"] == "modaction":
            logger.warning("unexpected kind: " + c["kind"])
            continue
        entry = c["data"]
        mod_actions.append(mod_action_from_json(entry))
    return mod_actions


def modlog_date(dt):
    """Format datetime according to modlog 0.11 spec."""
    return dt.isoformat(" ") + " UTC"


def format_mod_action(ma):
    return "{modname} {timestamp}; {platform} {place}; {action}".format(
        modname=ma.modname, timestamp=modlog_date(ma.date),
        platform=ma.platform, place=ma.place, action=ma.action)


def db_initialized(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='redditmodlog'")
    initialized = bool(cur.fetchone())
    cur.close()
    return initialized


def insert_mod_action(cursor, ma):
    cursor.execute("INSERT INTO redditmodlog VALUES (?,?,?,?,?)", (ma.id, ma.modname, ma.date.isoformat(" "), ma.place, ma.action))


def init_db(conn, mod_actions):
    cur = conn.cursor()
    cur.execute('CREATE TABLE "redditmodlog" ( `id` TEXT, `modname` TEXT, `updated` TEXT, `place` TEXT, `action` TEXT, PRIMARY KEY(`id`) )')
    conn.commit()
    logger.info("initialized database redditmodlog")
    for ma in mod_actions:
        insert_mod_action(cur, ma)
    conn.commit()
    cur.close()


def fetch(url):
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.text
    else:
        # todo: handle 429 Too Many Requests
        logger.warning("response status code: " + str(resp.status_code))
        return None


def reddit_mod_log():
    mod_log_db_name = config["redditmodlog"]["dbname"] + ".sqlite"

    mode = config["redditmodlog"]["mode"]
    if mode == "json":
        mod_log_url = config["redditmodlog"]["json_url"]
        converter = mod_actions_from_json
    elif mode == "atom":
        mod_log_url = config["redditmodlog"]["atom_url"]
        converter = mod_actions_from_atom
    else:
        raise Exception("unexpected mode: " + mode)

    resp = fetch(mod_log_url)
    if not resp:
        return
    mod_actions = converter(resp)

    db_conn = sqlite3.connect(mod_log_db_name)

    if not db_initialized(db_conn):
        init_db(db_conn, mod_actions)
        db_conn.close()
        return

    db_cur = db_conn.cursor()

    for ma in mod_actions:
        db_cur.execute("SELECT * from redditmodlog WHERE id=?", (ma.id,))
        # mod action not found in the db means it is new
        if not db_cur.fetchall():
            insert_mod_action(db_cur, ma)
            db_conn.commit()
            msg = json.dumps(format_mod_action(ma))[1:-1]
            send_matrix_msg(msg)
            logger.info("Sending:" + msg)

    db_conn.close()


def send_matrix_msg(msg):
    token = config["matrixconfig"]["accesstoken"]
    roomid = config["matrixconfig"]["roomid"]
    server_url = config["matrixconfig"]["server_url"]
    data = '{"msgtype":"m.text", "body":"' + msg + '"}'
    r = requests.post(server_url + "_matrix/client/r0/rooms/" + roomid + "/send/m.room.message?access_token=" + token, data = data)
