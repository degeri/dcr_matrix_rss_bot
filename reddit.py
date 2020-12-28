from collections import namedtuple
from datetime import datetime
import json
import sqlite3
import time
from time import mktime
from urllib.parse import urlparse

import feedparser
import requests

import conf
from log import logger


CONFIG = conf.config["redditmodlog"]
REDDIT_BASE = "https://www.reddit.com"
FETCH_RETRY_SECONDS = 8
FETCH_ATTEMPTS = 5


ModAction = namedtuple("ModAction", [
    "id", "modname", "timestamp", "platform", "place", "action", "raw_action",
    "object", "reason"])


def minimal_username(name):
    return name.replace("/u/", "")


def short_link(permalink):
    parsed = urlparse(permalink)
    parts = parsed.path.split("/")
    if len(parts) == 7:
        postid = parts[4]
        return "{}/comments/{}/".format(REDDIT_BASE, postid)
    elif len(parts) == 8:
        postid = parts[4]
        commentid = parts[6]
        return "{}/comments/{}/_/{}/".format(REDDIT_BASE, postid, commentid)
    else:
        logger.warning("unexpected permalink: " + permalink)
        return permalink


REDDIT_ACTION_FIXES = {
    "approved"      : "approve",
    "banned"        : "ban",
    "distinguished" : "distinguish",
    "edited"        : "edit",
    "removed"       : "remove",
    "stickied"      : "sticky",
    "unstickied"    : "unsticky",
    "spam"          : "remove",
}


REDDIT_ACTION_OBJECTS = {
    "banuser"       : ("ban", "user"),
    "unbanuser"     : ("unban", "user"),
    "spamlink"      : ("remove", "post"),
    "removelink"    : ("remove", "post"),
    "approvelink"   : ("approve", "post"),
    "spamcomment"   : ("remove", "comment"),
    "removecomment" : ("remove", "comment"),
    "approvecomment": ("approve", "comment"),
    "distinguish"   : ("distinguish", "comment"),
    "sticky"        : ("sticky", "comment"),
    "editflair"     : ("edit", "flair for post"),
    "wikirevise"    : ("edit", "wiki"),
    "createrule"    : ("create", "rule"),
    "editrule"      : ("edit", "rule"),
}


def drop_prefix(s, p):
    if s.startswith(p):
        return s.replace(p, "", 1)


def action_object_atom(ao):
    action = ao
    object_ = ""
    for wrong, fixed in REDDIT_ACTION_FIXES.items():
        if ao.startswith(wrong + " "):
            action = fixed
            object_ = ao.replace(wrong + " ", "", 1)
            break

    return action, object_


def mod_action_from_atom(entry):
    mid = entry["id"]
    modname = minimal_username(entry["authors"][0]["name"])
    stime = entry["updated_parsed"] # feedparser promises to return UTC
    timestamp = int(mktime(stime))
    platform = "reddit"
    place = entry.tags[0]["term"]
    actobj = entry["title_detail"]["value"]
    actobj = drop_prefix(actobj, place + ": ")
    actobj = drop_prefix(actobj, modname + " ")
    act, obj = action_object_atom(actobj)
    return ModAction(mid, modname, timestamp, platform, place, act, "", obj,
        "")


def mod_actions_from_atom(str_):
    feed = feedparser.parse(str_)
    return map(mod_action_from_atom, feed.entries)


def mod_action_from_json(obj):
    # get required keys with obj[] to trigger KeyErrors
    mid = obj["id"]
    modname = obj["mod"]
    timestamp = obj["created_utc"]
    platform = "reddit"
    place = obj["subreddit"]
    action = obj["action"]
    faction, objtype = REDDIT_ACTION_OBJECTS.get(action, (action, ""))

    # get optional fields with obj.get(), mind that it can return None
    # if the key exists but has an explicit `None` value
    title = obj.get("target_title")
    ftitle = '"' + title + '"' if title else ""
    author = obj.get("target_author")
    addby = (objtype == "post"
             or objtype == "comment"
             or objtype == "flair for post")
    fauthor = "by " + author if (author and addby) else author
    permalink = obj.get("target_permalink")
    fpermalink = short_link(permalink) + " " if permalink else ""
    details = obj.get("details")
    fdetails = details if details else ""
    desc = obj.get("description")
    fdesc = desc if desc else ""

    if action == "distinguish":
        reason = obj.get("target_body")
    elif action == "editrule" or action == "createrule":
        ftitle = '"' + fdetails + '"'
        reason = fdesc
    else:
        reason = (fdetails + ": " + fdesc if (fdetails and fdesc)
                  else fdetails + fdesc)

    fobject = " ".join(filter(bool, [objtype, fauthor, ftitle, fpermalink]))
    return ModAction(mid, modname, timestamp, platform, place, faction, action,
        fobject, reason)


def mod_actions_from_json(str_):
    try:
        feed = json.loads(str_)
    except ValueError as e:
        logger.error("malformed JSON")
        return []
    try:
        children = feed["data"]["children"]
        mod_actions = []
        for c in children:
            if not c["kind"] == "modaction":
                logger.warning("unexpected kind: " + c["kind"])
                continue
            entry = c["data"]
            try:
                ma = mod_action_from_json(entry)
            except KeyError as e:
                logger.error("skipping malformed Reddit modaction: "
                             + json.dumps(entry))
                continue
            mod_actions.append(ma)
        return mod_actions
    except KeyError as e:
        logger.error("malformed Reddit modaction Listing, missing key "
                     + str(e))
        return []


def filter_mod_actions(mas):
    return filter(lambda ma: not ma.raw_action == "editflair", mas)


def format_timestamp(ts):
    return datetime.utcfromtimestamp(ts).isoformat(" ") + " UTC"


def format_mod_action(ma):
    s = ("{modname} {timestamp}; {platform} {place};"
         " {action} {object}{reason}").format(
            modname=ma.modname,
            timestamp=format_timestamp(ma.timestamp),
            platform=ma.platform,
            place=ma.place,
            action=ma.action,
            object=ma.object,
            reason="; " + ma.reason if ma.reason else "")
    return s


def db_initialized(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master"
                " WHERE type='table' AND name='redditmodlog'")
    initialized = bool(cur.fetchone())
    cur.close()
    return initialized


def insert_mod_action(cursor, ma):
    cursor.execute('INSERT INTO redditmodlog VALUES (?,?,?,?,?,?,?)',
        (ma.id, ma.modname, ma.timestamp, ma.place, ma.action, ma.object,
            ma.reason))


def init_db(conn, mod_actions):
    cur = conn.cursor()
    cur.execute('CREATE TABLE redditmodlog ('
                '    "id"           TEXT,'
                '    "modname"      TEXT,'
                '    "timestamp"    INTEGER,'
                '    "place"        TEXT,'
                '    "action"       TEXT,'
                '    "object"       TEXT,'
                '    "reason"       TEXT,'
                '    PRIMARY KEY ("id")'
                ')')
    conn.commit()
    logger.info("initialized database redditmodlog")
    for ma in mod_actions:
        insert_mod_action(cur, ma)
    conn.commit()
    cur.close()


def fetch(url):
    retries = 0
    while retries <= FETCH_ATTEMPTS:
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.text
        else:
            logger.warning("response status {}, retrying in {} s".format(
                str(resp.status_code), FETCH_RETRY_SECONDS))
            retries += 1
            time.sleep(FETCH_RETRY_SECONDS)
    return None


def reddit_mod_log():
    mod_log_db_file = CONFIG["dbfile"]

    mode = CONFIG["mode"]
    if mode == "json":
        mod_log_url = CONFIG["json_url"]
        converter = mod_actions_from_json
    elif mode == "atom":
        mod_log_url = CONFIG["atom_url"]
        converter = mod_actions_from_atom
    else:
        raise Exception("unexpected mode: " + mode)

    resp = fetch(mod_log_url)
    if not resp:
        return []
    mod_actions = filter_mod_actions(converter(resp))

    db_conn = sqlite3.connect(mod_log_db_file)

    if not db_initialized(db_conn):
        init_db(db_conn, mod_actions)
        db_conn.close()
        return []

    db_cur = db_conn.cursor()
    records = []

    for ma in mod_actions:
        db_cur.execute('SELECT * FROM redditmodlog WHERE "id"=?', (ma.id,))
        # mod action not found in the db means it is new
        if not db_cur.fetchall():
            insert_mod_action(db_cur, ma)
            db_conn.commit()
            records.append(format_mod_action(ma))

    db_conn.close()
    return records
