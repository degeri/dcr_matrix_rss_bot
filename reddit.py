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
BASE_URL = "https://www.reddit.com"
FETCH_RETRY_SECONDS = 8
FETCH_RETRIES = 5


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
        return "{}/comments/{}/".format(BASE_URL, postid)
    elif len(parts) == 8:
        postid = parts[4]
        commentid = parts[6]
        return "{}/comments/{}/_/{}/".format(BASE_URL, postid, commentid)
    else:
        logger.warning("unexpected permalink: " + permalink)
        return permalink


MOD_ACTION_FIXES = {
    "approved"      : "approve",
    "banned"        : "ban",
    "distinguished" : "distinguish",
    "edited"        : "edit",
    "removed"       : "remove",
    "stickied"      : "sticky",
    "unstickied"    : "unsticky",
    "spam"          : "remove",
}


def extract_id_atom(idstr):
    path = urlparse(idstr).path
    start = path.find("ModAction")
    if start < 0: # should never be True
        logger.warn("something is wrong with this Atom entry id" + idstr)
        return idstr
    return path[start:]


def drop_prefix(s, p):
    if s.startswith(p):
        return s.replace(p, "", 1)


def split_action_atom(ao):
    action = ao
    object_ = ""
    for wrong, fixed in MOD_ACTION_FIXES.items():
        if ao.startswith(wrong + " "):
            action = fixed
            object_ = ao.replace(wrong + " ", "", 1)
            break

    return action, object_


def mod_action_from_atom(entry):
    mid = extract_id_atom(entry["id"])
    modname = minimal_username(entry["authors"][0]["name"])
    stime = entry["updated_parsed"] # feedparser promises to return UTC
    timestamp = int(mktime(stime))
    platform = "reddit"
    place = entry.tags[0]["term"]
    actobj = entry["title_detail"]["value"]
    actobj = drop_prefix(actobj, place + ": ")
    actobj = drop_prefix(actobj, modname + " ")
    act, obj = split_action_atom(actobj)
    return ModAction(mid, modname, timestamp, platform, place, act, "", obj,
        "")


def mod_actions_from_atom(str_):
    feed = feedparser.parse(str_)
    return map(mod_action_from_atom, feed.entries)


MOD_ACTIONS_OBJECTS = {
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


def mod_action_from_json(obj):
    # get required keys with obj[] to trigger KeyErrors
    mid = obj["id"]
    modname = obj["mod"]
    timestamp = obj["created_utc"]
    platform = "reddit"
    place = obj["subreddit"]
    action = obj["action"]
    faction, objtype = MOD_ACTIONS_OBJECTS.get(action, (action, ""))

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


def newest_mod_action(mod_actions):
    return max(mod_actions, key=lambda ma: ma.timestamp)


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


DB_SCHEMA_VERSION = 2


def table_exists(cur, table):
    cur.execute("SELECT name FROM sqlite_master"
                " WHERE type='table' AND name='{}'".format(table))
    return bool(cur.fetchone())


def db_initialized(cur):
    modlog_table = "redditmodlog"
    modlog_table_exists = table_exists(cur, modlog_table)
    meta_table = "redditmodlog_meta"
    meta_table_exists = table_exists(cur, meta_table)
    if modlog_table_exists != meta_table_exists:
        raise Exception("bad db state: tables {} and {} must either both exist"
                        " or not exist".format(modlog_table, meta_table))
    if meta_table_exists:
        cur.execute('SELECT "value" FROM {}'
                    ' WHERE "key"=\'schema_version\''.format(meta_table))
        verrow = cur.fetchone()
        ver = int(verrow[0]) if verrow else None
        if ver != DB_SCHEMA_VERSION:
            raise Exception("unsupported schema version, expected {} but"
                            " got {}".format(DB_SCHEMA_VERSION, ver))
    return modlog_table_exists


def update_newest_mod_action(conn, mod_actions):
    if mod_actions:
        newest = newest_mod_action(mod_actions)
        cur = conn.cursor()
        cur.execute('UPDATE redditmodlog_meta SET "value"=(?)'
                    ' WHERE "key"=\'newest_modaction_id\'', (newest.id,))
        conn.commit()


def init_db(conn):
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
    cur.execute('CREATE TABLE redditmodlog_meta('
                '    "key"          TEXT,'
                '    "value"        TEXT'
                ')')
    cur.executemany('INSERT INTO redditmodlog_meta VALUES (?,?)', [
        ("schema_version", str(DB_SCHEMA_VERSION)),
        ("newest_modaction_id", ""),
    ])
    conn.commit()
    logger.info("initialized database redditmodlog")
    cur.close()


def mod_action_exists(cursor, mid):
    cursor.execute('SELECT "id" FROM redditmodlog WHERE "id"=?', (mid,))
    return bool(cursor.fetchone())


def insert_mod_action(cursor, ma):
    cursor.execute('INSERT INTO redditmodlog VALUES (?,?,?,?,?,?,?)',
        (ma.id, ma.modname, ma.timestamp, ma.place, ma.action, ma.object,
            ma.reason))


def fetch(url):
    retries = 0
    while retries <= FETCH_RETRIES:
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.text
        else:
            logger.warning("response status {}, retrying in {} s".format(
                str(resp.status_code), FETCH_RETRY_SECONDS))
            retries += 1
            time.sleep(FETCH_RETRY_SECONDS)
    logger.warning("failed to fetch with {} retries, giving up this"
                   " request".format(FETCH_RETRIES))
    return None


def new_modlog_records():
    db_file = CONFIG["dbfile"]

    mode = CONFIG["mode"]
    if mode == "json":
        url = CONFIG["json_url"]
        converter = mod_actions_from_json
    elif mode == "atom":
        url = CONFIG["atom_url"]
        converter = mod_actions_from_atom
    else:
        raise Exception("unexpected mode: " + mode)

    resp = fetch(url)
    if not resp:
        return []
    mod_actions = list(converter(resp))
    mod_actions_filtered = list(filter_mod_actions(mod_actions))

    db_conn = sqlite3.connect(db_file)
    db_cur = db_conn.cursor()

    if not db_initialized(db_cur):
        init_db(db_conn)
        for ma in mod_actions_filtered:
            insert_mod_action(db_cur, ma)
        update_newest_mod_action(db_conn, mod_actions) # commits
        db_conn.close()
        return [] # nothing "new" on the first run

    records = []

    for ma in mod_actions_filtered:
        if not mod_action_exists(db_cur, ma.id):
            insert_mod_action(db_cur, ma)
            db_conn.commit()
            records.append(format_mod_action(ma))

    # mind that we use an _unfiltered_ fetch result to find the newest seen
    # mod action, to avoid re-checking filtered-out items next time
    update_newest_mod_action(db_conn, mod_actions) # commits

    db_conn.close()
    return records
