from collections import namedtuple
from contextlib import closing
from datetime import datetime
import json
import sqlite3
import time
from time import mktime
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import feedparser
import requests

import conf
from log import logger


CONFIG = conf.config["redditmodlog"]
BASE_URL = "https://www.reddit.com"
FETCH_RETRY_SECONDS = 8
FETCH_RETRIES = 5


ModAction = namedtuple("ModAction", [
    "id", "timestamp", "modname", "platform", "place", "action", "object",
    "details", "r_action", "raw"])


def minimal_username(name):
    return name.replace("/u/", "")


def short_md_link(permalink):
    parsed = urlparse(permalink)
    parts = parsed.path.split("/")
    if len(parts) == 7:
        postid = parts[4]
        return "[{}]({}/comments/{}/)".format(postid, BASE_URL, postid)
    elif len(parts) == 8:
        postid = parts[4]
        commentid = parts[6]
        return "[{}:{}]({}/comments/{}/_/{}/)".format(postid, commentid,
            BASE_URL, postid, commentid)
    else:
        logger.warning("unexpected permalink: " + permalink)
        return permalink


MOD_ACTION_FIXES_ATOM = {
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
    object = ""
    for wrong, fixed in MOD_ACTION_FIXES_ATOM.items():
        if ao.startswith(wrong + " "):
            action = fixed
            object = ao.replace(wrong + " ", "", 1)
            break

    return action, object


def mod_action_from_atom(entry):
    mid = extract_id_atom(entry["id"])
    stime = entry["updated_parsed"] # feedparser promises to return UTC
    timestamp = int(mktime(stime))
    modname = minimal_username(entry["authors"][0]["name"])
    platform = "reddit"
    place = entry.tags[0]["term"]
    actobj = entry["title_detail"]["value"]
    actobj = drop_prefix(actobj, place + ": ")
    actobj = drop_prefix(actobj, modname + " ")
    action, object = split_action_atom(actobj)
    details = ""
    r_action = ""
    raw = ""
    return ModAction(mid, timestamp, modname, platform, place, action, object,
        details, r_action, raw)


def mod_actions_from_atom(str_):
    feed = feedparser.parse(str_)
    return map(mod_action_from_atom, feed.entries)


MOD_ACTIONS_OBJTYPES = {
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
    timestamp = int(obj["created_utc"])
    modname = obj["mod"]
    platform = "reddit"
    place = obj["subreddit"]
    r_action = obj["action"]
    action, objtype = MOD_ACTIONS_OBJTYPES.get(r_action, (r_action, ""))

    # get optional fields with obj.get(), mind that it can return None
    # if the key exists but has an explicit `None` value
    r_title = obj.get("target_title")
    title = '"' + r_title + '"' if r_title else ""
    r_author = obj.get("target_author")
    addby = (objtype == "post"
             or objtype == "comment"
             or objtype == "flair for post")
    author = "by " + r_author if (r_author and addby) else r_author
    r_permalink = obj.get("target_permalink")
    md_link = "(" + short_md_link(r_permalink) + ")" if r_permalink else ""
    r_details = obj.get("details")
    fdetails = r_details if r_details else ""
    r_desc = obj.get("description")
    fdesc = r_desc if r_desc else ""

    if r_action == "distinguish":
        details = obj.get("target_body")
    elif r_action == "editrule" or r_action == "createrule":
        title = '"' + fdetails + '"'
        details = fdesc
    else:
        details = (fdetails + ": " + fdesc if (fdetails and fdesc)
                   else fdetails + fdesc)

    object = " ".join(filter(bool, [objtype, author, title, md_link]))
    return ModAction(mid, timestamp, modname, platform, place, action,
        object, details, r_action, obj)


def mod_actions_from_json(str):
    try:
        feed = json.loads(str)
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
    return filter(lambda ma: not ma.r_action == "editflair", mas)


def newest_mod_action(mod_actions):
    return max(mod_actions, key=lambda ma: ma.timestamp)


def format_timestamp(ts):
    return datetime.utcfromtimestamp(ts).isoformat(" ") + " UTC"


def format_mod_action(ma):
    s = ("{modname} {timestamp}; {platform} {place};"
         " {action} {object}{details}").format(
            modname=ma.modname,
            timestamp=format_timestamp(ma.timestamp),
            platform=ma.platform,
            place=ma.place,
            action=ma.action,
            object=ma.object,
            details="; " + ma.details if ma.details else "")
    return s


DB_SCHEMA_VERSION = 4
RAW_DB_SCHEMA_VERSION = 1


def table_exists(cur, table):
    cur.execute("SELECT name FROM sqlite_master"
                " WHERE type='table' AND name='{}'".format(table))
    return bool(cur.fetchone())


def get_db_value(cur, sql, params=(), converter=None):
    cur.execute(sql, params)
    row = cur.fetchone()
    val = row[0] if row else None
    return converter(val) if (converter and val is not None) else val


def get_meta_value(cur, key, converter):
    return get_db_value(cur,
        'SELECT "value" FROM redditmodlog_meta WHERE "key"=?', (key,),
        converter)


def int_or_none(x):
    return int(x) if x != "" else None


def set_meta_value(cur, key, val):
    cur.execute('UPDATE redditmodlog_meta SET "value"=? WHERE "key"=?',
                (val, key))


def db_initialized(cur):
    modlog_table = "redditmodlog"
    modlog_table_exists = table_exists(cur, modlog_table)
    meta_table = "redditmodlog_meta"
    meta_table_exists = table_exists(cur, meta_table)
    if modlog_table_exists != meta_table_exists:
        raise Exception("bad db state: tables {} and {} must either both exist"
                        " or not exist".format(modlog_table, meta_table))
    if meta_table_exists:
        ver = get_meta_value(cur, "schema_version", int_or_none)
        if ver != DB_SCHEMA_VERSION:
            raise Exception("unsupported schema version {},"
                            " expected {}".format(ver, DB_SCHEMA_VERSION))
    return modlog_table_exists

def raw_db_initialized(cur):
    table = "redditmodlog_raw"
    exists = table_exists(cur, table)
    if exists:
        ver = get_db_value(cur, "PRAGMA user_version", (), int_or_none)
        if ver != RAW_DB_SCHEMA_VERSION:
            raise Exception("unsupported schema version for table {}: found {}"
                            " but expected {}".format(table, ver,
                                RAW_DB_SCHEMA_VERSION))
    return exists


def get_newest_mod_action_idts(cur):
    newest_id = get_meta_value(cur, "newest_modaction_id", str)
    newest_ts = get_meta_value(cur, "newest_modaction_timestamp", int_or_none)
    return newest_id, newest_ts


def update_newest_mod_action(conn, mod_actions):
    if mod_actions:
        candidate = newest_mod_action(mod_actions)
        cur = conn.cursor()
        newest_id, newest_ts = get_newest_mod_action_idts(cur)
        if newest_ts and candidate.timestamp < newest_ts:
            logger.warning("not updating newest mod action as the candidate"
                           " with id={} and timestamp={} is OLDER than the"
                           " current one with id={} and timestamp={}".format(
                           candidate.id, candidate.timestamp,
                           newest_id, newest_ts))
            return
        elif (newest_ts and candidate.timestamp == newest_ts
              and candidate.id == newest_id):
            logger.warning("not updating newest mod action with identical"
                           " id={} and timestamp={}. Bug?".format(
                           newest_id, newest_ts))
            return
        set_meta_value(cur, "newest_modaction_id", candidate.id)
        set_meta_value(cur, "newest_modaction_timestamp", candidate.timestamp)
        conn.commit()


def init_db(conn):
    cur = conn.cursor()
    cur.execute('CREATE TABLE redditmodlog ('
                '    "id"           TEXT,'
                '    "timestamp"    INTEGER,'
                '    "modname"      TEXT,'
                '    "place"        TEXT,'
                '    "action"       TEXT,'
                '    "object"       TEXT,'
                '    "details"      TEXT,'
                '    PRIMARY KEY ("id")'
                ')')
    cur.execute('CREATE TABLE redditmodlog_meta('
                '    "key"          TEXT,'
                '    "value"        TEXT'
                ')')
    cur.executemany('INSERT INTO redditmodlog_meta VALUES (?,?)', [
        ("schema_version", str(DB_SCHEMA_VERSION)),
        ("newest_modaction_id", ""),
        ("newest_modaction_timestamp", ""),
    ])
    conn.commit()
    logger.info("initialized database redditmodlog")
    cur.close()


def init_raw_db(conn):
    cur = conn.cursor()
    cur.execute('CREATE TABLE redditmodlog_raw ('
                '    "id"           TEXT,'
                '    "timestamp"    INTEGER,'
                '    "data"         TEXT,'
                '    PRIMARY KEY ("id")'
                ')')
    cur.execute("PRAGMA user_version = " + str(RAW_DB_SCHEMA_VERSION))
    conn.commit()
    logger.info("initialized database redditmodlog_raw")
    cur.close()


def mod_action_exists(cur, mid):
    cur.execute('SELECT "id" FROM redditmodlog WHERE "id"=?', (mid,))
    return bool(cur.fetchone())


def insert_mod_action(cur, ma):
    cur.execute('INSERT INTO redditmodlog VALUES (?,?,?,?,?,?,?)',
        (ma.id, ma.timestamp, ma.modname, ma.place, ma.action, ma.object,
         ma.details))


def insert_raw_mod_action(cur, ma):
    raw = json.dumps(ma.raw, separators=(",", ":"), sort_keys=True)
    cur.execute('INSERT OR IGNORE INTO redditmodlog_raw VALUES (?,?,?)',
        (ma.id, ma.timestamp, raw))


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


def replace_query_param(url, param, value):
    urlp = urlparse(url)
    qsp = parse_qs(urlp.query, keep_blank_values=True)
    qsp[param] = value
    qs = urlencode(qsp, doseq=True)
    return urlunparse(urlp._replace(query=qs))


def save_raw(mod_actions, db_file):
    with closing(sqlite3.connect(db_file)) as conn:
        cur = conn.cursor()
        if not raw_db_initialized(cur):
            init_raw_db(conn)
        for ma in mod_actions:
            insert_raw_mod_action(cur, ma)
        conn.commit()


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

    db_conn = sqlite3.connect(db_file)
    db_cur = db_conn.cursor()

    first_run = not db_initialized(db_cur)
    if first_run:
        init_db(db_conn)
        newest_id, newest_ts = None, None
    else:
        newest_id, newest_ts = get_newest_mod_action_idts(db_cur)

    url2 = (replace_query_param(url, "before", newest_id)
            if newest_id else url)
    resp = fetch(url2)
    if not resp:
        db_conn.close()
        return [] # could not fetch anything, try again later

    mod_actions = list(converter(resp))

    if mode == "json" and conf.enabled(CONFIG["json_save_raw"]):
        save_raw(mod_actions, CONFIG["json_raw_dbfile"])

    mod_actions_filtered = list(filter_mod_actions(mod_actions))

    if first_run:
        for ma in mod_actions_filtered:
            insert_mod_action(db_cur, ma)
        update_newest_mod_action(db_conn, mod_actions) # commits
        db_conn.close()
        return [] # nothing "new" on the first run

    records = []

    for ma in mod_actions_filtered:
        exists = mod_action_exists(db_cur, ma.id)
        # use < to consider mod actions occurred same second as the newest
        # seen one. Note that newest_ts may be empty!
        older = ma.timestamp < newest_ts if newest_ts else False

        if not exists:
            if older:
                logger.warning("fetched mod action is older than the newest"
                               " seen one AND is missing from the db,"
                               " saving: " + str(ma))
            insert_mod_action(db_cur, ma)
            db_conn.commit()
            records.append(format_mod_action(ma))
        else: # exists
            # ideally report a diff with db version
            if older:
                logger.warning("fetched mod action id exists in the db and its"
                               " timestamp is older than the newest seen one."
                               " Keeping db version and ignoring the fetched"
                               " one: " + str(ma))
            else:
                # maybe update the row but log previous version first
                logger.warning("fetched mod action id exists in the db BUT its"
                               " timestamp is SAME OR NEWER than the newest"
                               " seen one saved in the db. This is odd. You"
                               " may have altered the db, or it is a bug, or"
                               " Reddit has altered the timestamp. Newest seen"
                               " id and timestamp will be updated in the meta"
                               " table but the existing mod action will stay"
                               " unchanged in the main table. Fetched version:"
                               " " + str(ma))

    # mind that we use an _unfiltered_ fetch result to find the newest seen
    # mod action, to avoid re-checking filtered-out items next time
    update_newest_mod_action(db_conn, mod_actions) # commits

    db_conn.close()
    return records
