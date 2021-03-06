from conf import *
import requests
import time
import feedparser 
import os
import sqlite3
import json
from log import *
from datetime import datetime, timezone

def pretty_date(ds):
    assert ds[22] == ':'
    # hack to make it parseable by strptime
    parseable = ds[:22] + ds[23:]
    dt = datetime.strptime(parseable, "%Y-%m-%dT%H:%M:%S%z")
    ds2 = dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat(' ') + " UTC"
    return ds2

def clean_name(name):
    return name.replace("/u/", "")

def reddit_mod_log():
    mod_log_url = config['redditmodlog']['url']
    mod_log_db_name = config['redditmodlog']['dbname']+".sqlite"
    postnow = True
    if not os.path.isfile(mod_log_db_name):
        postnow = False
        db_connection = sqlite3.connect(mod_log_db_name)
        db = db_connection.cursor()
        db.execute('CREATE TABLE "redditmodlog" ( `id` TEXT, `modname` TEXT, `updated` TEXT, `action` TEXT, PRIMARY KEY(`id`) )')
        db.close()
    feedobject = feedparser.parse(mod_log_url) 
    db_connection = sqlite3.connect(mod_log_db_name)
    db = db_connection.cursor()
    for entry in feedobject.entries:
        mid=entry['id']
        modname=entry['authors'][0]['name']
        updated=entry['updated']
        action=entry['title_detail']['value']
        db.execute("SELECT * from redditmodlog WHERE id=?", (mid,))
        if not db.fetchall():
            db.execute("INSERT INTO redditmodlog VALUES (?,?,?,?)", (mid, modname,updated,action))
            db_connection.commit()
            if postnow:
                modnameclean = clean_name(modname)
                updated_fmt = pretty_date(updated)
                msg = json.dumps(modnameclean+' '+updated_fmt+'; reddit decred; '+action)[1:-1]
                send_matrix_msg(msg)
                logger.info("Sending:" + msg)
    db.close()




def send_matrix_msg(msg):
    token=config['matrixconfig']['accesstoken']
    roomid=config['matrixconfig']['roomid']
    server_url=config['matrixconfig']['server_url']
    data = '{"msgtype":"m.text", "body":"'+msg+'"}'
    r = requests.post(server_url+"_matrix/client/r0/rooms/"+roomid+"/send/m.room.message?access_token="+token, data = data)
