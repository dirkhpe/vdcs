"""
This script will walk through log records and define visitors and sessions.
This script uses direct SQL queries to the database. Run this script to see results.
"""

import logging
from datetime import datetime, timedelta
from lib import my_env
from lib import sqlstore
from lib.neostructure import *


cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
cdb = sqlstore.DirectConn(cfg)
cdb.connect2db()
sessions = {}
visitors = {}
session_timeout = timedelta(minutes=30)

# Walk through all records - order by timestamp and clickID
query = "SELECT * FROM {ct} ORDER BY timestamp, id".format(ct=clicks_tbl)
res = cdb.get_query(query)
loop_info = my_env.LoopInfo("Logs", 500)
for rec in res:
    commitcnt = loop_info.info_loop()
    log_id = rec["id"]
    # Get visitor
    clientip = rec["clientip"]
    user = rec["user"]
    visitor = "{clientip}|{user}".format(clientip=clientip, user=user)
    try:
        visitor_id = visitors[visitor]
    except KeyError:
        # New visitor, create visitor record
        visitor_rec = dict(
            clientip=clientip,
            user=user
        )
        visitor_id = cdb.insert_row(visitor_tbl, visitor_rec)
        visitors[visitor] = visitor_id
    # Remember link log to visitor
    click2visitor = dict(
        click_id=log_id,
        visitor_id=visitor_id
    )
    cdb.insert_row(click2visitor_tbl, click2visitor)
    # Get session
    timestamp = rec["timestamp"]
    # Is this a session from a new source?
    try:
        session_rec = sessions[str(visitor_id)]
        session_id = session_rec["id"]
    except KeyError:
        # New session, create session record
        session_rec = dict(
            visitor_id=visitor_id,
            first=timestamp,
            last=timestamp,
            count=0
        )
        session_id = cdb.insert_row(session_tbl, session_rec)
        session_rec["id"] = session_id
        sessions[str(visitor_id)] = session_rec
    # Session record is available - Check if session is still valid
    last_dt = datetime.strptime(session_rec["last"], "%Y-%m-%dT%H:%M:%S")
    current_dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
    if (current_dt - last_dt) > session_timeout:
        # Session timeout - start new session for this visitor after saving previous version
        cdb.update_row(session_tbl, session_rec)
        session_rec = dict(
            visitor_id=visitor_id,
            first=timestamp,
            last=timestamp,
            count=1
        )
        session_id = cdb.insert_row(session_tbl, session_rec)
        session_rec["id"] = session_id
        sessions[str(visitor_id)] = session_rec
    else:
        # Continue current session:
        session_rec["count"] += 1
        session_rec["last"] = timestamp
    # Create log to session record
    click2session = dict(
        click_id=log_id,
        session_id=session_id
    )
    cdb.insert_row(click2session_tbl, click2session)
    if commitcnt % 10000 == 0:
        cdb.db_commit()
    # if commitcnt > 5000:
    #     break
loop_info.end_loop()
# Now also update all active sessions
logging.info("End of processing - update remaining sessions (this is not a requirement for live system)")
for k in sessions:
    cdb.update_row(session_tbl, sessions[k])
cdb.db_commit()
logging.info("End Application")
