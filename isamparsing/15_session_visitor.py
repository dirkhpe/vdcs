"""
This script will walk through log records and define visitors and sessions.
This script is using sqlalchemy objects. Performance is very low. Run this script if you have time.
"""

from datetime import datetime, timedelta
from lib import my_env
from lib import sqlstore
from lib.sqlstore import *


cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
sql_eng = sqlstore.init_session(cfg["Main"]["db"])
sessions = {}
visitors = {}
session_timeout = timedelta(minutes=30)

# Walk through all records
query = sql_eng.query(Click).order_by(Click.timestamp, Click.id)
loop_info = my_env.LoopInfo("Logs", 100)
for rec in query.all():
    commitcnt = loop_info.info_loop()
    log_id = rec.id
    # Get visitor
    clientip = rec.clientip
    user = rec.user
    visitor = "{clientip}|{user}".format(clientip=clientip, user=user)
    try:
        visitor_id = visitors[visitor]
    except KeyError:
        # New visitor, create visitor record
        visitor_rec = Visitor(
            clientip=clientip,
            user=user
        )
        sql_eng.add(visitor_rec)
        sql_eng.flush()
        sql_eng.refresh(visitor_rec)
        visitors[visitor] = visitor_rec.id
        visitor_id = visitors[visitor]
    # Remember link log to visitor
    click2visitor = Click2Visitor(
        click_id=log_id,
        visitor_id=visitor_id
    )
    sql_eng.add(click2visitor)
    # Get session
    timestamp = rec.timestamp
    # Is this a session from a new source?
    try:
        session_rec = sessions[str(visitor_id)]
    except KeyError:
        # New session, create session record
        session_rec = Session(
            visitor_id=visitor_id,
            first=timestamp,
            last=timestamp,
            count=0
        )
        sql_eng.add(session_rec)
        sql_eng.flush()
        sql_eng.refresh(session_rec)
        sessions[str(visitor_id)] = session_rec
    # Session record is available - Check if session is still valid
    last_dt = datetime.strptime(session_rec.last, "%Y-%m-%dT%H:%M:%S")
    current_dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
    if (current_dt - last_dt) > session_timeout:
        # Session timeout - start new session after saving previous version
        sql_eng.flush()
        session_rec = Session(
            visitor_id=visitor_id,
            first=timestamp,
            last=timestamp,
            count=1
        )
        sql_eng.add(session_rec)
        sql_eng.flush()
        sql_eng.refresh(session_rec)
        sessions[str(visitor_id)] = session_rec
    else:
        # Continue current session:
        session_rec.count += 1
        session_rec.last = timestamp
    # Create log to session record
    click2session = Click2Session(
        click_id=log_id,
        session_id=session_rec.id
    )
    sql_eng.add(click2session)
    if commitcnt % 100000 == 0:
        sql_eng.commit()
    # if commitcnt > 5000:
    #     break
sql_eng.commit()
logging.info("End Application")
