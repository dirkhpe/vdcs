"""
This script will get click information and make it available in neo4j. Visitor and session information will be
calculated along the path.
"""
# Todo: split event and filter.

from lib import my_env
from lib.neostructure import *
from lib import neostore
from lib import sqlstore
from lib.sqlstore import *


wl_vindeenjob = ["arbeidsregime", "jobdomein", "ervaring", "arbeidscircuit", "arbeidsduur", "internationaal",
                 "diplomaniveau", "sinds", "vakgebieden", "doelgroepen", "leervorm", "lesvorm", "organisator",
                 "knelpuntberoep", "trefwoord"]
wl_collect_action = ["action", "cat", "context"]


def collect_action():
    """
    Collect Action URLs start with action, then category or context. Action, category or context will be handled as
    Param nodes and attached to the session.

    VacatureId will be handled as a vacature node. Other parameters will not be handled now.

    :return:
    """
    urlquery = rec.urlquery[1:]
    queryparms = urlquery.split("&")
    for kvpair in queryparms:
        param_name, param_value = kvpair.split("=")
        if param_name in wl_collect_action:
            pardic = dict(applicatie="collect_action",
                          naam=param_name,
                          waarde=param_value)
            param_node = param.get_node(pardic)
            ns.create_relation(session_node, session2param, param_node)
        elif param_name == "vacatureId":
            vac_node = vacature.get_node(param_value)
            if vac_node:
                ns.create_relation(session_node, session2vacature, vac_node)
    return

def jobs_vacatures():
    """
    This method will handle the /jobs/vacatures request. Third part of the request is vacature ID. Fourth part can be
    the title of the vacature. There is optionally a query, that will be handled in the same way as 'vind een job'.

    :return:
    """
    if "?" in rec.urlpath:
        path, urlquery = rec.urlpath.split("?")
    else:
        path, urlquery = rec.urlpath, None
    urlpath_arr = path.split("/")
    if len(urlpath_arr) > 3:
        vac_id = urlpath_arr[3]
        vac_node = vacature.get_node(vac_id)
        if vac_node:
            ns.create_relation(session_node, session2vacature, vac_node)
            if len(urlpath_arr) == 5:
                vac_title = urlpath_arr[4]
                vacature.add_title(vac_node, vac_title)
    if urlquery:
        applicatie = "jobs_vacatures"
        queryparms = urlquery.split("&")
        for kvpair in queryparms:
            if "=" in kvpair:
                param_name, param_value = kvpair.split("=")
                if param_name in wl_vindeenjob:
                    pardic = dict(applicatie=applicatie,
                                  naam=param_name,
                                  waarde=param_value)
                    param_node = param.get_node(pardic)
                    ns.create_relation(session_node, session2param, param_node)
    return


def vindeenjob_vacatures():
    """
    This method will handle query and vacature ID request. A query has 3 / in the path, a vacature has 4 / and last one
    needs to be a vacature ID.

    :return:
    """
    path_arr = rec.urlpath.split("/")
    if len(path_arr) == 4:
        # Investigate Query
        query2process = rec.urlquery[1:].lower()
        queryparms = query2process.split("&")
        for kvpair in queryparms:
            kvpair = kvpair.replace("==", "=")
            param_name, param_value = kvpair.split("=")
            if param_name in wl_vindeenjob:
                pardic = dict(applicatie="vindeenjob",
                              naam=param_name,
                              waarde=param_value)
                param_node = param.get_node(pardic)
                ns.create_relation(session_node, session2param, param_node)
    elif len(path_arr) == 5:
        if "?" in path_arr[4]:
            vac_id, urlquery = path_arr[4].split("?")
        else:
            vac_id = path_arr[4]
        if vac_id.isdigit():
            # OK - vacature has been found
            # New vacature?
            vac_node = vacature.get_node(vac_id)
            if vac_node:
                ns.create_relation(session_node, session2vacature, vac_node)
    return

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
ns = neostore.NeoStore(cfg, refresh="No")
sql_eng = sqlstore.init_session(cfg["Main"]["db"])
urlpath_starters = my_env.urlpath_starters


clientip = neostore.ClientIp(ns)
log = neostore.Log(ns)
param = neostore.Param(ns)
user = neostore.User(ns)
session = neostore.Session(ns)
vacature = neostore.Vacature(ns)
vhost = neostore.Vhost(ns)
visitor = neostore.Visitor(ns)

# Now handle all log records.
query = sql_eng.query(Click).filter(Click.timestamp >= "2017-11-24T09:30:00", Click.timestamp <= "2017-11-24T09:40:00")
recloop = my_env.LoopInfo("logrecords", 50)
for rec in query.all():
    # New record, create logrecord node
    logcnt = recloop.info_loop()
    # Get session. The session will optionally create and link to the visitor.
    session_node = session.get_node(rec)
    # Link to vhost
    vhost_node = vhost.get_node(rec)
    ns.create_relation(session_node, session2vhost, vhost_node)
    # Click behaviour will be linked to the session. I'm not interested in repeated behaviour, I want to understand
    # specific behaviour.
    for key in urlpath_starters:
        urlpath = rec.urlpath.lower()
        if key == urlpath[:len(key)]:
            # Call the function for this urlpath_starter key.
            try:
                eval(urlpath_starters[key])()
            except NameError:
                logging.error("Function {fn} needs to be defined!".format(fn=urlpath_starters[key]))
            break
    else:
        logging.error("URLPath not handled for Click ID: {cid} - ({url})".format(cid=rec.id, url=rec.urlpath))
    """
    if logcnt >= 500:
        break
    """
recloop.end_loop()

logging.info("End Application")
