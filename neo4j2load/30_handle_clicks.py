"""
This script will get click information and make it available in csv files for load in Neo4J.
"""

import csv
import logging
import os
import pandas
from lib import my_env
from lib import neoload as nl
from lib.neostructure import *
from lib import sqlstore
from urllib import parse


wl_vindeenjob = ["arbeidsregime", "jobdomein", "ervaring", "arbeidscircuit", "arbeidsduur", "internationaal",
                 "diplomaniveau", "vakgebieden", "doelgroep", "leervorm", "lesvorm", "organisator",
                 "knelpuntberoep", "trefwoord"]


def opleidingaanbod():
    """
    This method handles queries with urlpath starter /opleidingen/aanbod/. Third element in the path is the course ID
    if it exist and if it is not equal to 'opleidingen'. (in a few cases the URL is
    /opleidingen/aanbod/opleidingen/aanbod. These cases are ignored for now.

    :return:
    """
    applicatie = "vindeenopleiding"
    ts = rec["timestamp"]
    path = parse.urlparse(rec["urlpath"]).path
    path_arr = path.split("/")
    if len(path_arr) > 3:
        course_id = path_arr[3]
        if course_id:
            course_node = course_obj.get_node(course_id)
            rel_obj.set(session_node, session2course, course_node, source=applicatie, ts=ts)
    return


def vindeenjobopl():
    """
    This method will handle query and vacature ID request. A query has 3 / in the path, a vacature has 4 / and last one
    needs to be a vacature ID.

    :return:
    """
    path_arr = rec["urlpath"].split("/")
    applicatie = path_arr[2]
    # source = applicatie
    ts = rec["timestamp"]
    if (applicatie == "vindeenopleiding") or (len(path_arr) == 4):
        # Investigate Query
        queryparms = parse.parse_qsl(rec["urlquery"][1:])
        for param_name, param_value in queryparms:
            if param_name.lower() in wl_vindeenjob:
                pd = dict(applicatie=applicatie,
                          key=param_name,
                          value=param_value)
                try:
                    pn, rel = param_obj.get_node(pd)
                except TypeError:
                    logging.error("Error while parsing dictionary {pd}".format(pd=pd))
                    pn = False
                    rel = False
                if pn:
                    # Link session to parameter
                    rel_obj.set(session_node, rel, pn, source=applicatie, ts=ts)
    elif (applicatie != "vindeenopleiding") and (len(path_arr) == 5):
        if "?" in path_arr[4]:
            vac_id, urlquery = path_arr[4].split("?")
        else:
            vac_id = path_arr[4]
        # New vacature?
        vac_node = vacature_obj.get_node(vac_id)
        if vac_node:
            # Link session to vacature
            rel_obj.set(session_node, session2vacature, vac_node, source=applicatie, ts=ts)
    return


def werkgever():
    """
    This method will find application and IKL. This will be linked to the session.

    :return:
    """
    path = parse.urlparse(rec["urlpath"]).path
    path_arr = path.split("/")
    if len(path_arr) == 4:
        source = path_arr[3][:-3]
        ts = rec["timestamp"]
        # Investigate Query
        queryparms = parse.parse_qs(rec["urlquery"][1:].lower())
        try:
            iklnr = queryparms['kandidaatiklnummer'][0]
        except KeyError:
            pass
        else:
            ikl_node = ikl_obj.get_node(iklnr)
            rel_obj.set(session_node, session2ikl, ikl_node, source=source, ts=ts)
            # Link ikl, timestamp, application name to session for werkgever
    return


cfg = my_env.init_env("vdab", __file__)
cs = my_env.cleanstr
logging.info("Start Application")
cdb = sqlstore.DirectConn(cfg)
cdb.connect2db()

# Load User Data
user_ext = {}
user_file = cfg["Main"]["users"]
df = pandas.read_csv(user_file)
for row in df.iterrows():
    # Get csv row in dict format
    xl = row[1].to_dict()
    # Convert csv line to param dictionary.
    uid = xl["uid"]
    ikl = xl["persoonId"]
    user_ext[cs(uid)] = ikl

# Load Vacature Data

urlpath_starters = my_env.urlpath_starters

# Initialize node and relations dictionaries
applications = {}
clientips = {}
courses = {}
ikls = {}
params = {}
sessions = {}
users = {}
vacatures = {}
vhosts = {}
visitors = {}
relations = {}
repo = dict(application=applications,
            clientip=clientips,
            course=courses,
            ikl=ikls,
            param=params,
            session=sessions,
            user=users,
            user_ext=user_ext,
            vacature=vacatures,
            vhost=vhosts,
            visitor=visitors,
            relation=relations)

# Initialize objects
appl_obj = nl.Application(repo)
course_obj = nl.Course(repo)
ikl_obj = nl.Ikl(repo)
param_obj = nl.Param(repo)
session_obj = nl.Session(repo)
vacature_obj = nl.Vacature(repo)
vhost_obj = nl.Vhost(repo)
rel_obj = nl.Relation(repo)

# First populate data dictionary param
vej_file = cfg["Main"]["vej_params"]
df = pandas.read_excel(vej_file, skiprows=1)
for row in df.iterrows():
    # Get excel row in dict format
    xl = row[1].to_dict()
    # Convert excel line to param dictionary.
    pardic = dict(
        applicatie=xl["Applicatie"],
        key=xl["ParameterNaam"],
        value=xl["ParameterWaarde"],
        definitie=xl["Functionele definitie"]
    )
    param_node, rel_type = param_obj.get_node(pardic)

# Then add Vacature nodes containing IDs and Titles.
vacature_file = cfg["Main"]["vacature_titels"]
csv.register_dialect('pipedelim', delimiter='|')
with open(vacature_file, newline='\r\n', encoding="iso-8859-1") as f:
    reader = csv.DictReader(f, dialect='pipedelim')
    li = my_env.LoopInfo("Vacature Titles", 10000)
    for row in reader:
        li.info_loop()
        vacid = row["ID"]
        # Titel field can contain \n or \r\n (^M and/or newline in output).
        title = row["FUNCTIE_NAAM"].replace("\r", "").replace("\n", " ")
        if not vacature_obj.get_node(vacid, title):
            msg = "Line could not be added: ID: {vac_id}, Title: {title}".format(vac_id=vacid, title=title)
            logging.error(msg)
    li.end_loop()

# Now handle all log records.
query = """
select c.id, c.clientip, c.urlpath, c.urlquery, c.auth, c.user, c.vhost, c.timestamp,
       v.id vid, s.id sid, s.count, s.first, s.last, s.bot
from clicks c
inner join click2visitor cv on cv.click_id = c.id
inner join visitors v on v.id = cv.visitor_id
inner join click2session cs on cs.click_id = c.id
inner join sessions s on s.id = cs.session_id
"""
res = cdb.get_query(query)
recloop = my_env.LoopInfo("logrecords", 5000)
for rec in res:
    # New record, create logrecord node
    logcnt = recloop.info_loop()
    # Get session.
    session_node = session_obj.get_node(rec)

    # Get vhost
    # vhost_node = vhost_obj.get_node(rec)
    # Link Session to Vhost
    # rel_obj.set(session_node, session2vhost, vhost_node)

    # Click behaviour will be linked to the session. I'm not interested in repeated behaviour, I want to understand
    # specific behaviour.
    for key in urlpath_starters:
        urlpath = rec["urlpath"].lower()
        if key == urlpath[:len(key)]:
            # Call the function for this urlpath_starter key.
            try:
                eval(urlpath_starters[key])()
            except NameError:
                logging.error("Function {fn} needs to be defined!".format(fn=urlpath_starters[key]))
            break
    """
    else:
        logging.error("URLPath not handled for Click ID: {cid} - ({url})".format(cid=rec["id"], url=rec["urlpath"]))
    """
recloop.end_loop()

# Write Node files.
nodes.append("params")
nodes.append("applications")
for lbl in nodes:
    # Header File
    func = eval("nl.get_{lbl}_header".format(lbl=lbl))
    fn = os.path.join(cfg["Main"]["neo4jcsv_dir"], "node_{lbl}_00.csv".format(lbl=lbl))
    with open(fn, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(func())
    # Then write content file
    fn = os.path.join(cfg["Main"]["neo4jcsv_dir"], "node_{lbl}_main.csv".format(lbl=lbl))
    with open(fn, "w", newline="", encoding="utf-8") as csvfile:
        func = eval("nl.get_{lbl}_header".format(lbl=lbl))
        writer = csv.DictWriter(csvfile, fieldnames=func())
        arr = eval(lbl)
        for k in arr:
            writer.writerow(arr[k])

# Write Relation files.
# First write Relation header
fn = os.path.join(cfg["Main"]["neo4jcsv_dir"], "rel_main_00.csv")
with open(fn, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(nl.get_relations_header())
# Then write content file
fn = os.path.join(cfg["Main"]["neo4jcsv_dir"], "rel_main_01.csv")
with open(fn, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=nl.get_relations_header())
    for k in relations:
        writer.writerow(relations[k])

logging.info("End Application")
