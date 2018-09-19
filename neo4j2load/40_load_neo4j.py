"""
This script will create and execute the command line to load nodes and relationships in the Neo4J database.
"""

import logging
import os
import subprocess as sp
from lib import my_env
from lib.neostructure import *

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")

cmd = os.path.join(cfg["Graph"]["path"], "neo4j-admin.bat")
args = [cmd, "import", "--database={db}".format(db=cfg["Graph"]["neo_db"])]
nodes.append("params")
nodes.append("applications")
for lbl in nodes:
    hdr = os.path.join(cfg["Main"]["neo4jcsv_dir"], "node_{lbl}_1.csv".format(lbl=lbl))
    con = os.path.join(cfg["Main"]["neo4jcsv_dir"], "node_{lbl}_2.csv".format(lbl=lbl))
    arg = "--nodes={hdr},{con}".format(hdr=hdr, con=con)
    args.append(arg)

rf = os.path.join(cfg["Main"]["neo4jcsv_dir"], "relations.csv")
arg = "--relationships={rf}".format(rf=rf)
args.append(arg)
module = my_env.get_modulename(__file__)
sof = os.path.join(cfg["Main"]["logdir"], "{mod}_out.log".format(mod=module))
sef = os.path.join(cfg["Main"]["logdir"], "{mod}_err.log".format(mod=module))
# print(" ".join(args))
so = open(sof, "w")
se = open(sef, "w")
try:
    sp.run(args, stderr=se, stdout=so, check=True)
except sp.CalledProcessError as e:
    logging.error("Some issues during execution, check {sef} and {sof}".format(sof=sof, sef=sef))
else:
    logging.info("No error messages returned, see {sof}!".format(sof=sof))
se.close()
so.close()
