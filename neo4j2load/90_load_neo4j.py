"""
This script will create and execute the command line to load nodes and relationships in the Neo4J database.
"""

import logging
import os
import subprocess as sp
from lib import my_env

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")

cmd = os.path.join(cfg["Graph"]["path"], cfg["Graph"]["adm"])
args = [cmd, "import", "--database={db}".format(db=cfg["Graph"]["neo_db"])]
filedir = cfg["Main"]["neo4jcsv_dir"]
# Remove all files from directory
filelist = os.listdir(filedir)

# Add arguments for node files
my_env.neo4j_load_param("node", args, filedir)
# Then add arguments for relation files
my_env.neo4j_load_param("rel", args, filedir)

module = my_env.get_modulename(__file__)
sof = os.path.join(cfg["Main"]["logdir"], "{mod}_out.log".format(mod=module))
sef = os.path.join(cfg["Main"]["logdir"], "{mod}_err.log".format(mod=module))
# print(" ".join(args))
so = open(sof, "w")
se = open(sef, "w")
logging.info("Command: {c}".format(c=args))
try:
    sp.run(args, stderr=se, stdout=so, check=True)
except sp.CalledProcessError as e:
    logging.error("Some issues during execution, check {sef} and {sof}".format(sof=sof, sef=sef))
else:
    logging.info("No error messages returned, see {sof}!".format(sof=sof))
se.close()
so.close()
