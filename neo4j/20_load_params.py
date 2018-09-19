"""
This script will collect the known parameters and load them into the Neo4J database.
"""

import logging
import pandas
from lib import my_env
from lib import neostore

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
ns = neostore.NeoStore(cfg)
vej_file = cfg["Main"]["vej_params"]
df = pandas.read_excel(vej_file, skiprows=1)

param = neostore.Param(ns)

my_loop = my_env.LoopInfo("Param definitions", 20)
for row in df.iterrows():
    cnt = my_loop.info_loop()
    # Get excel row in dict format
    xl = row[1].to_dict()
    # Convert excel line to param dictionary.
    pardic = dict(
        applicatie=xl["Applicatie"],
        naam=xl["ParameterNaam"],
        waarde=xl["ParameterWaarde"],
        definitie=xl["Functionele definitie"]
    )
    param.get_node(pardic)
my_loop.end_loop()
