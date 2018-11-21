"""
This script will add the competentie - competentie affinity to the Neo4J database.
"""
import csv
import logging
import os
from lib import my_env
from lib import neoload as nl
from lib.neostructure import *

# Initialize environment
cfg = my_env.init_env("vdab", __file__)
competenties = {}
ikls = {}
relations = {}
repo = dict(competentie=competenties,
            relation=relations)
comp_obj = nl.Competentie(repo)
rel_obj = nl.Relation(repo)
# Collect current information from Burgers
comp_obj.populate_repo(cfg)
rel_obj.populate_repo(cfg)
logging.info("I know about{c2} competenties and {c3} relations".format(c2=len(competenties), c3=len(relations)))

ffp = cfg["Data"]["comp_comp"]
csv.register_dialect('tabdelim', delimiter=',')
li = my_env.LoopInfo("Competentie Affiniteit", 5000)
with open(ffp, newline="", encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, dialect='tabdelim')
    for row in reader:
        li.info_loop()
        comp_a = row["id_a"][3:]
        comp_b = row["id_b"][3:]
        score = int(row["score"])
        if score > 0:
            comp_node_a = comp_obj.get_node(dict(id=comp_a))
            comp_node_b = comp_obj.get_node(dict(id=comp_b))
            rel_obj.set(comp_node_a, competentie2competentie, comp_node_b, score=score)
li.end_loop()

for lbl in ["competenties"]:
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
