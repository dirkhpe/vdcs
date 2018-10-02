"""
This script will add the burger - competenties link to the Neo4J database.
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
courses = {}
relations = {}
repo = dict(competentie=competenties,
            course=courses,
            relation=relations)
course_obj = nl.Course(repo)
comp_obj = nl.Competentie(repo)
rel_obj = nl.Relation(repo)
# Collect current information from Burgers
course_obj.populate_repo(cfg)
comp_obj.populate_repo(cfg)
rel_obj.populate_repo(cfg)
logging.info("I know about {c1} courses, {c2} competenties and {c3} relations".format(c1=len(courses),
                                                                                       c2=len(competenties),
                                                                                       c3=len(relations)))

ffp = cfg["Data"]["opleiding_comp"]
fieldnames = ["COURSE_ID", "COMPETENCE_ID", "COMPETENCE_DESCRIPTION"]
csv.register_dialect('tabdelim', delimiter='|')
li = my_env.LoopInfo("Course-Competences", 5000)
with open(ffp, newline="", encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, fieldnames=fieldnames, dialect='tabdelim')
    for row in reader:
        li.info_loop()
        course_node = course_obj.get_node(row["COURSE_ID"])
        comp_dic = dict(id=row["COMPETENCE_ID"], desc=row["COMPETENCE_DESCRIPTION"])
        comp_node = comp_obj.get_node(comp_dic)
        rel_obj.set(course_node, course2competentie, comp_node)
li.end_loop()

for lbl in ["courses", "competenties"]:
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
