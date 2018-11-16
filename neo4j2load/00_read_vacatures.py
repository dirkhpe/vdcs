import csv
import logging
import os
from lib import my_env
from lib import neoload as nl

cfg = my_env.init_env("vdab", __file__)
logging.info("Start application")
vacatures = {}
repo = dict(vacature=vacatures)
vacature_obj = nl.Vacature(repo)
# Then add Vacature nodes containing IDs and Titles.
vacature_file = cfg["Main"]["vacature_titels"]
csv.register_dialect('pipedelim', delimiter='|')
with open(vacature_file, newline='\r\n', encoding="iso-8859-1") as f:
    reader = csv.DictReader(f, dialect='pipedelim')
    li = my_env.LoopInfo("Vacature Titles", 10000)
    for row in reader:
        li.info_loop()
        vac_id = row["ID"]
        # Titel can contain \n - this will cause \r\n (^M in output).
        title = row["FUNCTIE_NAAM"].replace("\r", "").replace("\n", " ")
        if not vacature_obj.get_node(vac_id, title):
            msg = "Line could not be added: ID: {vac_id}, Title: {title}".format(vac_id=vac_id, title=title)
            logging.error(msg)
    li.end_loop()

nodes = ["vacatures"]
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
