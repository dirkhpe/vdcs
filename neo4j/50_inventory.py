"""
This script will collect the Neo4J Inventory
"""
import datetime
import os
from lib import my_env
from lib import neostore
from lib import write2excel

# Initialize environment
cfg = my_env.init_env("vdab", __file__)
ns = neostore.NeoStore(cfg, refresh="No")

# Collect Node inventory
query = "match (n) return distinct(labels(n)) as labels, count(n) as cnt"
cur = ns.get_query(query)
res_list = []
while cur.forward():
    rec = cur.current
    res = dict(label=rec["labels"][0], cnt=rec["cnt"])
    res_list.append(res)

xl = write2excel.Write2Excel()
xl.init_sheet("Nodes")
xl.write_content(res_list)

# Collect Relations Inventory
query = "match (a)-[rel]->(b) return labels(a) as from, labels(b) as to, count(rel) as cnt"
cur = ns.get_query(query)
res_list = []
while cur.forward():
    rec = cur.current
    res = dict(from_lbl=rec["from"][0], to_lbl=rec["to"][0], cnt=rec["cnt"])
    res_list.append(res)

xl.init_sheet("Relations")
xl.write_content(res_list)

fn = "neo4j_inv_{db}_{ts}.xlsx".format(db=cfg["Graph"]["neo_db"], ts=datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
xl.close_workbook(os.path.join(cfg["Main"]["logdir"], fn))
