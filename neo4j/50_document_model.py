"""
This script will run a number of queries as documentation for the model.
"""
from lib import my_env
from lib import neostore

# Initialize environment
cfg = my_env.init_env("vdab", __file__)
ns = neostore.NeoStore(cfg, refresh="No")

print("A visitor can have more than one session")
print("VisitorID\tCount")
query = "match (v:Visitor)-[:hasSession]->(s:Session) return v.vid as vid, count(*) as cnt order by cnt desc limit 10"
cur = ns.get_query(query)
while cur.forward():
    rec = cur.current
    print("Vid: {vid}\tCount: {cnt}".format(vid=rec["vid"], cnt=rec["cnt"]))
print("Timeframe (10 minutes) is too small to see different sessions per visitor.")

print("A user can be connected to multiple visitors (user logs in to different devices - Top 10:")
print("Count\t\tUser")
query = "match (u:User)<-[:isUser]-(v:Visitor) return u.name as name, count(*) as cnt order by cnt desc limit 5"
cur = ns.get_query(query)
while cur.forward():
    rec = cur.current
    print("Count: {cnt}\tUser: {name}".format(name=rec["name"], cnt=rec["cnt"]))
print("The timeframe (10 minutes) can be too short to show this.")

print("\nReview vacatures.")
query = "match (n:Vacature) return count (n)"
cur = ns.get_query(query)
print("Total number of vacatures found: {tot}".format(tot=cur.evaluate()))
query = "match (n:Vacature) where exists (n.titel) return count (n)"
cur = ns.get_query(query)
print("Vacatures with title found: {tot}".format(tot=cur.evaluate()))
query = "match (n:Vacature) where not exists (n.titel) return count (n)"
cur = ns.get_query(query)
print("Vacatures without title found: {tot}".format(tot=cur.evaluate()))

print("\nList Users and number of vacatures per user")
print("Note that definition of Visitor and Session needs to be reviewed to have correct results.")
query = """
match (user:User)<-[:isUser]-(vis:Visitor)-[:hasSession]->(sess:Session)-[:interest]->(vac:Vacature)
return user.name as name, count(vac) as cnt 
order by cnt desc limit 15
"""
cur = ns.get_query(query)
print("Count\tUser")
while cur.forward():
    rec = cur.current
    print("Count: {cnt}\tUser: {u}".format(u=rec["name"], cnt=rec["cnt"]))
print("Top request 36 vacatures in 10 minutes (upper limit in web app?).")
print("These top users appear to be bots requesting vacatures.")

print("\nMost popular vacatures")
query = """
match (user:User)<-[:isUser]-(vis:Visitor)-[:hasSession]->(sess:Session)-[:interest]->(vac:Vacature)
return vac.vac_id as vac, count(user) as cnt 
order by cnt desc limit 15
"""
cur = ns.get_query(query)
print("Count\tVacature")
while cur.forward():
    rec = cur.current
    print("Count: {cnt}\tVac: {u}".format(u=rec["vac"], cnt=rec["cnt"]))

print("\nMost popular vacatures including vacature titel")
query = """
match (user:User)<-[:isUser]-(vis:Visitor)--(sess:Session)-[:interest]->(vac:Vacature)
where exists(vac.titel)
return vac.titel as vac, count(user) as cnt 
order by cnt desc limit 15
"""
cur = ns.get_query(query)
print("Count\tVacature")
while cur.forward():
    rec = cur.current
    print("Count: {cnt}\tVac: {u}".format(u=rec["vac"], cnt=rec["cnt"]))

print("\nMost popular client IP - most of the visitors.")
query = "match (c:ClientIp)<-[:onDevice]-(v:Visitor) return c.ip as ip, count(*) as cnt order by cnt desc limit 10"
cur = ns.get_query(query)
print("Count\tip")
while cur.forward():
    rec = cur.current
    print("{cnt}\t{ip}".format(ip=rec["ip"], cnt=rec["cnt"]))

query = """
match (user:User)<-[:isUser]-(vis:Visitor)-[:hasSession]->(sess:Session)-[:interest]->(vac:Vacature)
where vac.titel="winkelverantwoordelijke-nieuwpoort" or vac.titel="medewerker-inpak-regio-nieuwpoort"
return user,vis,sess,vac
"""
print("Apparantly same 7 users are querying these top-2 vacatures from the same session.")
print(query)
print("Note that the session ID for this is 131, the top 1 user earlier...")