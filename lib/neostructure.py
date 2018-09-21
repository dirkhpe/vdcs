nodes = ["clientips", "ikls", "sessions", "users", "vacatures", "vhosts", "visitors"]

# Node
lbl_application = "Applicatie"
lbl_logrecord = "Log"
lbl_clientip = "IP"
lbl_ikl = "Burger"
lbl_param = "Filter"
lbl_session = "Sessie"
lbl_user = "User"
lbl_vacature = "Vacature"
lbl_vhost = "Vhost"         # Do not use Vhost
lbl_vhostip = "VhostIp"     # Do not use VhostIp
lbl_visitor = "Visitor"
lbl_zoekwoord = "Zoekwoord"

# Relations
ikl2user = "heeftUsername"
param2appl = "vanApplicatie"
session2clientip = "fromDevice"
session2ikl = "bekeekKandidaat"
session2logrecord = "sesLog"
session2param = "gebruikteFilter"
session2vacature = "bekeekVacature"
session2vhost = "forHost"   # Do not use session2vhost
session2zoekwoord = "gebruiktZoekwoord"
user2visitor = "heeftVisitor"
visitor2clientip = "heeftIP"
visitor2logrecord = "visLog"
visitor2session = "heeftSessie"
vhost2vhostip = "hasIp"

# SQLite configuration
clicks_tbl = "clicks"
click2session_tbl = "click2session"
click2visitor_tbl = "click2visitor"
session_tbl = "sessions"
visitor_tbl = "visitors"
