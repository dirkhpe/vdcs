nodes = ["clientips", "ikls", "sessions", "users", "vacatures", "vhosts", "visitors"]

# Node
lbl_application = "Applicatie"
lbl_logrecord = "Log"
lbl_clientip = "ClientIp"
lbl_ikl = "Client"
lbl_param = "Filter"
lbl_session = "Session"
lbl_user = "User"
lbl_vacature = "Vacature"
lbl_vhost = "Vhost"
lbl_vhostip = "VhostIp"
lbl_visitor = "Visitor"

# Relations
appl2param = "hasFilter"
session2clientip = "fromDevice"
session2ikl = "checkIkl"
session2logrecord = "sesLog"
session2param = "usedFilter"
session2vacature = "interest"
session2vhost = "forHost"
user2ikl = "hasIkl"
visitor2clientip = "onDevice"
visitor2logrecord = "visLog"
visitor2session = "hasSession"
visitor2user = "isUser"
vhost2vhostip = "hasIp"

# SQLite configuration
clicks_tbl = "clicks"
click2session_tbl = "click2session"
click2visitor_tbl = "click2visitor"
session_tbl = "sessions"
visitor_tbl = "visitors"
