"""
This library consolidates the classes required to prepare the Neo4J csv load files.
This should allow to speed up the Neo4J load process.
This library is using direct (dictionary) SQL results, not sqlalchemy results.
"""

from datetime import datetime
from lib.neostructure import *
from lib import my_env

cs = my_env.cleanstr

class Application:
    def __init__(self, repo):
        self.applications = repo["application"]

    def get_node(self, appl_name):
        """
        This method will return the application node. If the application node does not exist, it will be created.

        :param appl_name:

        :return:
        """
        node_id = self.get_application_id(appl_name)
        if node_id not in self.applications:
            lbl = lbl_application
            props = {":ID": node_id, ":LABEL": lbl, "naam": self.get_application_name(appl_name)}
            self.applications[node_id] = props
        return node_id

    def get_application_id(self, appl_name):
        return "{lbl}|{val}".format(lbl=lbl_application, val=self.get_application_name(appl_name))

    @staticmethod
    def get_application_name(appl_name):
        return cs(appl_name).replace(" ", "")

def get_applications_header():
    return [":LABEL", ":ID", "naam"]

class Clientip:
    def __init__(self, repo):
        self.clientips = repo["clientip"]

    def get_node(self, rec):
        node_id = self.get_id(rec)
        if node_id not in self.clientips:
            ip = cs(rec["clientip"])
            lbl = lbl_clientip
            props = {":ID": node_id, ":LABEL": lbl, "adres": ip}
            self.clientips[node_id] = props
        return node_id

    @staticmethod
    def get_id(rec):
        return "{lbl}|{val}".format(lbl=lbl_clientip, val=cs(rec["clientip"]))

def get_clientips_header():
    return [":LABEL", ":ID", "adres"]

class Ikl:
    def __init__(self, repo):
        self.ikls = repo["ikl"]

    def get_node(self, ikl):
        node_id = self.get_id(ikl)
        if node_id not in self.ikls:
            lbl = lbl_ikl
            props = {":ID": node_id, ":LABEL": lbl, "ikl": cs(ikl)}
            self.ikls[node_id] = props
        return node_id

    @staticmethod
    def get_id(ikl):
        return "{lbl}|{val}".format(lbl=lbl_ikl, val=cs(ikl))

def get_ikls_header():
    return [":LABEL", ":ID", "ikl"]

class Param:
    def __init__(self, repo):
        """
        The Param class will manage parameter nodes. A parameter node will manage a link to the Application node if
        required.

        :param repo: Repository with all dictionaries
        """
        self.parameters = repo["param"]
        self.relation = Relation(repo)
        self.application = Application(repo)

    def get_node(self, pardic):
        """
        This method will return the parameter node. A parameter node is defined by the parameter 'key' and the
        'value'. Attribute 'definitie' will be added to the parameter row if available. If this is a new
        parameter, it will be added to the dictionary.

        If 'applicatie' is available in the parameter dictionary, then the parameter is linked to the application.
        Be careful: for now I cannot have duplicate parameter names in different applications.

        :param pardic: Parameter dictionary for the current node. 'naam' is mandatory and must be string. 'waarde' is
        mandatory. 'definitie' and 'applicatie' are optional.

        :return: parameter node and relation name, or False if value could not be translated to str (not ascii)
        """
        node_id = self.get_param_id(pardic)
        if node_id:
            name = cs(pardic["key"])
            val = cs(pardic["value"])
            if name == "trefwoord":
                lbl = lbl_zoekwoord
                rel = session2zoekwoord
                waarde = val
            else:
                lbl = lbl_param
                rel = session2param
                code = val
                if "definitie" in pardic:
                    waarde = pardic["definitie"]
            if node_id not in self.parameters:
                # I'm sure that name and val are not False here.
                props = {":ID": node_id, ":LABEL": lbl, "naam": name}
                try:
                    props["waarde"] = waarde
                except NameError:
                    pass
                try:
                    props["code"] = code
                except NameError:
                    pass
                self.parameters[node_id] = props
                if 'applicatie' in pardic:
                    appl_node = self.application.get_node(pardic["applicatie"])
                    self.relation.set(node_id, param2appl, appl_node)
            return node_id, rel
        else:
            return False

    @staticmethod
    def get_param_id(pardic):
        lbl = cs(pardic["key"])
        val = cs(pardic["value"])
        if lbl and val:
            return "{lbl}|{val}".format(lbl=lbl, val=val)
        else:
            return False

def get_params_header():
    return [":LABEL", ":ID", "naam", "waarde", "code", "applicatie"]

class Relation:
    def __init__(self, repo):
        self.relations = repo["relation"]

    def set(self, start_node, reltype, end_node, source=None, ts=None):
        """
        This method will verify if a relation exists already. If not, it will be created.

        :param start_node: Start node identifier

        :param end_node: End node identifier

        :param reltype: Relation type

        :param source: Optional attribute to specify source of the information

        :param ts: Optional attribute to specify timestamp of the information

        :return:
        """
        # Disable this block to disable timestamp as a relation attribute
        """
        if ts:
            ts = None
        """
        rel_id = self.get_rel_id(start_node, reltype, end_node, source, ts)
        if rel_id not in self.relations:
            self.relations[rel_id] = self.get_rel_props(start_node, reltype, end_node, source, ts)
        return

    @staticmethod
    def get_rel_id(start_node, relation, end_node, source, ts):
        return "{sn}|{rel}|{en}|{source}|{ts}".format(sn=start_node, rel=relation, en=end_node, source=source, ts=ts)

    @staticmethod
    def get_rel_props(start_node, relation, end_node, source, ts):
        props = {":START_ID": start_node, ":END_ID":end_node, ":TYPE": relation}
        if source:
            props["source"] = source
        if ts:
            props["ts:datetime"] = ts
        return props

def get_relations_header():
    return [":START_ID", ":END_ID", ":TYPE", "source", "ts:datetime"]


class Session:
    def __init__(self, repo):
        self.sessions = repo["session"]
        self.visitor = Visitor(repo)
        self.relation = Relation(repo)

    def get_node(self, rec):
        """
        This method will return the session dictionary. A session has sid - first (occurrence) - last (occurrence) -
        count (number of clicks in the session).

        :param rec: Click record, links to session record via rec.click4sess[0]

        :return:
        """
        node_id = self.get_id(rec)
        if node_id not in self.sessions:
            sid = cs(rec["sid"])
            lbl = lbl_session
            first = rec["first"]
            last = rec["last"]
            df = "%Y-%m-%dT%H:%M:%S"
            dur = datetime.strptime(last, df) - datetime.strptime(first, df)
            props = {":ID": node_id, ":LABEL": lbl, "sessieID": sid, "start:datetime": first,
                     "stop:datetime": last, "aantalPaginas": rec["count"], "duur": dur}
            self.sessions[node_id] = props
            visitor_node = self.visitor.get_node(rec)
            self.relation.set(visitor_node, visitor2session, node_id)
        return node_id

    @staticmethod
    def get_id(rec):
        return "{lbl}|{val}".format(lbl=lbl_session, val=cs(rec["sid"]))

def get_sessions_header():
    return [":LABEL", ":ID", "sessieID", "start:datetime", "stop:datetime", "aantalPaginas", "duur"]

class User:
    def __init__(self, repo):
        self.ikl = Ikl(repo)
        self.users = repo["user"]
        self.user_ext = repo["user_ext"]
        self.relation = Relation(repo)

    def get_node(self, uid):
        node_id = self.get_id(uid)
        if node_id not in self.users:
            lbl = lbl_user
            props = {":ID": node_id, ":LABEL": lbl, "naam": cs(uid)}
            self.users[node_id] = props
            if uid in self.user_ext:
                ikl_node = self.ikl.get_node(self.user_ext[uid])
                self.relation.set(ikl_node, ikl2user, node_id)
        return node_id

    @staticmethod
    def get_id(uid):
        return "{lbl}|{val}".format(lbl=lbl_user, val=cs(uid))

def get_users_header():
    return [":LABEL", ":ID", "naam", "group"]

class Vacature:
    def __init__(self, repo):
        self.vacatures = repo["vacature"]

    def add_title(self, node_id, title):
        self.vacatures[node_id]["titel"] = title
        return

    def get_node(self, vac_id):
        """
        This method will return the node ID for the Vacature, or False if vacature ID is not integer with length of 8.
        In case that the vacature ID does not exist, it will be added.

        :param vac_id:

        :return:
        """
        node_id = self.get_id(vac_id)
        if node_id:
            if node_id not in self.vacatures:
                lbl = lbl_vacature
                props = {":ID": node_id, ":LABEL": lbl, "vacatureID": vac_id, "titel": ""}
                self.vacatures[node_id] = props
            return node_id
        else:
            return False

    @staticmethod
    def get_id(vac_id):
        """
        This method will return the node ID for the Vacature, or False if vacature ID is not integer with length of 8.

        :param vac_id:

        :return: node ID for the vacature or False.
        """
        if vac_id.isdigit() and len(vac_id) == 8:
            return "{lbl}|{val}".format(lbl=lbl_vacature, val=vac_id)
        else:
            return False

def get_vacatures_header():
    return [":LABEL", ":ID", "vacatureID", "titel"]

class Visitor:
    def __init__(self, repo):
        self.visitors = repo["visitor"]
        self.clientip = Clientip(repo)
        self.user = User(repo)
        self.relation = Relation(repo)

    def get_node(self, rec):
        node_id = self.get_id(rec)
        if node_id not in self.visitors:
            vid = rec["vid"]
            lbl = lbl_visitor
            props = {":ID": node_id, ":LABEL": lbl, "visitorID": vid}
            self.visitors[node_id] = props

            # Connect visitor to clientIp
            clientip_node = self.clientip.get_node(rec)
            self.relation.set(node_id, visitor2clientip, clientip_node)

            # Connect visitor to user if authenticated
            if rec["user"] != "unauthenticated":
                user_node = self.user.get_node(rec["user"])
                self.relation.set(user_node, user2visitor, node_id)

        return node_id

    @staticmethod
    def get_id(rec):
        return "{lbl}|{val}".format(lbl=lbl_visitor, val=cs(rec["vid"]))

def get_visitors_header():
    return [":LABEL", ":ID", "visitorID"]

class Vhost:
    def __init__(self, repo):
        self.vhosts = repo["vhost"]

    def get_node(self, rec):
        node_id = self.get_id(rec)
        if node_id not in self.vhosts:
            lbl = lbl_vhost
            props = {":ID": node_id, ":LABEL": lbl, "host": cs(rec["vhost"])}
            self.vhosts[node_id] = props
        return node_id

    @staticmethod
    def get_id(rec):
        return "{lbl}|{val}".format(lbl=lbl_vhost, val=cs(rec["vhost"]))

def get_vhosts_header():
    return [":LABEL", ":ID", "host"]