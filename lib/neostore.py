"""
This class consolidates functions related to the neo4J datastore.
"""

import logging
import sys
# from datetime import datetime
from lib.neostructure import *
from pandas import DataFrame
from py2neo import Graph, Node, Relationship, NodeMatcher


class NeoStore:

    def __init__(self, config, refresh='No'):
        """
        Method to instantiate the class in an object for the neostore module.

        :param config object, to get connection parameters.

        :param refresh: If Yes, then database will be made empty.

        :return: Object to handle neostore commands.
        """
        logging.debug("Initializing Neostore object")
        self.config = config
        self.graph = self._connect2db()
        if refresh == 'Yes':
            self._delete_all()
        self.matcher = NodeMatcher(self.graph)
        return

    def _connect2db(self):
        """
        Internal method to create a database connection. This method is called during object initialization.

        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Neostore object.")
        neo4j_config = {
            'user': self.config['Graph']['username'],
            'password': self.config['Graph']['password'],
        }
        # Connect to Graph
        graph = Graph(**neo4j_config)
        # Check that we are connected to the expected Neo4J Store - to avoid accidents...
        dbname = graph.database.name
        if dbname != self.config['Graph']['neo_db']:
            logging.fatal("Connected to Neo4J database {d}, but expected to be connected to {n}"
                          .format(d=dbname, n=self.config['Graph']['neo_db']))
            sys.exit(1)
        return graph

    def create_node(self, *labels, **props):
        """
        Function to create node. The function will return the node object.

        :param labels: Labels for the node

        :param props: Value dictionary with values for the node.

        :return: node object
        """
        logging.debug("Trying to create node with params {p}".format(p=props))
        component = Node(*labels, **props)
        self.graph.create(component)
        return component

    def create_relation(self, from_node, rel, to_node):
        """
        Function to create relationship between nodes. If the relation exists already, it will not be created again.

        :param from_node:

        :param rel:

        :param to_node:

        :return:
        """
        rel = Relationship(from_node, rel, to_node)
        self.graph.merge(rel)
        return

    def _delete_all(self):
        """
        Function to remove all nodes and relations from the graph database.
        Then create calendar object.

        :return:
        """
        logging.info("Remove all nodes and relations from database.")
        self.graph.delete_all()
        return

    def get_nodes(self, *labels, **props):
        """
        This method will select all nodes that have labels and properties

        :param labels:

        :param props:

        :return: list of nodes that fulfill the criteria, or False if no nodes are found.
        """
        nodes = self.matcher.match(*labels, **props)
        nodelist = list(nodes)
        if len(nodelist) == 0:
            # No nodes found that fulfil the criteria
            return False
        else:
            return nodelist

    def get_query(self, query):
        """
        This function will run a query and return the result as a cursor.

        :param query:

        :return: cursor containing the query result
        """
        return self.graph.run(query)

    def get_query_as_df(self, query):
        """
        This function will run a query and return the result as a dataframe.

        :param query:

        :return: Dataframe as result
        """
        return DataFrame(self.graph.data(query))

    def get_relations(self, start_lbl, rel_type, end_lbl):
        """
        This method will select all nodes that fulfill a relations.
        py2neo RelationshipMatcher seems like dark magic and needs to be implemented when more time is available.

        :param start_lbl: Label of the start node

        :param rel_type: Relation type

        :param end_lbl: Label of the End node

        :return: cursor with list of nodes (start, end) that fulfill the criteria, or False if no nodes are found.
        """
        query = """
        MATCH (start:{start_lbl})-[:{rel_type}]->(end:{end_lbl})
        RETURN start, end
        """.format(start_lbl=start_lbl, rel_type=rel_type, end_lbl=end_lbl)
        return self.get_query(query)

    def update_node(self, my_node):
        """
        This method will push node updates to the graph.

        :param my_node:

        :return:
        """
        self.graph.push(my_node)
        return

class Application:
    """
    The Application node.
    """

    def __init__(self, ns):
        self.ns = ns

    def get_node(self, appl_name):
        """
        This method will return the application node. In case the node does not exist, it will be created.

        :param appl_name: Application Name

        :return: Application node
        """
        appl_name = appl_name.lower().replace(" ", "")
        props = dict(name=appl_name)
        nodes = self.ns.get_nodes(lbl_application, **props)
        if nodes:
            return nodes[0]
        else:
            return self.ns.create_node(lbl_application, **props)


class ClientIp:
    """
    ClientIp is identified by client IP address only.
    """

    def __init__(self, ns):
        self.ns = ns

    def get_node(self, rec):
        """
        This method will return the clientip node for the ip address in clientip field. In case the node does not exist,
        it will be created.

        :param rec: Click information, needs to contain the clientIP attribute for the client IP

        :return: clientIp node
        """
        props = dict(ip=rec.clientip)
        nodes = self.ns.get_nodes(lbl_clientip, **props)
        if nodes:
            return nodes[0]
        else:
            return self.ns.create_node(lbl_clientip, **props)


class Log:
    """
    The Log node.
    """

    def __init__(self, ns):
        self.ns = ns

    def create_node(self, rec):
        """
        In this case no get node is done before.

        :param rec:

        :return: The node created is returned.
        """
        # dt = datetime.strptime(rec.timestamp, "%Y-%m-%dT%H:%M:%S")
        # Log Sequence is the measure for the sequence of the clicks (as timestamp is not accurate - up to the second
        # only). This is relative and should be used for clicks in a session for a visitor and on exact same timestamp.
        logseq = rec.id
        # Add node for the click.
        query = """
        CREATE (log:{lbl_logrec} {{ seq:{logseq}, ts:datetime("{ts}") }})
        RETURN log
        """.format(lbl_logrec=lbl_logrecord, logseq=logseq, ts=rec.timestamp[:19])
        cursor = self.ns.get_query(query)
        return next(cursor)["log"]


class Param:
    """
    A param object will manage all parameters. A parameter needs to have a 'naam', to be used as label, and a 'waarde'.
    A parameter object can have a 'definitie'. A parameter object will be linked to the application it belongs to.
    The 'applicatie' attribute will be converted to lowercase and stripped from spaces.
    """

    def __init__(self, ns):
        self.ns = ns

    def get_node(self, pardic):
        """
        This method will return the parameter node. A parameter node is defined by the parameter 'naam' and the
        'waarde'. If the node does not exist, it will be created. Attributes 'definitie' and 'applicatie' will be
        added to the parameter node if available.

        :param pardic: Parameter dictionary. 'naam' is mandatory and must be string. 'waarde' is mandatory. 'definitie'
        and 'applicatie' are optional.

        :return: parameter node.
        """
        lbl = pardic["naam"].lower()
        # Make sure waarde is always a string, otherwise mix of int and str is too complex.
        waarde = str(pardic["waarde"]).lower()
        props = dict(waarde=waarde)
        nodes = self.ns.get_nodes(lbl, **props)
        if nodes:
            return nodes[0]
        else:
            if "definitie" in pardic:
                props["definitie"] = pardic["definitie"]
            param_node = self.ns.create_node(lbl, **props)
            if "applicatie" in pardic:
                appl_node = Application(self.ns).get_node(pardic["applicatie"])
                # props["applicatie"] = pardic["applicatie"].lower().replace(" ", "")
                self.ns.create_relation(param_node, param2appl, appl_node)
            return param_node


class Session:
    """
    A session object is a number of logs (clicks) from a visitor without interruption. If the interruption is larger
    than a threshold value, then a new session is defined.
    """

    def __init__(self, ns):
        self.ns = ns

    def get_node(self, rec):
        """
        This method will return the session node for the log record. In case the node does not exist,
        a session will be created.

        :param rec: Click information, needs to contain the session record.

        :return: visitor node
        """
        sesrec = rec.sess4click[0]
        sid = sesrec.id
        props = dict(sid=sid)
        nodes = self.ns.get_nodes(lbl_session, **props)
        if nodes:
            return nodes[0]
        else:
            # Create Session node
            query = """
            CREATE (session:{lbl_session} {{ sid:{sid}, first:datetime("{first}"), last:datetime("{last}"), 
                                             count:{count} }})
            RETURN session
            """.format(lbl_session=lbl_session, sid=sid, count=sesrec.count, first=sesrec.first, last=sesrec.last)
            cursor = self.ns.get_query(query)
            session_node = next(cursor)["session"]
            visitor_node = Visitor(self.ns).get_node(rec)
            self.ns.create_relation(visitor_node, visitor2session, session_node)
            return session_node


class User:
    """
    User is populated by auth field. The name and the group are added. The organization needs to be vdab.
    The name is the user's unique identifier.
    """

    def __init__(self, ns):
        self.ns = ns

    def get_node(self, rec):
        """
        This method will return the user node for the name in user field. In case the node does not exist,
        it will be created.

        :param rec: Click information, needs to contain the user and auth attributes.

        :return: user node, or False in case the user is not authenticated.
        """
        props = dict(name=rec.user)
        nodes = self.ns.get_nodes(lbl_user, **props)
        if nodes:
            return nodes[0]
        else:
            if rec.auth == "-":
                # logging.debug("User Auth info expected for click record ID {cid}".format(cid=rec.id))
                return False
            else:
                auth_arr = rec.auth.split(",")
                authid, name = auth_arr[0].split("=")
                authid, usergroup = auth_arr[1].split("=")
                if usergroup != "users":
                    logging.warning(
                        "Unexpected Group identifier in authentication: {ug} for record {rid}".format(ug=usergroup,
                                                                                                      rid=rec.id))
                authid, usergroup = auth_arr[2].split("=")
                authid, org = auth_arr[3].split("=")
                if org.lower() != "vdab":
                    logging.warning("Unexpected Organization {org} found for record {rid}".format(org=org, rid=rec.id))
                props = dict(
                    name=name,
                    group=usergroup
                )
                return self.ns.create_node(lbl_user, **props)


class Vacature:
    """
    A vacature node is identified by a vacature ID. It must have an ID and it can have (one or more) titles.
    """

    def __init__(self, ns):
        self.ns = ns

    def add_title(self, vac_node, title):
        """
        This method will check if title needs to be added to the vacature. Titles will be added using "**" as delimiter.

        :param vac_node:

        :param title:

        :return:
        """
        if vac_node["titel"]:
            if not title in vac_node["titel"]:
                # Additional title found for this node
                vac_node["titel"] += " ** {vt}".format(vt=title)
                self.ns.update_node(vac_node)
        else:
            vac_node["titel"] = title
            self.ns.update_node(vac_node)
        return

    def get_node(self, vac_id, title=None):
        """
        This method will return the vacature node, or False in case vac_id is not in required format (8 digits).
        If the vacature ID does not exist, it will be created.

        :param vac_id: must be 8 characters long.

        :param title: Optional, will be added to vacature if it exists.

        :return: vacature node, or False if format is not OK.
        """
        if vac_id.isdigit() and len(vac_id) == 8:
            props = dict(vac_id=vac_id)
            nodes = self.ns.get_nodes(lbl_vacature, **props)
            if nodes:
                return nodes[0]
            else:
                if title:
                    props["titel"] = title
                return self.ns.create_node(lbl_vacature, **props)
        else:
            return False


class Vhost:
    """
    Vhost is identified by hostname (host). Vhost maintains a connection to the vhostIp node.
    """

    def __init__(self, ns):
        self.ns = ns

    def get_node(self, rec):
        """
        This method will return the vhost node for the name in vhost. In case the node does not exist,
        it will be created.

        The link to vhostip will be created if required.

        :param rec: Record with click information. rec.vhost and rec.vhostip are required.

        :return: vhost node
        """
        props = dict(host=rec.vhost)
        nodes = self.ns.get_nodes(lbl_vhost, **props)
        if nodes:
            return nodes[0]
        else:
            vhost_node = self.ns.create_node(lbl_vhost, **props)
            logging.info("Vhost node {host} is created".format(host=rec.vhost))
        # Then check if relation to vhostip exists
        vhostip_node = VhostIp(self.ns).get_node(rec)
        # create_relationship is a merge, so no problem to recreate.
        self.ns.create_relation(vhost_node, vhost2vhostip, vhostip_node)
        return vhost_node


class VhostIp:
    """
    VhostIP is identified by IP.
    """

    def __init__(self, ns):
        self.ns = ns

    def get_node(self, rec):
        """
        This method will return the vhostip node for the ip address in vhostip field. In case the node does not exist,
        it will be created.

        :param rec: Record with click information. rec.vhostip is required.

        :return: vhostip node
        """
        props = dict(ip=rec.vhostip)
        nodes = self.ns.get_nodes(lbl_vhostip, **props)
        if nodes:
            return nodes[0]
        else:
            logging.info("VhostIp node {ip} will be created".format(ip=rec.vhostip))
            return self.ns.create_node(lbl_vhostip, **props)


class Visitor:
    """
    A visitor object is a visitor node connected to a user and a clientIP node. If user is unauthenticated, then the
    visitor object only has a visitor node and a clientip node.
    """

    def __init__(self, ns):
        self.ns = ns

    def get_node(self, rec):
        """
        This method will return the visitor node for the log record. In case the node does not exist,
        a visitor object will be created. This is a visitor node and a relation to a user (if authenticated) and
        a clientIp

        :param rec: Click information, needs to contain the visitor record, clientIp and user information

        :return: visitor node
        """
        vid = rec.visitor[0].id
        props = dict(vid=vid)
        nodes = self.ns.get_nodes(lbl_visitor, **props)
        if nodes:
            return nodes[0]
        else:
            visitor_node = self.ns.create_node(lbl_visitor, **props)
            clientip_node = ClientIp(self.ns).get_node(rec)
            self.ns.create_relation(visitor_node, visitor2clientip, clientip_node)
            if rec.user != "unauthenticated":
                user_node = User(self.ns).get_node(rec)
                self.ns.create_relation(user_node, user2visitor, visitor_node)
            return visitor_node
