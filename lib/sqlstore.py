"""
This module consolidates Database access for the lkb project.
"""

import logging
import os
import sqlite3
from sqlalchemy import Column, Integer, Text, create_engine, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


Base = declarative_base()


class Click(Base):
    """
    Table containing clickdata
    """
    __tablename__ = "clicks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    clientip = Column(Text)
    duration = Column(Integer)
    protocol = Column(Text)
    urlpath = Column(Text)
    urlquery = Column(Text)
    bytes = Column(Integer)
    timestamp = Column(Text)
    auth = Column(Text)
    user = Column(Text)
    uid = Column(Text)
    useragent = Column(Text)
    vhost = Column(Text)
    vhostip = Column(Text)
    source = Column(Text, comment="Source File for this record")


class Click2Session(Base):
    """
    Every log record needs to be associated with a session. This table links the logs with the sessions.
    """
    __tablename__ = "click2session"
    id = Column(Integer, primary_key=True, autoincrement=True)
    click_id = Column(Integer, ForeignKey("clicks.id"))
    session_id = Column(Integer, ForeignKey("sessions.id"))


class Click2Visitor(Base):
    """
    Every log record needs to be associated with a visitor. This table links the logs with the visitors.
    """
    __tablename__ = "click2visitor"
    id = Column(Integer, primary_key=True, autoincrement=True)
    click_id = Column(Integer, ForeignKey('clicks.id'))
    visitor_id = Column(Integer, ForeignKey('visitors.id'))
    # click = relationship("Click", foreign_keys=[click_id], backref="visitor")
    # visitor = relationship("Visitor", foreign_keys=[visitor_id])


class Session(Base):
    """
    Table containing session information. A session is a continuous usage of an IP address. Continuous usage means the
    intervals between logs is never more than a fixed period of time (currently 30 minutes).
    """
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    visitor_id = Column(Integer, ForeignKey('visitors.id'))
    first = Column(Text, nullable=False)
    last = Column(Text, nullable=False)
    bot = Column(Text)
    count = Column(Integer, nullable=False)
    session4visitor = relationship("Visitor", backref="inSessions")
    clicksPerSession = relationship("Click", secondary="click2session", backref="sess4click")


class Visitor(Base):
    """
    Table containing visitor information. A visitor is identified by clientip and user name.
    """
    __tablename__ = "visitors"
    id = Column(Integer, primary_key=True, autoincrement=True)
    clientip = Column(Text, nullable=False)
    user = Column(Text, nullable=False)
    hasClicks = relationship("Click", secondary="click2visitor", backref="visitor")


class DirectConn:
    """
    This class will set up a direct connection to the database. It allows to reset the database,
    in which case the database will be dropped and recreated, including all tables.
    """

    def __init__(self, config):
        """
        To drop a database in sqlite3, you need to delete the file.
        """
        self.db = config['Main']['db']
        self.dbConn = ""
        self.cur = ""

    def connect2db(self):
        """
        Internal method to create a database connection and a cursor. This method is called during object
        initialization.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.

        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Datastore object and cursor")
        self.dbConn = sqlite3.connect(self.db)
        self.dbConn.row_factory = sqlite3.Row
        self.cur = self.dbConn.cursor()
        logging.debug("Datastore object and cursor are created")
        return

    def db_commit(self):
        """
        This method will commit open actions as required.

        :return:
        """
        self.dbConn.commit()
        return

    def get_query(self, query):
        """
        This method will get a query and return the result of the query.

        :param query:

        :return:
        """
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def get_table(self, tablename):
        """
        This method will return the table as a list of named rows. This means that each row in the list will return
        the table column value as an attribute. E.g. row.name will return the value for column name in each row.

        :param tablename:

        :return:
        """
        query = "SELECT * FROM {t}".format(t=tablename)
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def insert_row(self, tablename, rowdict):
        columns = ", ".join(rowdict.keys())
        values_template = ", ".join(["?"] * len(rowdict.keys()))
        query = "insert into {tn} ({cols}) values ({vt})".format(tn=tablename, cols=columns, vt=values_template)
        values = tuple(rowdict[key] for key in rowdict.keys())
        self.cur.execute(query, values)
        return self.cur.lastrowid

    def rebuild(self):
        # A drop for sqlite is a remove of the file
        try:
            os.remove(self.db)
            logging.info("Database {db} will be recreated".format(db=self.db))
        except FileNotFoundError:
            logging.info("New database {db} will be created".format(db=self.db))
        # Reconnect to the Database
        self.connect2db()
        # Use SQLAlchemy connection to build the database
        conn_string = "sqlite:///{db}".format(db=self.db)
        engine = set_engine(conn_string=conn_string)
        Base.metadata.create_all(engine)

    def run_query(self, query):
        """
        This method will run a query and not return any value. This can be used for CREATE TABLE or DELETE statements

        :param query:

        :return:
        """
        self.cur.execute(query)
        return

    def update_row(self, tn, rowdict):
        """
        This method will update the record with ID rowdict["id"] with the values for columns as specified in dictionary
        rowdict.

        :param tn: tablename

        :param rowdict: "id" field is mandatory

        :return:
        """
        try:
            rec_id = rowdict.pop("id")
        except KeyError:
            logging.error("Trying to update table {tn} with {d}, but record ID is missing.".format(tn=tn, d=rowdict))
            return False
        else:
            setarr = ["{k}=?".format(k=k) for k in rowdict]
            settemp = ", ".join(setarr)
            vals = [rowdict[k] for k in rowdict]
            vals.append(rec_id)
            query = "update {tn} set {settemp} where id=?".format(tn=tn, settemp=settemp)
            try:
                self.cur.execute(query, tuple(vals))
                return self.cur.lastrowid
            except sqlite3.InterfaceError:
                logging.error("Something bad happened with query {q} and values {v}".format(q=query, v=vals))
                return  False


def init_session(db, echo=False):
    """
    This function configures the connection to the database and returns the session object.

    :param db: Name of the sqlite3 database.

    :param echo: True / False, depending if echo is required. Default: False

    :return: session object.
    """
    conn_string = "sqlite:///{db}".format(db=db)
    engine = set_engine(conn_string, echo)
    session = set_session4engine(engine)
    return session


def set_engine(conn_string, echo=False):
    engine = create_engine(conn_string, echo=echo)
    return engine


def set_session4engine(engine):
    session_class = sessionmaker(bind=engine)
    session = session_class()
    return session
