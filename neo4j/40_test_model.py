"""
This script will run a number of test scripts.
"""
import unittest
from lib import my_env
from lib import neostore


class TestNeoModel(unittest.TestCase):

    def setUp(self):
        # Initialize environment
        cfg = my_env.init_env("vdab", __file__)
        self.ns = neostore.NeoStore(cfg, refresh="No")

    def test_session2visitor(self):
        # A Session needs to be connected to one and only one Visitor.
        # Find sessions connected to more than one visitor
        query = """
        match (s:Sessie)--(v:Visitor),
              (s)--(w:Visitor)
        where v <> w
        return s, v, w
        """
        cur = self.ns.get_query(query)
        self.assertIsNone(cur.evaluate())
        # There should be no session without a visitor.
        query = "match (s:Sessie) where not (s)<-[:heeftSessie]-(:Visitor) return s"
        cur = self.ns.get_query(query)
        self.assertIsNone(cur.evaluate())
        # Every session needs to be connected to a visitor using a for-relation
        # Count number of sessions
        query = "match (s:Session) return count(s) as cnt"
        cur = self.ns.get_query(query)
        sescnt = cur.evaluate()
        # Count number of for-relations between sessions and visitors
        query = "match (s:Session)<-[rel:heeftSessie]-(:Visitor) return count(rel) as cnt"
        cur = self.ns.get_query(query)
        relcnt = cur.evaluate()
        self.assertEqual(sescnt, relcnt)

    def test_visitor2clientIp(self):
        # Each visitor needs to be connected to exactly one clientIp
        query = """
        match (v:Visitor)-[:heeftIP]->(c1:IP),
              (v)-[:heeftIP]->(c2:IP)
        where c1 <> c2
        return v, c1, c2
        """
        cur = self.ns.get_query(query)
        self.assertIsNone(cur.evaluate())
        # Each visitor needs to be connected to not less than one clientIp
        query = "match (v:Visitor) where not (v)-[:onDevice]->(:ClientIp) return v"
        cur = self.ns.get_query(query)
        self.assertIsNone(cur.evaluate())

    def test_session2vhost(self):
        # Each session needs to be connected to one or more vhosts
        query = "match (s:Session) where not (s)-[:forHost]->(:Vhost) return s"
        cur = self.ns.get_query(query)
        self.assertIsNone(cur.evaluate())

    def test_user2visitor(self):
        # Each user needs to be connected to at least one visitor.
        query = "match (u:User) where not (u)<-[:isUser]-(:Visitor) return u"
        cur = self.ns.get_query(query)
        self.assertIsNone(cur.evaluate())
        # Each visitor can have maximum one user
        query = """
        match (v:Visitor)--(u1:User),
              (v)--(u2:User)
        where u1 <> u2
        return v, u1, u2
        """
        cur = self.ns.get_query(query)
        self.assertIsNone(cur.evaluate())

if __name__ == "__main__":
    # cfg = my_env.init_env("vdab", __file__)
    unittest.main()
