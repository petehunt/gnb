import sqlite3
import json
import uuid
import unittest
import threading

class EdgeConfig(object):
  def __init__(self, type, unique=False, bidi=False, inverse_type=None, inverse_unique=None):
    assert not (bidi and inverse_type), 'Cannot be bidirectional and have an inverse type'
    self.type = type
    self.unique = unique
    self.bidi = bidi
    self.inverse_type = inverse_type
    self.inverse_unique = inverse_unique
  def get_inverse(self):
    if not self.inverse_type:
      return None
    return EdgeConfig(self.inverse_type, self.inverse_unique, self.bidi, self.type, self.unique)

class Edge(object):
  def __init__(self, oid1, oid2, type, order=None, data=None):
    self.oid1 = oid1
    self.oid2 = oid2
    self.type = type
    self.order = order
    self.data = data

class ConnectionManager(object):
  ''' blah, hack to make multithreading work '''
  def __init__(self, filename):
    self.filename = filename
    self.local = threading.local()
  def cursor(self):
    if not getattr(self.local, 'conn', None):
      self.local.conn = sqlite3.connect(self.filename)
    return self.local.conn.cursor()
  def commit(self):
    self.local.conn.commit()

class GNB(object):
  def __init__(self, filename):
    self.conn = ConnectionManager(filename)
    self.init_schema()
    self.refresh_edge_config()
  def init_schema(self):
    c = self.conn.cursor()
    c.execute('create table if not exists obj (oid varchar(255) primary key, data text)')
    c.execute('create table if not exists edges (oid1 varchar(255), oid2 varchar(255), type varchar(255), order_ integer, data varchar(255), primary key (oid1, oid2, type))')
    c.execute('create table if not exists edge_config (type varchar(255) primary key, unique_ boolean, bidi boolean, inverse_type varchar(255), inverse_unique boolean)')
  def oid(self):
    return str(uuid.uuid4())
  def obj_get(self, oid):
    c = self.conn.cursor()
    c.execute('select data from obj where oid=?', (oid,))
    return json.loads(c.fetchone()[0])
  def obj_put(self, oid, value):
    c = self.conn.cursor()
    c.execute('insert or replace into obj (oid, data) values (?,?)', (oid, json.dumps(value)))
    self.conn.commit()
  def obj_delete(self, oid):
    c = self.conn.cursor()
    c.execute('delete from obj where oid=?', (oid,))
    self.conn.commit()
  def edge_add(self, edge):
    c = self.conn.cursor()
    config = self.configs[edge.type]
    inverse_config = config.get_inverse()
    edges = [(edge, config)]
    if config.bidi:
      inverse_edge = Edge(edge.oid2, edge.oid1, edge.type, edge.order, edge.data)
      edges.append((inverse_edge, config))
    if inverse_config:
      inverse_edge = Edge(edge.oid2, edge.oid1, inverse_config.type, edge.order, edge.data)
      edges.append((inverse_edge, inverse_config))
    for (edge, config) in edges:
      if config.unique:
        # delete old edges
        old_edges = self.edge_get(edge.oid1, edge.type)
        for old_edge in old_edges.values():
          self.edge_remove(old_edge.oid1, old_edge.oid2, old_edge.type)
      c.execute('insert into edges (oid1, oid2, type, order_, data) values (?,?,?,?,?)', (edge.oid1, edge.oid2, edge.type, edge.order, json.dumps(edge.data)))
      self.conn.commit()
  def edge_remove(self, oid1, oid2, type):
    c = self.conn.cursor()
    config = self.configs[type]
    inverse_config = config.get_inverse()
    edges_to_remove = [(oid1, oid2, type)]
    if config.bidi:
      edges_to_remove.append((oid2, oid1, type))
    if inverse_config:
      edges_to_remove.append((oid2, oid1, inverse_config.type))
    for (oid1, oid2, type) in edges_to_remove:
      c.execute('delete from edges where oid1=? and oid2=? and type=?', (oid1, oid2, type))
    self.conn.commit()
  def edge_get(self, oid, type, start=None, end=None):
    c = self.conn.cursor()
    query = 'select oid1, oid2, type, order_, data from edges where oid1=? and type=?'
    args = [oid, type]
    if start:
      query += ' and order_ >= ?'
      args.append(start)
    if end:
      query += ' and order_ <= ?'
      args.append(end)

    query += ' order by order_'
    c.execute(query, tuple(args))
    edges = {}
    for (oid1, oid2, type, order, data) in c.fetchall():
      edges[oid2] = Edge(oid1, oid2, type, order, data)
    return edges
  def edge_get_one(self, oid, type):
    return self.edge_get(oid, type).values()[0]
  def edge_config_add(self, config):
    # write the config
    c = self.conn.cursor()
    configs =[config]
    inverse = config.get_inverse()
    if inverse:
      configs.append(inverse)
    for config in configs:
      c.execute('insert or replace into edge_config (type, unique_, bidi, inverse_type, inverse_unique) values (?,?,?,?,?)', (
          config.type, config.unique, config.bidi, config.inverse_type, config.inverse_unique))
    self.refresh_edge_config()
  def refresh_edge_config(self):
    c = self.conn.cursor()
    self.configs = {}
    c.execute('select type, unique_, bidi, inverse_type, inverse_unique from edge_config')
    for (type, unique, bidi, inverse_type, inverse_unique) in c.fetchall():
      self.configs[type] = EdgeConfig(type, unique, bidi, inverse_type, inverse_unique)

class GNBTestCase(unittest.TestCase):
  def setUp(self):
    self.gnb = GNB(':memory:')
  def assertEmpty(self, l):
    return len(l) == 0
  def testObj(self):
    self.gnb.obj_put('pete', {'x':'y'})
    self.assertEquals(self.gnb.obj_get('pete'), {'x':'y'})
  def testEdge(self):
    self.gnb.edge_config_add(EdgeConfig('testedge'))
    self.gnb.obj_put('pete', 'pete')
    self.gnb.obj_put('pete2', 'pete2')
    self.gnb.edge_add(Edge('pete', 'pete2', 'testedge'))
    self.assertEquals(self.gnb.edge_get_one('pete', 'testedge').oid2, 'pete2')
  def testRange(self):
    self.gnb.edge_config_add(EdgeConfig('testedge'))
    self.gnb.obj_put('pete', 'pete')
    self.gnb.obj_put('pete2', 'pete2')
    self.gnb.obj_put('pete3', 'pete3')
    self.gnb.edge_add(Edge('pete', 'pete2', 'testedge', order=1))
    self.gnb.edge_add(Edge('pete', 'pete3', 'testedge', order=10))
    self.assertEquals(self.gnb.edge_get('pete', 'testedge', 0, 5).keys(), ['pete2'])
  def testBidi(self):
    self.gnb.edge_config_add(EdgeConfig('testedge', False, True))
    self.gnb.obj_put('a', 'a')
    self.gnb.obj_put('b', 'b')
    self.gnb.edge_add(Edge('a', 'b', 'testedge'))
    self.assertEquals(self.gnb.edge_get_one('a', 'testedge').oid2, 'b')
    self.assertEquals(self.gnb.edge_get_one('b', 'testedge').oid2, 'a')
    # deleting the edge should delete both directions
    self.gnb.edge_remove('a', 'b', 'testedge')
    self.assertEmpty(self.gnb.edge_get('a', 'testedge'))
    self.assertEmpty(self.gnb.edge_get('b', 'testedge'))
  def testAsymmetrical(self):
    self.gnb.edge_config_add(EdgeConfig('testedge', False, False, 'testedge2', False))
    self.gnb.obj_put('a', 'a')
    self.gnb.obj_put('b', 'b')
    self.gnb.edge_add(Edge('a', 'b', 'testedge'))
    self.assertEquals(self.gnb.edge_get_one('a', 'testedge').oid2, 'b')
    self.assertEmpty(self.gnb.edge_get('a', 'testedge2'))
    self.assertEquals(self.gnb.edge_get_one('b', 'testedge2').oid2, 'a')
    self.assertEmpty(self.gnb.edge_get('b', 'testedge'))
    # deleting the edge should delete both directions
    self.gnb.edge_remove('a', 'b', 'testedge')
    self.assertEmpty(self.gnb.edge_get('a', 'testedge'))
    self.assertEmpty(self.gnb.edge_get('b', 'testedge'))
    self.assertEmpty(self.gnb.edge_get('a', 'testedge2'))
    self.assertEmpty(self.gnb.edge_get('b', 'testedge2'))
  def testUnique(self):
    self.gnb.edge_config_add(EdgeConfig('testedge', True))
    self.gnb.obj_put('a', 'a')
    self.gnb.obj_put('b', 'b')
    self.gnb.obj_put('c', 'c')
    self.gnb.edge_add(Edge('a', 'b', 'testedge'))
    self.gnb.edge_add(Edge('a', 'c', 'testedge'))
    self.assertEquals(self.gnb.edge_get('a', 'testedge').keys(), ['c'])

if __name__ == '__main__':
  unittest.main()
