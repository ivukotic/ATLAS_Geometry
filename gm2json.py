#!/anaconda/bin/python

### declarative autoload method ###
# short-way:
# - using autoload
# - using the declarative way
#
# refs:
# - http://www.blog.pythonlibrary.org/2010/09/10/sqlalchemy-connecting-to-pre-existing-databases/

# use: gm2json.py -i geometry_atlas_20Apr17.db

"""
child table has id and position. it is not a regular table.

"""


import argparse

import json
import sys
import os
import copy
import pprint
import logging

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from elasticsearch import Elasticsearch, helpers, exceptions as es_exceptions

PARSER = argparse.ArgumentParser(description='This code indexes geo info into ES.')
PARSER.add_argument('-i', '--input', help='Input file name', required=True)
ARGS = PARSER.parse_args()

## show values ##
print("Input file: %s" % ARGS.input)
DB_PATH = ARGS.input


if not os.path.isfile(DB_PATH):
    logging.warning("could not find the input DB file!! Exiting...")
    sys.exit()
ENGINE = create_engine('sqlite:///%s' % DB_PATH, echo=False)
BASE = declarative_base(ENGINE)

es = Elasticsearch([{'host':'atlas-kibana.mwt2.org', 'port':9200}],timeout=60)
docs_to_store=[]

# debug containers
NOTEXPANDED = set()  # store the GeoModel objects which are not expanded

#------------------------------------------------------------------------


class MyBaseClass(object):
    """ a general node object can be from any table."""
    __table__ = None
    def as_dict(self):
        """returns a dictionary of all the columns and values
        eg {'id': 3798, 'name': u'BLM Module'}
        """
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

### Utility tables ###


class GeoNodesType(BASE, MyBaseClass):
    """ there are 11 node types
    id:1 nodeType:GeoPhysVol tableName:PhysVols
    id:2 nodeType:GeoFullPhysVol tableName:FullPhysVols
    ...
    id:11 nodeType:GeoNameTag tableName:NameTags
    """
    nodeType = None
    tableName = None
    __tablename__ = 'GeoNodesTypes'
    __table_args__ = {'autoload': True}

class RootVolume(BASE, MyBaseClass):
    """docstring for ."""
    __tablename__ = 'RootVolume'
    __table_args__ = {'autoload': True}


class ChildPos(BASE, MyBaseClass):
    """docstring for ."""
    parentId = None
    __tablename__ = 'ChildrenPositions'
    __table_args__ = {'autoload': True}




### GeoModel objects tables ###

class PhysVol(BASE, MyBaseClass):
    """ from table 1. only points to a logvol"""
    logvol = None
    __tablename__ = 'PhysVols'
    __table_args__ = {'autoload': True}
    def __repr__(self):
        return self.as_dict().__str__()

class LogVol(BASE, MyBaseClass):
    """ from table 3. """
    name = None
    shape = None
    material = None
    __tablename__ = 'LogVols'
    __table_args__ = {'autoload': True}


class Material(BASE, MyBaseClass):
    """from table 4"""
    name = None
    __tablename__ = 'Materials'
    __table_args__ = {'autoload': True}


class Shape(BASE, MyBaseClass):
    """from table 5."""
    type = None  # string like box, cylinder etc.
    parameters = None # string that needs parsing.
    __tablename__ = 'Shapes'
    __table_args__ = {'autoload': True}


class SerialDenominator(BASE, MyBaseClass):
    """docstring for ."""
    __tablename__ = 'SerialDenominators'
    __table_args__ = {'autoload': True}


class Function(BASE, MyBaseClass):
    """docstring for ."""
    __tablename__ = 'Functions'
    __table_args__ = {'autoload': True}


class SerialTransformer(BASE, MyBaseClass):
    """docstring for ."""
    __tablename__ = 'SerialTransformers'
    __table_args__ = {'autoload': True}


class AlignableTransform(BASE, MyBaseClass):
    """in table 10. only these 12 variables and id
      xx, xy, xz, yx, yy, yz, zx, zy, zz
      dx, dy, dz
    """
    __tablename__ = 'AlignableTransforms'
    __table_args__ = {'autoload': True}
    def __repr__(self):
        return [self.xx, self.xy, self.xz, self.yx, self.yy, self.yz, self.zx, self.zy, self.zz, self.dx, self.dy, self.dz].__str__()


class NameTags(BASE, MyBaseClass):
    """docstring for ."""
    __tablename__ = 'NameTags'
    __table_args__ = {'autoload': True}
    def __repr__(self):
        return self.as_dict().__str__()

class FullPhysVols(BASE, MyBaseClass):
    """ points to a log_vol. comes from table 2."""
    logvol = None
    __tablename__ = 'FullPhysVols'
    __table_args__ = {'autoload': True}
    def __repr__(self):
        return self.as_dict().__str__()


class Transforms(BASE, MyBaseClass):
    """in table 9. only these 12 variables and id
       xx, xy, xz, yx, yy, yz, zx, zy, zz
       dx, dy, dz
    """
    __tablename__ = 'Transforms'
    __table_args__ = {'autoload': True}
    def __repr__(self):
        return [self.xx, self.xy, self.xz, self.yx, self.yy, self.yz, self.zx, self.zy, self.zz, self.dx, self.dy, self.dz].__str__()

#----------------------------------------------------------------------
class Transf():
    """ used for folding all the transforms """
    def __init__(self):
        self.xx = 1.0
        self.xy = 0.0
        self.xz = 0.0

        self.yx = 0.0
        self.yy = 1.0
        self.yz = 0.0

        self.zx = 0.0
        self.zy = 0.0
        self.zz = 1.0

        self.dx = 0.0
        self.dy = 0.0
        self.dz = 0.0
    def add_transform(self, rt):
        s11 = self.xx * rt.xx + self.yx * rt.xy + self.zx * rt.xz
        s12 = self.xy * rt.xx + self.yy * rt.xy + self.zy * rt.xz
        s13 = self.xz * rt.xx + self.yz * rt.xy + self.zz * rt.xz

        s21 = self.xx * rt.yx + self.yx * rt.yy + self.zx * rt.yz
        s22 = self.xy * rt.yx + self.yy * rt.yy + self.zy * rt.yz
        s23 = self.xz * rt.yx + self.yz * rt.yy + self.zz * rt.yz

        s31 = self.xx * rt.zx + self.yx * rt.zy + self.zx * rt.zz
        s32 = self.xy * rt.zx + self.yy * rt.zy + self.zy * rt.zz
        s33 = self.xz * rt.zx + self.yz * rt.zy + self.zz * rt.zz

        self.xx = s11
        self.xy = s12
        self.xz = s13
        self.yx = s21
        self.yy = s22
        self.yz = s23
        self.zx = s31
        self.zy = s32
        self.zz = s33

        self.dx += rt.dx
        self.dy += rt.dy
        self.dz += rt.dz

    def matrix(self):
        """ returning rotation matrix only """
        return [self.xx, self.xy, self.xz, self.yx, self.yy, self.yz, self.zx, self.zy, self.zz, self.dx, self.dy, self.dz]

def load_session():
    """docstring for ."""
    # metadata = BASE.metadata
    lsession = sessionmaker(bind=ENGINE)
    return lsession()


def dumpTable(lsession, table):
    """ something """
    #out = {}
    res = lsession.query(table).all()
    items = [u.as_dict() for u in res]
    # print "\n%s:"%table.__tablename__, json.dumps( items )
    # out[table.__tablename__] = json.dumps( items )
    # out[table.__tablename__] = items
    return items


def get_item_from_table(tableName, itemId):
    # print "tableName:", tableName, "- itemId:", itemId # debug
    tableClass = get_class_by_tablename(tableName)
    res = SESSION.query(tableClass).filter(tableClass.id == itemId).one()
    return res


def get_item_from_NodeType(nodeType, itemId):
    tableName = get_table_name_from_NodeType(nodeType)
    item = get_item_from_table(tableName, itemId)
    return item


def get_type_and_item(table_id, item_id):
    """ for a given tableID and itemID returns
    nodeType and the item itself
    """
    table_name = get_tablename_from_tableid(table_id)
    node_type = get_nodetype_from_tableid(table_id)
    item = get_item_from_table(table_name, item_id)
    return (node_type, item)


def get_tablename_from_tableid(table_id):
    """ for a given tableID returns tableName """
    res = SESSION.query(GeoNodesType).filter(GeoNodesType.id == table_id).one()
    return res.tableName


def get_table_name_from_NodeType(node_type):
    """ something """
    res = SESSION.query(GeoNodesType).filter(GeoNodesType.nodeType == node_type).one()
    return res.tableName


def get_nodetype_from_tableid(tableId):
    """ something """
    res = SESSION.query(GeoNodesType).filter(GeoNodesType.id == tableId).one()
    return res.nodeType









def get_children_of_this_vol(node_id, node_table):
    """ get volume children and their positions """
    ret = SESSION.query(ChildPos).filter(ChildPos.parentId == node_id).all()
    res = []
    for child in ret:
        if child.parentTable == node_table: # only if parent table was PhysVols
            res.append(child)
    return [(u.id, u.parentId, u.childTable, u.childId, u.position,) for u in res]


def getPhysVolChildrenExpanded(parentId, jsonOut=False):
    """ get volume children, expanded view """

    # get the list of children of this volume
    children = get_children_of_phys_vol(parentId)

    childrenDict = {}
    childrenDict['physVolId'] = parentId
    childrenDict['children'] = {}

    for child in children:
        id = child[0]
        parentId = child[1]
        childTable = child[2]
        childId = child[3]
        position = child[4]

        typeAndItem = get_type_and_item(childTable, childId)
        logging.debug("item: " + typeAndItem.__repr__())  # debug

        nodeType = typeAndItem[0]
        nodeItem = typeAndItem[1]

        # childrenDict['children'] = {}
        childrenDict['children'][position] = {}
        childrenDict['children'][position]['type'] = nodeType
        childrenDict['children'][position]['object'] = nodeItem.as_dict()

    if jsonOut:
        return json.dumps(childrenDict)
    return childrenDict


def get_all_nodes(node, tags, current_transform, current_depth, max_depth):
    """ Main function that starts traverse. Not recursively callable """

    print("*" * current_depth, end=' ')
    print("node - ", node.as_dict())
    #print("node-  id:", node.id, "\ttable:", node.volTable)

    if current_depth==0: #root node
        children = get_children_of_this_vol(node.id, 1)
    else:
        children = get_children_of_this_vol(node.id, node.logvol)

    if not children:
        print("no children. Returns...")
        return
    else:
        print("got:", len(children), "children")

    folded_transform = copy.copy(current_transform)

    for child in children:
        # id = child[0]  unused ?
        #parentId = child[1]  unused ?
        child_table = child[2]
        child_id = child[3]
        position = child[4]

        (node_type, node) = get_type_and_item(child_table, child_id)


        print(" pos:", position, 'tags:', tags, end=' ')
        print("type: " + node_type.__repr__()  + "   item:" + node.__repr__())

        if node_type == "GeoNameTag":
            tags[current_depth] = node.name
            continue

        if node_type == "GeoAlignableTransform" or node_type == "GeoTransform":
            folded_transform.add_transform(node)
            continue

        if node_type == "GeoPhysVol" or node_type == "GeoFullPhysVol":
            #get_phys_vol_children(node, tags, current_transform, current_depth+1, max_depth)
            phys_vol_expanded = get_physvol_item(node)
            generate_document(phys_vol_expanded, current_depth, tags, folded_transform)
            
            #print(node.as_dict())

            if current_depth < max_depth:
                get_all_nodes(node, tags, folded_transform, current_depth+1, max_depth)
            if current_depth+1 in tags: # can't leave them hanging
                del tags[current_depth+1]
            continue

        #else:
        #    continue
        # if (node_type == "GeoSerialDenominator"):
        #     nodeItemExpanded = get_physvol_item(node)
        #elif (node_type == "GeoSerialTransformer"):
        #    (volId, nodeItemExpanded) = getSerialTransformerItemExpanded(node)
        # nodeItemExpanded = get_all_nodes_recurse(volId, depth-1, nodeItemExpanded['vol'])
        # else:
            # print "WARNING!! node type", nodeType, "not expanded"
            #NOTEXPANDED.add(node_type)
            #node_item_expanded = node.as_dict()


        # if node_type == 'GeoPhysVol' or node_type == "GeoFullPhysVol":  # only these can branch
        #     get_all_nodes_recurse(node, tags, folded_transform, current_depth+1, max_depth)
        #     if current_depth+1 in tags: # can't leave them hanging
        #         del tags[current_depth+1]
    return

def get_phys_vol_children(node, tags, current_transform, current_depth=0, max_depth=1):
    '''this should actually be recursive function'''
    logvol_item = get_item_from_table('LogVols', node.id)
    print(logvol_item)
    #children = get_children_of_this_vol(node.id, node.logvol)
    #print(len(children))

def get_physvol_item(item):
    ''' returns physvol content with logvol replaced with it's content '''
    item_dict = item.as_dict()
    #print("item_dict:", item_dict)
    if not 'logvol' in item_dict:
        print("Error! Item is not a GeoPhysVol!")
        # print item_dict
        return
    child_id = item_dict['logvol']
    logvol_item = get_item_from_table('LogVols', child_id)
    item_dict['logvol'] = {}  # transform 'logvol' entry in a dict
    item_dict['logvol']['object'] = get_logvol_item(logvol_item)  # get logVol item expanded
    item_dict['logvol']['type'] = "GeoLogVol"
    return item_dict

def get_logvol_item(item):
    """
    returns content of logvol
    called only from get_physvol_item
    """

    item_dict = item.as_dict()
    if not 'shape' in item_dict:
        print("Error! Item is not a GeoLogVol!")
        # print item_dict
        return
    # print "logvol item:", item_dict
    # get material expanded
    mat_id = item_dict['material']
    mat = get_item_from_NodeType('GeoMaterial', mat_id)
    item_dict['material'] = {}  # transform 'material' entry in a dict
    item_dict['material'] = mat.as_dict()
    # get shape expanded
    shape_id = item_dict['shape']
    shape = get_item_from_NodeType('GeoShape', shape_id)
    item_dict['shape'] = shape.as_dict()
    del item_dict['shape']['id']
    del item_dict['material']['id']
    return item_dict

def getSerialTransformerItemExpanded(item):
    '''getSerialTransformerItemExpanded'''
    itemDict = item.as_dict()
    # print itemDict:", itemDict
    if not 'func' in itemDict:
        print("Error! Item is not a GeoSerialTransformer!")
        # print itemDict
        return
    # get function expanded
    funcId = itemDict['func']
    function = getItemFromTable('Functions', funcId)
    itemDict['func'] = {}  # transform 'func' entry in a dict
    itemDict['func']['object'] = function.as_dict()
    itemDict['func']['type'] = "Function"
    # get physVol expanded
    volId = itemDict['vol']
    vol = getItemFromNodeType('GeoPhysVol', volId)
    # TODO: this PhysVol should be checked for children, as well!!!
    itemDict['vol'] = {}  # transform 'vol' entry in a dict
    itemDict['vol']['object'] = get_physvol_item(vol)
    itemDict['vol']['type'] = "GeoPhysVol"
    return (volId, itemDict)





def getPhysVol():
    pass




def generate_document(item, depth, tags, transform):
    """ this will produce full json doc to index """
    print('-'*20)
    doc = {}
    doc['depth'] = depth
    doc['tags'] = [ v for v in tags.values() ]
    doc['transform'] = transform.matrix()
    sit = item['logvol']['object']
    doc['shape'] = sit['shape']['type']
    doc['dimensions'] = sit['shape']['parameters']
    doc['material'] = sit['material']['name']
    doc['name'] = sit['name']
    print(doc)
    doc['_index'] = 'atlas_geo'
    doc['_type'] = 'vol'
    print('-'*20)
    docs_to_store.append(doc)




def dumpAllObjects(jsonOut=False, jsonFile=False):
    '''get a dict with all tables'''
    out = {}
    #out['GeoNodesType'] = dumpTable(SESSION, GeoNodesType)
    #out['children_positions'] = dumpTable(SESSION, ChildPos)
    #out['RootVolume'] = dumpTable(SESSION, RootVolume)
    #out['GeoPhysVol'] = dumpTable(SESSION, PhysVol)
    #out['GeoLogVol'] = dumpTable(SESSION, LogVol)
    #out['GeoMaterial'] = dumpTable(SESSION, Material)
    #out['GeoShape'] = dumpTable(SESSION, Shape)
    #out['GeoSerialDenominator'] = dumpTable(SESSION, SerialDenominator)
    #out['Function'] = dumpTable(SESSION, Function)
    #out['GeoSerialTransformer'] = dumpTable(SESSION, SerialTransformer)
    #out['GeoAlignableTransform'] = dumpTable(SESSION, AlignableTransform)

    if jsonOut:
        return json.dumps(out)
    if jsonFile:
        with open(jsonFile, 'w') as outfile:
            json.dump(out, outfile)
            msg = "JSON data saved to file '%s'." % jsonFile
            logging.warning(msg)
        return

    return out

def store(docs):
    """ bulk indexing """
    print('storing', len(docs), 'volumes')
    try:
        res = helpers.bulk(es, docs, raise_on_exception=True, request_timeout=60)
        #print("inserted:",res[0], '\tErrors:',res[1])
    except es_exceptions.ConnectionError as e:
        print('ConnectionError ', e)
    except es_exceptions.TransportError as e:
        print('TransportError ', e)
    except helpers.BulkIndexError as e:
        print(e[0])
        for i in e[1]:
            print(i)
    except Exception as e:
        print('Something seriously wrong happened.', e)
    print('done')

def get_class_by_tablename(table_fullname):
    """Return class reference mapped to table.
    :param table_fullname: String with fullname of table.
    :return: Class reference or None.
    """

    classItem = None

    for c in BASE._decl_class_registry.values():
        if hasattr(c, '__table__') and c.__table__.fullname == table_fullname:
            classItem = c

    if classItem:
        return classItem
    else:
        logging.warning("ERROR!! Table '%s' not handled yet!" % table_fullname)
        sys.exit()


if __name__ == "__main__":

    SESSION = load_session()

    # get the root PhysVol volume
    ROOT = SESSION.query(RootVolume).one()
    print("rootVol:", ROOT.as_dict())

    get_all_nodes(ROOT, {}, Transf(), 0, 20)
    store(docs_to_store)

    # get a dict with all tables
    #print "\nout [Python dict]:", dumpAllObjects() # to screen as Python dict
    #print "\nout [JSON]:", dumpAllObjects(jsonOut=True) # to screen as JSON

    #dumpAllObjects( jsonFile = args.output+".json" ) # to file as JSON

    # get the children of the root PhysVol in the compact format
    # print getPhysVolChildren(ROOT.id) # compact format, only tableId and
    # childId

    # print the children of the PhysVol with id=2
    # print getPhysVolChildren(2) # compact format
    # print getPhysVolChildrenExpanded(2) # expanded format

    # pprint.pprint(getPhysVolChildrenExpanded(2))

    # get the children of the root PhysVol in the expanded format
    # print
    # only one level of children

    # pprint.pprint(all_children, depth=20, width=120)
    #for w in all_children:
    #    print(w)
    # print get_all_nodes_recurse(ROOT.id, depth=2) # two levels of children
    # pprint.pprint( get_all_nodes_recurse(ROOT.id, depth=3) ) #
    # two levels of children

    # get output in JSON format, on the screen
    # print get_all_nodes_recurse(ROOT.id, depth=3, jsonOut=True)
    # # two levels of children

    # get_all_nodes_recurse(ROOT.id, depth=3) # two levels of
    # children

    print("nodes not expanded:", NOTEXPANDED)

    # nPhysVol = dumpTable(SESSION, PhysVol)
    # print ('physVol',len(nPhysVol))

    # nLogVol = dumpTable(SESSION, LogVol)
    # print ('logVol',len(nLogVol))

    # nLinks = dumpTable(SESSION, ChildPos)
    # print ('links',len(nLinks))
