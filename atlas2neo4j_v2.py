#!/usr/bin/env python

from __future__ import print_function

import sys
import logging

from py2neo import neo4j, node, rel

graph_db = neo4j.GraphDatabaseService()
# graph_db = neo4j.Graph() #DatabaseService()


class Node(object):

    _label = "physvol"

    @classmethod
    def create(cls, volId, *childrenIds):

        # Get or create the node
        vol_node = cls.getNodeFromDB(volId)
        if not vol_node:
            logging.debug("creating", cls._label, "node with Id:", volId)
            vol_node, = graph_db.create( node(volId=volId) )
            vol_node.add_labels( cls._label )
        else:
            logging.debug("node with label", cls._label, "with volId:", volId, "is stored already in the DB. Using that.")

        # Get or create the children nodes and the relationships
        for childId in childrenIds:

            vol_child = cls.getNodeFromDB(nodeId=childId)
            if not vol_child:
                logging.debug("Creating node", childId, "and the relationship", volId, "->", childId)
                vol_child, _ = graph_db.create(node(volId=childId), rel(vol_node, "CHILD", 0))
                vol_child.add_labels( cls._label )
            else:
                logging.debug("Node", childId, "stored already")
                # check if a parent->child relationship with this child node exists already
                rels = list(graph_db.match(start_node=vol_node, end_node=vol_child, rel_type="CHILD"))
                if len(rels) == 0:
                    graph_db.create(rel(vol_node, "CHILD", vol_child))
                else:
                    logging.debug("Relationship", volId, "->", childId, "stored already")
                    pass

        # Return the volume node
        return Node(vol_node)


    @classmethod
    def createNode(cls, volId, volType):
        # Get or create the node
        vol_node = cls.getNodeFromDB(volId, volType)
        if not vol_node:
            # logging.debug("creating", cls._label, "node with Id:", volId)
            logging.debug("creating", volType, "node with Id:", volId)
            vol_node, = graph_db.create( node(volId=volId) )
            # vol_node.add_labels( cls._label )
            vol_node.add_labels( volType )
        else:
            # logging.debug("node with label", cls._label, "with volId:", volId, "is stored already in the DB. Using that.")
            logging.debug("parent node with label", volType, "with volId:", volId, "is stored already in the DB. Using that.")



    @classmethod
    def createChild(cls, volId, childId, position, parentType="", childType=""):

        # Get or create the parent node
        vol_node = cls.getNodeFromDB(volId, parentType)
        if not vol_node:
            # logging.debug("creating", cls._label, "node with Id:", volId)
            logging.debug("creating", parentType, "node with Id:", volId)
            vol_node, = graph_db.create( node(volId=volId) )
            # vol_node.add_labels( cls._label )
            vol_node.add_labels( parentType )
        else:
            # logging.debug("node with label", cls._label, "with volId:", volId, "is stored already in the DB. Using that.")
            logging.debug( " ".join( ["parent node with label", parentType, "with volId:", str(volId), "is stored already in the DB. Using that."] ))

        # # Get or create the children nodes and the relationships
        # for childId in childrenIds:

        vol_child = cls.getNodeFromDB(childId, childType)
        if not vol_child:
            logging.debug( " ".join( ["Creating child node", str(childId), "and the relationship", str(volId), "->", str(childId) ] ) )
            vol_child, _ = graph_db.create(node(volId=childId, position=position), rel(vol_node, "CHILD", 0))
            # vol_child.add_labels( cls._label )
            vol_child.add_labels( childType )
        else:
            logging.debug("Child node", childId, "of type", childType, "stored already")
            # check if a parent->child relationship with this child node exists already
            rels = list(graph_db.match(start_node=vol_node, end_node=vol_child, rel_type="CHILD"))
            if len(rels) == 0:
                graph_db.create(rel(vol_node, "CHILD", vol_child))
            else:
                logging.debug("Relationship", volId, "->", childId, "stored already")
                pass

        # Return the volume node
        return Node(vol_node)


    @classmethod
    def createReferencedNode(cls, volId, volType):
        # Get or create the node
        vol_node = cls.getNodeFromDB(volId, volType)
        if not vol_node:
            # logging.debug("creating", cls._label, "node with Id:", volId)
            logging.debug( " ".join(["creating", volType, "node with Id:", str(volId)]) )
            vol_node, = graph_db.create( node(volId=volId) )
            # vol_node.add_labels( cls._label )
            vol_node.add_labels( volType )
        else:
            # logging.debug("node with label", cls._label, "with volId:", volId, "is stored already in the DB. Using that.")
            logging.debug( "".join( ["parent node with label", volType, "with volId:", str(volId), "is stored already in the DB. Using that."]) )


    @classmethod
    def addRel(cls, parentId, parentType, childId, childType, relString, unique=True):
        logging.debug( " ".join( ["addRel() -", str(parentId), parentType, str(childId), childType, relString]) )
        vol_parent = cls.getNodeFromDB(parentId, parentType)
        vol_child = cls.getNodeFromDB(childId, childType)
        if (vol_parent and vol_child):
            # verify if this relation exists already
            relation = list( graph_db.match(start_node=vol_parent, end_node=vol_child, rel_type=relString) )
            if ( (not relation and unique) or (relation and not unique) ):
                graph_db.create( rel( vol_parent, relString, vol_child) )
            else:
                logging.debug( " ".join( ["Relationship", relString, "between parent ", parentType, str(parentId), "and child", childType, str(childId), "exists already. Skipping..."]) )
        else:
            logging.error( "\nERROR!! parent or child node does not exist!!")
            logging.error( " ".join( ["vol_parent:", vol_parent.__repr__(), "vol_child:", vol_child.__repr__()]) )
            logging.error("Aborting...\n")
            sys.exit()


    @classmethod
    def addPropertiesToNode(cls, volId, volType, properties):
        vol = cls.getNodeFromDB(volId, volType)
        if not vol:
            logging.error("\nERROR!! node does not exist!!")
            logging.error("vol: %s", vol)
            logging.error("Aborting...\n")
            sys.exit()
        # vol.set_properties(properties) # set new properties
        vol.update_properties(properties) # update properties

    @classmethod
    def addLabelsToNode(cls, volId, volType, labels):
        node = cls.getNodeFromDB(volId, volType)
        if not node:
            logging.error( " ".join(["\nERROR!! node", volType, str(volId), "does not exist!! Aborting..."]) )
            sys.exit()
        for label in labels:
            node.add_labels( label )


    @classmethod
    def get_all(cls):
        return [Node(vol.end_node) for vol in cls._root.match("CHILD")]

    @classmethod
    def getNodeFromDB(cls, nodeId, nodeType=_label):
        # vol_node_gen = graph_db.find(cls._label, "volId", nodeId)
        vol_node_gen = graph_db.find(nodeType, "volId", nodeId)
        if (vol_node_gen):
            vol_node_list = list(vol_node_gen)
        else:
            logging.error("ERROR!! Node %s %s not found in the DB!! Aborting...", nodeId, nodeType)
            sys.exit()
        if len(vol_node_list) > 1:
            logging.warning("WARNING!!! Found more than one %s node with Id: %s", nodeType, nodeId)
        elif len(vol_node_list) == 1:
            vol_node = vol_node_list[0]
            # logging.debug("Found: %s", vol_node )
            return vol_node
        else:
            # print("No", cls._label, "node found with Id:", nodeId, " in the DB")
            return None


    def __init__(self, node):
        logging.debug(node)
        self._volNode = node
        self._volId = node["volId"]

    def __str__(self):
        return self._volId + "".join("  <{0}>".format(child) for child in self.children )
        # return "ciao"

    @property
    def volId(self):
        return self._volNode["volId"]

    @property
    def children(self):
        return [rel.end_node["volId"] for rel in self._volNode.match("CHILD")]



if __name__ == "__main__":
    if len(sys.argv) < 2:
        app = sys.argv[0]
        print("Usage: {0} add <volId>".format(app))
        print("       {0} list".format(app))
        sys.exit()
    method = sys.argv[1]
    if method == "add":
        print("created vol with volId", Node.create(*sys.argv[2:]))
    elif method == "list":
        for physvol in Node.get_all():
            print(physvol)
    elif method == "clear":
        graph_db.clear()
        print('DB cleared')
    else:
        print(method + " : Unknown command")
