import requests
import json
import string
import re

class TableQueries:

    def __init__(self, hostname):
        self.hostname = hostname

    def create_database(self, db_name):
        # Create a new Node for the database
        path = self.hostname + "/nodes/{}".format(db_name)
        db_node = requests.post(path).json()

        # Create new NodeVersion
        db_node_version = self.create_node_version(db_node["id"])
        return db_node_version

    def get_database(self, db_name):
        """
        Retrieves the inforamtion about the database called *db_name*
        """
        db_node_version = self.get_latest_node_version(db_name)
        db_info = [db_name]
        db_info.append(self.get_node_metadata(db_node_version))
        return db_info

    def drop_database(self, db_name):
        """
        Drops the databse called *db_name* by creating a new NodeVersion
        with a "dropped" Tag.
        """
        db_node_version = self.get_latest_node_version(db_name)
        node_id = db_node_version["nodeId"]
        parent_ids = db_node_version["id"]
        tag_map = {}
        tag_map["dropped"] = {
            "versionId": "",
            "key": "dropped",
            "value": "dropped",
            "type": "string"
        }
        db_updated_node_version = self.create_node_version(node_id, \
                                        tag_map=tag_map, parents=list(parent_ids))

    def get_all_tables(self, db_name):
        db_node_version = self.get_latest_node_version(db_name)
        tables = []
        for tag, val in db_node_version["tags"].items():
            if string.find(tag, "table_") != -1:
                tables.append(val["value"])
        return tables

    def create_table(self, db_name, table_name, columns):
        """
        Create a new table called *table_name* with columns specified
        by *columns*
        """
        # Create Node for new table
        path = self.hostname + "/nodes/{}".format(table_name)
        table_node = requests.post(path).json()

        # Next, create a NodeVersion with a tag for number of columns
        tag_map = {}
        tag_map["num_columns"] = {
            "key": "num_columns",
            "value": len(columns),
            "type": "integer"
            }
        table_node_version = self.create_node_version(table_node["id"], tag_map=tag_map)
        for col_name, col_type in columns.items():
            col_node = requests.post(self.hostname + "/nodes/{}".format(col_name)).json()

            # create NodeVersion with tags for type
            tag_map = {}
            tag_map[col_name] = {
                "key": col_name,
                "value": col_type,
                "type": col_type
            }
            col_node_version = self.create_node_version(col_node["id"], tag_map=tag_map)

            # create Edge, then create EdgeVersion
            edge_path = self.hostname + "/edges/{0}-to-{1}".format(table_name, col_name)
            edge = requests.post(edge_path).json()
            result = self.create_edge_version(edge["id"], \
                                table_node["id"], col_node["id"])

        # Update database metadata to contain this table
        db_node_version = self.get_latest_node_version(db_name)
        node_id = db_node_version["nodeId"]
        parent_ids = db_node_version["id"]
        node_tags = db_node_version["tags"]
        node_tags["table_{}".format(table_name)] = {
            "key": "table_name",
            "value": table_name,
            "type": "string"
        }
        db_updated_node_version = self.create_node_version(node_id, \
                                        tag_map=node_tags, parents=parent_ids)
        edge_path = self.hostname + "/edges/{0}-to-{1}".format(db_name, table_name)
        edge = requests.post(edge_path).json()
        edge_id = edge["id"]
        fromId = db_updated_node_version["id"]
        toId = table_node["id"]
        self.create_edge_version(edge_id, fromId, toId)
        return table_node_version

    def get_table(self, table_name):
        """
        Retrieves the table with the name *table_name*
        """
        table_info = [table_name]
        table_node_version = self.get_latest_node_version(table_name)
        table_info.append(self.get_node_metadata(table_node_version))
        node_id = table_node_version["nodeId"]
        edge_regex = table_name + "-to-[a-zA-Z0-9_]*"
        adjacent_path = self.hostname + "/nodes/adjacent/{0}/{1}".format(node_id, edge_regex)
        columns = requests.get(adjacent_path).json()
        print(columns)
        for col_name in columns:
            col = self.get_latest_node_version(col_name)
            table_info.append(self.get_node_metadata(col))
        return table_info

    def drop_table(self, table_name):
        """
        Drops the databse called *db_name* by creating a new NodeVersion
        with a "dropped" Tag.
        """
        table_node_version = self.get_latest_node_version(table_name)
        node_id = table_node_version["nodeId"]
        parent_ids = table_node_version["id"]
        tag_map = {}
        tag_map["dropped"] = {
            "key": "dropped",
            "value": "dropped",
            "type": "string"
        }
        table_updated_node_version = self.create_node_version(node_id, \
                                        tag_map=tag_map, parents=parent_ids)

    def create_edge_version(self, edge_id, fromId, toId):
        edge_version_path = self.hostname + "/edges/versions"
        edge_version = {
            "tags": {},
            "referenceParameters": {},
            "edgeId": edge_id,
            "fromId": fromId,
            "toId": toId
        }
        return requests.post(edge_version_path, json=edge_version).json()

    def create_node_version(self, node_id, tag_map={}, parents=None):
        """
        Helper method to create a new node version with tags spedified by
        *tag_map* for the Node with *node_id*
        """
        if parents:
            version_path = self.hostname + "/nodes/versions?parents={}".format(parents)
        else:
            version_path = self.hostname + "/nodes/versions"
        node_version = {
            "tags": tag_map,
            "referenceParameters": {},
            "nodeId": node_id
        }
        result = requests.post(version_path, json=node_version)
        return result.json()

    def get_latest_node_version(self, node_name):
        """
        Helper method to retrieve the latest NodeVersion of *node_name*
        """
        node_path = self.hostname + "/nodes/{}/latest".format(node_name)
        node_version_id = requests.get(node_path).json()
        get_node_path = self.hostname + "/nodes/versions/{}".format(node_version_id[0])
        node_version = requests.get(get_node_path)
        return node_version.json()

    def get_node_metadata(self, node_version):
        """
        Returns a dictionary representing the metadata of the *node_version*
        """
        metadata = {}
        for name, tag in node_version["tags"].items():
            metadata[name] = tag["value"]
        return metadata
