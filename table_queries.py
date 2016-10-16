import requests
import json

class TableQueries:

    def __init__(self, hostname):
        self.hostname = hostname

    def create_database(self, db_name):
        # Create a new Node for the database
        path = self.hostname + "/nodes/{}".format(db_name)
        db_node = requests.post(path)

        # Create new NodeVersion
        db_node_version = self.create_node_version(db_node.json()["id"])
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
        node_id = db_node_version.json()["nodeId"]
        parent_ids = db_node_version.json()["id"]
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
        for tag, val in db_node_version.json()["tags"]:
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
        table_node = requests.post(path)

        # Next, create a NodeVersion with a tag for number of columns
        tag_map = {}
        tag_map["num_columns"] = {
            "versionId": "",
            "key": "num_columns",
            "value": len(columns),
            "type": "integer"
            }
        table_node_version = self.create_node_version(table_node.json()["id"], tag_map=tag_map)
        for col_name, col_type in columns.items():
            col_node = requests.post(self.hostname + "/nodes/{}".format(col_name))

            # create NodeVersion with tags for type
            tag_map = {}
            tag_map[col_name] = {
                "versionId": "",
                "key": col_name,
                "value": col_type,
                "type": col_type
            }
            col_node_version = self.create_node_version(col_node.json()["id"], tag_map=tag_map)

            # create Edge, then create EdgeVersion
            edge_path = self.hostname + "/edges/{0}-to-{1}".format(table_name, col_name)
            edge = requests.post(edge_path)
            edge_version = self.create_edge_version(edge.json()["id"], \
                                table_node.json()["id"], col_node.json()["id"])

        # Update database metadata to contain this table
        db_node_version = self.get_latest_node_version(db_name)
        node_id = db_node_version.json()["nodeId"]
        parent_ids = db_node_version.json()["id"]
        node_tags = db_node_version.json()["tags"]
        node_tags["table_{}".format(table_name)] = {
            "versionId": "",
            "key": "table_name",
            "value": table_name,
            "type": "string"
        }
        db_updated_node_version = self.create_node_version(node_id, \
                                        tag_map=node_tags, parents=list(parent_ids))
        edge_path = self.hostname + "/edges/{0}-to-{1}".format(db_name, table_name)
        edge = requests.post(edge_path)
        edge_id = edge.json()["id"]
        fromId = db_updated_node_version.json()["id"]
        toId = table_node.json()["id"]
        self.create_edge_version(edge_id, fromId, toId)
        return table_node_version

    def get_table(self, table_name):
        """
        Retrieves the table with the name *table_name*
        """
        table_info = [table_name]
        table_node_version = self.get_latest_node_version(table_name)
        table_info.append(self.get_table_metadata(table_node_version))
        node_id = table_node_version.json()["nodeId"]
        edge_regex = table_name + "-to-*"
        adjacent_path = self.hostname + "/nodes/adjacent/{0}/{1}".format(node_id, edge_regex)
        columns = requests.get(adjacent_path)
        for col_name in columns:
            col = self.get_latest_node_version(col_name)
            table_info.append(self.get_node_metadata(col))

    def drop_table(self, table_name):
        """
        Drops the databse called *db_name* by creating a new NodeVersion
        with a "dropped" Tag.
        """
        table_node_version = self.get_latest_node_version(table_name)
        node_id = table_node_version.json()["nodeId"]
        parent_ids = table_node_version.json()["id"]
        tag_map = {}
        tag_map["dropped"] = {
            "versionId": "",
            "key": "dropped",
            "value": "dropped",
            "type": "string"
        }
        table_updated_node_version = self.create_node_version(node_id, \
                                        tag_map=tag_map, parents=list(parent_ids))

    def create_edge_version(self, edge_id, fromId, toId):
        edge_version_path = self.hostname + "/edges/versions"
        edge_version = {
            "id": "",
            "tags": "",
            "structureVersionId": "",
            "reference": "",
            "referenceParameters": "",
            "edgeId": edge_id,
            "fromId": fromId,
            "toId": toId
        }
        return requests.post(edge_version_path, json=edge_version)

    def create_node_version(self, node_id, tag_map={}, parents=[]):
        """
        Helper method to create a new node version with tags spedified by
        *tag_map* for the Node with *node_id*
        """
        version_path = self.hostname + "/nodes/versions?parents={}".format(parents)
        node_version = {
            "id": "",
            "tags": tag_map,
            "structureVersionId": "",
            "reference": "",
            "referenceParameters": "",
            "nodeId": node_id
        }
        result = requests.post(version_path, json=node_version)
        return result

    def get_latest_node_version(self, node_name):
        """
        Helper method to retrieve the latest NodeVersion of *node_name*
        """
        node_path = self.hostname + "/nodes/{}/latest".format(node_name)
        node_version = requests.get(node_path)
        return node_version

    def get_node_metadata(self, node_version):
        """
        Returns a dictionary representing the metadata of the *node_version*
        """
        metadata = {}
        for name, tag in node_version.json()["tags"].items():
            metadata[name] = tag["value"]
        return metadata
