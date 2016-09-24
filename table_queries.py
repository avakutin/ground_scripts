import requests
import json

class TableQueries:

    def __init__(self, db_name, hostname):
        self.db_name = db_name
        self.hostname = hostname

    def create_table(self, table_name, columns):
        # Create Node for new table
        path = self.hostname + "/nodes/{}".format(table_name)
        table_node = requests.post(path)

        # Next, create a NodeVersion with a tag for number of columns
        version_path = self.hostname + "/nodes/versions?parents=[]"
        tag_map = {}
        tag_map["num_columns"] = {
            "versionId": "",
            "key": "num_columns",
            "value": len(columns),
            "type": "integer"
            }
        node_version = {
            "id": "",
            "tags": tag_map,
            "structureVersionId": "",
            "reference": "",
            "parameters": "",
            "nodeId": table_node.json()["id"]
        }
        table_node_version = requests.post(version_path, data=table_node_version)
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
            node_version = {
                "id": "",
                "tags": tag_map,
                "structureVersionId": "",
                "reference": "",
                "parameters": "",
                "nodeId": col_node.json()["id"]
            }
            col_node_version = requests.post(version_path, data=node_version)

            # create Edge, then create EdgeVersion
            edge_path = self.hostname + "/edges/{}".format(str(table_name) + "-to-" + str(col_name))
            edge = requests.post(edge_path)
            edge_version_path = self.hostname + "/edges/versions"
            edge_version = {
                "id": "",
                "tags": "",
                "structureVersionId": "",
                "reference": "",
                "parameters": "",
                "edgeId": edge.json()["id"],
                "fromId": table_node.json()["id"],
                "toId": col_node.json()["id"]
            }
            edge_version = requests.post(edge_version_path, data=edge_version)

    def get_table(self, table_name):
        table_info = [table_name]
        table_node_version = self.get_latest_node_version(table_name)
        table_info.append(self.get_table_metadata(table_node_version))
        node_id = table_node_version.json()["nodeId"]
        edge_regex = table_name + "-to-*"
        adjacent_path = self.hostname + "/nodes/adjacent/{1}/{2}".format(node_id, edge_regex)
        columns = requests.get(adjacent_path)
        for col_name in columns:
            col = self.get_latest_node_version(col_name)
            table_info.append(self.get_node_metadata(col))

    def get_latest_node_version(self, node_name):
        node_path = self.host + "/nodes/{}/latest".format(node_name)
        node_version = requests.get(node_path)
        return node_version

    def get_node_metadata(self, node_version):
        metadata = {}
        for name, tag in node_version.json()["tags"].items():
            metadata[name] = tag["value"]
        return metadata
