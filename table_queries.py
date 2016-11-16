import requests
import json
import utils

class TableQueries:

    def __init__(self, hostname):
        self.hostname = hostname

    def create_database(self, db_name):
        # Create a new Node for the database
        path = self.hostname + "/nodes/{}".format(db_name)
        db_node = requests.post(path).json()

        # Create new NodeVersion
        db_node_version = utils.create_node_version(self.hostname, db_node["id"])
        return db_node_version

    def get_database(self, db_name):
        """
        Retrieves the inforamtion about the database called *db_name*
        """
        db_node_version = utils.get_latest_node_version(self.hostname, db_name)
        if self.check_if_dropped(db_node_version):
            print "Database {} has been dropped".format(db_name)
            return
        db_info = [db_name]
        db_info.append(utils.get_node_version_metadata(db_node_version))
        return db_info

    def drop_database(self, db_name):
        """
        Drops the databse called *db_name* by creating a new NodeVersion
        with a "dropped" Tag.
        """
        db_node_version = utils.get_latest_node_version(self.hostname, db_name)
        node_id = db_node_version["nodeId"]
        parent_id = db_node_version["id"]
        tag_map = {}
        tag_map["dropped"] = {
            "versionId": "",
            "key": "dropped",
            "value": "dropped",
            "type": "string"
        }
        db_updated_node_version = utils.create_node_version(self.hostname, node_id, \
                                        tag_map=tag_map, parents=parent_id)

    def get_all_tables(self, db_name):
        db_node_version = utils.get_latest_node_version(self.hostname, db_name)
        node_id = db_node_version["id"]
        edge_regex = db_name + "-to-"
        adjacent_path = self.hostname + "/nodes/adjacent/{0}/{1}".format(node_id, edge_regex)
        table_ids = requests.get(adjacent_path).json()
        tables = []
        for tid in table_ids:
            version_path = self.hostname + "/nodes/versions/{}".format(tid)
            table_node_version = requests.get(version_path).json()
            latest_version = utils.get_latest_node_version(self.hostname, table_node_version["nodeId"][6:])
            if not self.check_if_dropped(latest_version):
                name = latest_version["nodeId"][6:]
                tables.append(name)
        return tables

    def create_table(self, db_name, table_name, columns):
        """
        Create a new table called *table_name* with columns specified
        by *columns*
        """
        # Check that the database exists
        db_node_version = utils.get_latest_node_version(self.hostname, db_name)
        if self.check_if_dropped(db_node_version):
            print "Database {} has been dropped".format(db_name)
            return

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
        table_node_version = utils.create_node_version(self.hostname, table_node["id"], tag_map=tag_map)

        utils.create_nodes_for_tags(self.hostname, table_name, columns, table_node_version["id"])

        # Create edge between database and this table
        node_id = db_node_version["nodeId"]
        parent_ids = db_node_version["id"]
        db_updated_node_version = utils.create_node_version(self.hostname, node_id, parents=parent_ids)
        edge_path = self.hostname + "/edges/{0}-to-{1}".format(db_name, table_name)
        edge = requests.post(edge_path).json()
        edge_id = edge["id"]
        fromId = db_updated_node_version["id"]
        toId = table_node_version["id"]
        utils.create_edge_version(self.hostname, edge_id, fromId, toId)

        # Make edge from new db NodeVersion to any previous Tables
        edge_regex = db_name + "-to-"
        adjacent_path = self.hostname + "/nodes/adjacent/{0}/{1}".format(db_node_version["id"], edge_regex)
        table_ids = requests.get(adjacent_path).json()
        for table_id in table_ids:
            table_node = requests.get(self.hostname + "/nodes/versions/{}".format(table_id)).json()
            table_name = table_node["nodeId"][6:]
            edge_path = self.hostname + "/edges/{0}-to-{1}".format(db_name, table_name)
            edge = requests.post(edge_path).json()
            edge_id = edge["id"]
            fromId = db_updated_node_version["id"]
            utils.create_edge_version(self.hostname, edge_id, fromId, table_id)
        return table_node_version

    def get_table(self, table_name):
        """
        Retrieves the table with the name *table_name*
        """
        table_info = [table_name]
        table_node_version = utils.get_latest_node_version(self.hostname, table_name)
        if self.check_if_dropped(table_node_version):
            print "Table {} has been dropped".format(table_name)
            return
        table_info.append(utils.get_node_version_metadata(table_node_version))
        node_id = table_node_version["id"]
        edge_regex = table_name + "-to-"
        adjacent_path = self.hostname + "/nodes/adjacent/{0}/{1}".format(node_id, edge_regex)
        column_ids = requests.get(adjacent_path).json()
        for col_id in column_ids:
            version_path = self.hostname + "/nodes/versions/{}".format(col_id)
            col_node_version = requests.get(version_path).json()
            table_info.append(utils.get_node_version_metadata(col_node_version))
        return table_info

    def drop_table(self, table_name):
        """
        Drops the table called *table_name* by creating a new NodeVersion with a
        "dropped" Tag.
        """
        table_node_version = utils.get_latest_node_version(self.hostname, table_name)
        node_id = table_node_version["nodeId"]
        parent_ids = table_node_version["id"]
        tag_map = {}
        tag_map["dropped"] = {
            "key": "dropped",
            "value": "dropped",
            "type": "string"
        }
        table_updated_node_version = utils.create_node_version(self.hostname, node_id, \
                                        tag_map=tag_map, parents=parent_ids)

    def update_table(self, table_name, schema):
        """
        Update *table_name* to have the given *schema*
        """
        tag_map = {}
        tag_map["num_columns"] = {
            "key": "num_columns",
            "value": len(schema),
            "type": "integer"
            }
        latest_node_version = utils.get_latest_node_version(self.hostname, table_name)
        node_id = latest_node_version["nodeId"]
        parent_id = latest_node_version["id"]
        table_node_version = utils.create_node_version(self.hostname, node_id, tag_map=tag_map, \
                                        parents=parent_id)
        utils.create_nodes_for_tags(self.hostname, table_name, schema, table_node_version["id"])


    def check_if_dropped(self, node_version):
        """
        Checks whether the table or databse that this *node_version*
        refers to has been dropped.
        """
        if node_version["tags"].get("dropped"):
            return True
        return False
