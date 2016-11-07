import requests
import json
import string
import re
from table_queries import TableQueries

class FileQueries:

    def __init__(self, hostname):
        self.hostname = hostname
        self.helper = TableQueries(hostname)

    def create_file(self, filename, metadata):
        """
        Creates a Node and NodeVersion for a file called *filename*
        containing *metadata*
        """
        # Create Node for file
        path = self.hostname + "/nodes/{}".format(filename)
        file_node = requests.post(path).json()

        # Create Tags for each item of metadata
        tag_map = {}
        for label, value in metadata.items():
            tag_map[label] = {
                "key": label,
                "value": value,
                "type": "string"
            }
        file_node_version = self.helper.create_node_version(file_node["id"], tag_map=tag_map)
        self.helper.create_columns(filename, metadata, file_node_version["id"])
        return file_node_version

    def get_file(self, filename):
        file_info = [filename]
        file_node_version = self.helper.get_latest_node_version(filename)
        file_info.append(self.helper.get_node_version_metadata(file_node_version))
        return file_info
