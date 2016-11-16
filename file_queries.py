import requests
import json
import string
import utils

class FileQueries:

    def __init__(self, hostname):
        self.hostname = hostname

    def create_directory(self, path):
        # Create Node for directory
        path_list = self.split_path(path)

        req_path = self.hostname + "/nodes/{}".format(path_list[-1])
        dir_node = requests.post(req_path).json()

        # Create NodeVersion for the directory
        dir_node_version = utils.create_node_version(self.hostname, dir_node["id"])
        if len(path_list) > 1:
            # Create edge between parent directory NodeVersion and new NodeVersion
            self.create_edge_to_parent_dir(path_list, dir_node_version["id"])


    def create_file(self, filepath, metadata):
        """
        Creates a Node and NodeVersion for a file located at *filepath*
        containing *metadata*
        """
        path_list = self.split_path(filepath)
        filename = path_list[-1]

        # Create Node for file
        path = self.hostname + "/nodes/{}".format(filename)
        file_node = requests.post(path).json()

        # Create Tags for each item of metadata
        tag_map = {}
        tag_map["file"] = {
            "key": "file",
            "value": "file",
            "type": "string"
        }
        for label, value in metadata.items():
            tag_map[label] = {
                "key": label,
                "value": value,
                "type": "string"
            }
        file_node_version = utils.create_node_version(self.hostname, file_node["id"], tag_map=tag_map)
        # utils.create_nodes_for_tags(self.hostname, filename, metadata, file_node_version["id"])

        # Create edge between the file and its parent directory
        self.create_edge_to_parent_dir(path_list, file_node_version["id"])
        return file_node_version

    def get_file(self, filepath):
        path_list = self.split_path(filepath)
        filename = path_list[-1]
        file_info = [filename]
        file_node_version = utils.get_latest_node_version(self.hostname, filename)
        file_info.append(utils.get_node_version_metadata(file_node_version))
        return file_info

    def split_path(self, path):
        """
        Splits the input path into a list, adding "root" to the list
        """
        path_split = string.split(path, "/")
        path_list = ["root"]
        path_list += [name for name in path_split if name != '']
        return path_list

    def create_edge_to_parent_dir(self, path_list, new_node_version_id):
        """
        Creates an Edge and EdgeVersion between a new NodeVersion with id
        *new_node_version_id* and its parent directory, as specified by *path_list*
        """
        parent_dir_node_version = utils.get_latest_node_version(self.hostname, path_list[-2])
        edge_path = self.hostname + "/edges/{0}-to-{1}".format(path_list[-2], path_list[-1])
        edge = requests.post(edge_path).json()
        edge_id = edge["id"]
        fromId = parent_dir_node_version["id"]
        toId = new_node_version_id
        utils.create_edge_version(self.hostname, edge_id, fromId, toId)
