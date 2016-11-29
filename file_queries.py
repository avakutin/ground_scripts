import requests
import json
import string
import utils

class FileQueries:

    def __init__(self, hostname):
        self.hostname = hostname

    def ls(self, path):
        # path_list = self.split_path(path)
        node_name = path.replace("/", ">")
        i = string.rfind(node_name, ">")
        if i == 0:
            parent = None
            dirname = node_name
        else:
            parent = node_name[:i]
            dirname = node_name[i+1:]

        # Check that supplied path refers to a directory
        if self.is_file(node_name, parent=False):
            print "Path '{}' is not a directory".format(path)
            return

        dir_node_version = utils.get_latest_node_version(self.hostname, node_name)
        adj_path = self.hostname + "/nodes/adjacent/{0}/{1}-to-".format(dir_node_version["id"], \
                                    node_name)
        adj_list = requests.get(adj_path).json()
        output = [path]
        for node_id in adj_list:
            node = requests.get(self.hostname + "/nodes/versions/{}".format(node_id)).json()
            name = node["nodeId"][6:]
            node_version = utils.get_latest_node_version(self.hostname, name)
            metadata = utils.get_node_version_metadata(node_version)
            name = name.replace(">", "/")
            if metadata.get("file") != None:
                output.append((name, "File"))
            else:
                output.append((name, "Directory"))
        return output

    def create_directory(self, path):
        # Create Node for directory
        node_name = path.replace("/", ">")
        i = string.rfind(node_name, ">")
        if i == 0:
            if len(path) < 2:
                parent = None
            else:
                parent = node_name[0]
            dirname = node_name
        else:
            parent = node_name[:i]
            dirname = node_name[i+1:]

        # Check that supplied parent is a directory
        if parent and self.is_file(node_name):
            print "Parent '{}' is not a directory".format(parent.replace(">", "/"))
            return

        req_path = self.hostname + "/nodes/{}".format(node_name)
        dir_node = requests.post(req_path).json()

        # Create NodeVersion for the directory
        dir_node_version = utils.create_node_version(self.hostname, dir_node["id"])
        if parent:
            # Create edge between parent directory NodeVersion and new NodeVersion
            self.create_edge_to_parent_dir(parent, node_name, dir_node_version["id"])


    def create_file(self, filepath, metadata):
        """
        Creates a Node and NodeVersion for a file located at *filepath*
        containing *metadata*
        """
        node_name = filepath.replace("/", ">")
        i = string.rfind(node_name, ">")
        if i == 0:
            parent = node_name[0]
        else:
            parent = node_name[:i]
        filename = node_name[i+1:]

        # Check that supplied parent is a directory
        if self.is_file(node_name):
            print "Parent '{}' is not a directory".format(parent.replace(">", "/"))
            return

        # Create Node for file
        path = self.hostname + "/nodes/{}".format(node_name)
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

        # Create edge between the file and its parent directory
        self.create_edge_to_parent_dir(parent, node_name, file_node_version["id"])
        return file_node_version

    def get_file(self, filepath):
        node_name = filepath.replace("/", ">")
        i = string.rfind(node_name, ">")
        if i == 0:
            parent = node_name[0]
        else:
            parent = node_name[:i]
        filename = node_name[i+1:]

        if not self.is_file(node_name, parent=False):
            print "{} is not a file".format(filepath)
            return

        file_info = [node_name.replace(">", "/")]
        file_node_version = utils.get_latest_node_version(self.hostname, node_name)
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

    def create_edge_to_parent_dir(self, parent, new_node_name, new_node_version_id):
        """
        Creates an Edge and EdgeVersion between a new NodeVersion with id
        *new_node_version_id* and name *new_node_name* and its parent directory,
        as specified by *parent*
        """
        parent_dir_node_version = utils.get_latest_node_version(self.hostname, parent)
        edge_path = self.hostname + "/edges/{0}-to-{1}".format(parent, new_node_name)
        edge = requests.post(edge_path).json()
        edge_id = edge["id"]
        fromId = parent_dir_node_version["id"]
        toId = new_node_version_id
        utils.create_edge_version(self.hostname, edge_id, fromId, toId)

    def is_file(self, path, parent=True):
        """
        If parent is true, returns whether the parent of the supplied
        directory/file is a directory. If parent is false, returns whether
        the entire path refers to a directory
        """
        i = string.rfind(path, ">")

        if parent:
            if i == 0:
                name = path[0]
            else:
                name = path[:i]
        else:
            name = path

        # If root directory
        if i == 0 and len(path) < 2:
            return False

        parent_node_version = utils.get_latest_node_version(self.hostname, name)
        metadata = utils.get_node_version_metadata(parent_node_version)
        return metadata.get("file") != None
