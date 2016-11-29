import requests
import json

def create_nodes_for_tags(hostname, node_name, tags, node_version_id):
    """
    Creates Nodes and NodeVersions for the tags specified by *tags*
    and attaches them to the node with name *node_name* whose latest NodeVersion id
    is *node_version_id*
    """
    for tag_name, tag_type in tags.items():
        tag_node = requests.post(hostname + "/nodes/{}".format(tag_name)).json()

        # create NodeVersion with tags for type
        tag_map = {}
        tag_map[tag_name] = {
            "key": tag_name,
            "value": tag_type,
            "type": "string"
        }
        tag_node_version = create_node_version(hostname, tag_node["id"], tag_map=tag_map)
        # create Edge, then create EdgeVersion
        edge_path = hostname + "/edges/{0}-to-{1}".format(node_name, tag_name)
        edge = requests.post(edge_path).json()
        result = create_edge_version(hostname, edge["id"], node_version_id, tag_node_version["id"])

def create_edge_version(hostname, edge_id, fromId, toId):
    """
    Creates a new EdgeVersion for the Edge with *edge_id*. The EdgeVersion
    is from NodeVersion *fromId* to NodeVersion *toId*.

    Returns the new EdgeVersion
    """
    edge_version_path = hostname + "/edges/versions"
    edge_version = {
        "tags": {},
        "referenceParameters": {},
        "edgeId": edge_id,
        "fromId": fromId,
        "toId": toId
    }
    return requests.post(edge_version_path, json=edge_version).json()

def create_node_version(hostname, node_id, tag_map={}, parents=None):
    """
    Creates a new node version with tags spedified by *tag_map* for the
    Node with *node_id*. If *parents* is provided, creates edges between
    the new node version and its parents.

    Returns the new NodeVersion
    """
    if parents:
        version_path = hostname + "/nodes/versions?parents={}".format(parents)
    else:
        version_path = hostname + "/nodes/versions"
    node_version = {
        "tags": tag_map,
        "referenceParameters": {},
        "nodeId": node_id
    }
    result = requests.post(version_path, json=node_version)
    return result.json()

def get_latest_node_version(hostname, node_name):
    """
    Returns the latest NodeVersion of the node with name *node_name*
    """
    node_path = hostname + "/nodes/{}/latest".format(node_name)
    node_version_id = requests.get(node_path).json()
    get_node_path = hostname + "/nodes/versions/{}".format(node_version_id[0])
    node_version = requests.get(get_node_path)
    return node_version.json()

def get_node_version_metadata(node_version):
    """
    Returns a dictionary representing the metadata (Tags) of the *node_version*
    """
    metadata = {}
    for name, tag in node_version["tags"].items():
        metadata[name] = tag["value"]
    return metadata
