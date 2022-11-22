# IMPORTING PYTHON PACKAGES
import requests as req
import json


# GLOBAL VARIABLES
firebase_url = "https://dsci-551-project-8f06f-default-rtdb.firebaseio.com/"

# FUNCTION TO UPDATE THE DIRECTORY STRUCTURE
def update_dir_structure(counter, dir, trigger):
    if trigger:
        response_object = req.patch(url=firebase_url + "/dir_str/inodes.json", data=json.dumps(dir))
    else:
        response_object = req.patch(url=firebase_url + "/dir_str/.json", data=json.dumps(dir))
    if response_object.status_code != 200:
        print("Failed to update directories! => ", response_object.text)
        return -1
    else:
        return counter

# FUNCTION TO READ THE DIRECTORY STRUCTURE
def read_dir_structure(counter):
    response_object = req.get(url=firebase_url + "/dir_str/.json")
    if response_object.status_code != 200:
        print("Failed to read directories! => ", response_object.text)
        return -1, None
    else:
        return counter + 1, response_object.json()

# FUNCTION TO UPDATE THE CONFIG FILE
def update_config_structure(counter, conf):
    response_object = req.patch(url=firebase_url + "/config/.json", data=json.dumps(conf))
    if response_object.status_code != 200:
        print("Failed to update config! => ", response_object.text)
        return -1
    else:
        return counter + 1

# FUNCTION TO READ THE CONFIG FILE
def read_config_structure(counter):
    response_object = req.get(url=firebase_url + "/config/.json")
    if response_object.status_code != 200:
        print("Failed to read config! => ", response_object.text)
        return -1, None
    else:
        return counter + 1, response_object.json()

# FUNCTION TO UPDATE THE FILE->CLUSTER MAPPING
def update_cluster_mapping_structure(counter, mapping):
    response_object = req.patch(url=firebase_url + "/file_cluster_map/.json", data=json.dumps(mapping))
    if response_object.status_code != 200:
        print("Failed to update cluster-file mapping! => ", response_object.text)
        return -1
    else:
        return counter + 1

def read_cluster_mapping_structure(counter):
    response_object = req.get(url=firebase_url + "/file_cluster_map/.json")
    if response_object.status_code != 200:
        print("Failed to read cluster-file mapping! => ", response_object.text)
        return -1, None
    else:
        return counter + 1, response_object.json()
