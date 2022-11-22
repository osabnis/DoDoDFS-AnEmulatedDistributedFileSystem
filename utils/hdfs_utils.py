# IMPORTING PYTHON PACKAGES
from termcolor import colored
from tabulate import tabulate
import json
import ast
from datetime import datetime, timezone
import pandas as pd
import random
import time
import os

# IMPORTING LOCAL PYTHON LIBRARIES
from utils.firebase_utils import read_config_structure, update_config_structure
from utils.firebase_utils import read_dir_structure, update_dir_structure
from utils.firebase_utils import read_cluster_mapping_structure, update_cluster_mapping_structure
from utils.util_utils import octal_to_readable, file_parent_exist


# GLOBAL VARIABLES
menu_color = "green"
divider_color = "blue"
header_color = "yellow"
error_color = "red"

operations = [[0, "Help", "dodfs help()"],
              [1, "Print DODO DFS configurations", "dodfs displayconfig()"],
              [2, 'Update DODO DFS configurations', 'dodfs updateconfig -u {"key":*new value*}'],
              [3, "Make a new directory", "dodfs mkdir -p *path/to/folder*"],
              [4, "List the contents of the directory", "dodfs ls -p *path/to/folder*"],
              [5, "Display the contents of the file", "dodfs cat -p *path/to/file* -ar/arc/ac (all rows/all rows and columns/all columns)"],
              [6, "Delete the file from the DFS", "dodfs rm -p *path/to/file*"],
              [7, "Put a file into the DFS", "dodfs put -p *path/to/store* -f *path/to/local/file* -k *partitions*"],
              [8, "Locations of the partitions of the file", "dodfs getPartitionLocations -p *path/to/file*"],
              [9, "Read the data from the partition", "dodfs readPartition -p *path/to/file* -k *partition number*"],
              [10, "Query a Dataset of your choice", "dodfs query-mode()"],
              [11, "Special Query Operations on the Finance Dataset", 'dodfs spec-ops() -d "finance"'],
              [12, "Close the DFS", "dodfs quit()"]]


# FUNCTIONS TO INFORM THE USER ABOUT THE FUNCTIONALITIES
def help(counter):
    print()
    print(colored("DODO DFS can perform the following operations:", menu_color))
    print(colored(tabulate(operations, headers=["ID", "Description", "Command"]), menu_color))
    print()
    return counter + 1

# FUNCTION TO HELP THE USER QUIT THE HDFS
def quit():
    print(colored("Thank you for using DODO DFS!", menu_color))
    return -1

# FUNCTION TO HELP THE USER SEE THE HDFS CONFIG
def displayconfig(counter):
    count, config = read_config_structure(counter)
    print()
    print(colored("Config: ", menu_color))
    print(json.dumps(config, indent=4))
    print()
    return count

# FUNCTION TO HELP THE USER UPDATE THE HDFS CONFIG
def updateconfig(new_user_input, counter):
    if "-u" != new_user_input[2]:
        print(colored("Invalid argument! Please enter a valid argument!", error_color))
        print()
        return counter + 1, ""
    else:
        update_dict = ast.literal_eval(new_user_input[3])
        if type(update_dict) != dict:
            print(colored("Invalid Dictionary! Please enter a valid dictionary!", error_color))
            print()
            return counter + 1, ""
        else:
            count, config = read_config_structure(counter)
            if list(update_dict.keys())[0] not in config:
                print(colored("Invalid Key! Please enter a valid key for config!", error_color))
                print()
                return counter + 1, ""
            else:
                count = update_config_structure(counter, update_dict)
                print(colored("Key has been updated!", menu_color))
                print()
                return count, ""


# FUNCTION FOR THE 'LS' FUNCTION
def ls(new_user_input, counter):
    if "-p" != new_user_input[2]:
        print(colored("Invalid argument! Please enter a valid argument!", error_color))
        print()
        return counter + 1, ""
    else:
        user_path = ast.literal_eval(new_user_input[3])
        if type(user_path) != str:
            print(colored("Invalid Path! Please enter a valid path!", error_color))
            print()
            return counter + 1, ""
        if "." in user_path:
            print(
                colored("Invalid Directory Path! Please enter a valid path to a directory - not a file!", error_color))
            print()
            return counter + 1, ""
        if user_path[-1] == "/" and len(user_path) != 1:
            user_path = user_path[:len(user_path) - 1]
        children = []
        total_size = 0
        count, dirstruct = read_dir_structure(counter)
        parent_user_path = "/" if user_path.count("/") == 1 else user_path[:user_path.rfind("/")]
        exists, parent_exists, _ = file_parent_exist(dirstruct, user_path, parent_user_path)
        if not exists:
            print(colored("Directory does not exist!", error_color))
            print()
            return counter + 1, ""
        for inode in dirstruct["inodes"]:
            if inode["deleted"]:
                continue
            if inode["parent_name"] == user_path:
                children.append(
                    [datetime.fromtimestamp(inode["last_acmtime"] / 1000, timezone.utc).strftime("%m/%d/%y %H:%M"),
                     inode["node_type"],
                     inode['filesize'] if inode['filesize'] != 0 else "-",
                     inode['name'],
                     octal_to_readable(inode['permission'], inode["node_type"])])
                total_size += inode['filesize'] if inode['filesize'] != 0 else 0
        print()
        print(colored(tabulate(children, headers=["ACM-Time", "Type", "Size", "Name", "Permission"]), menu_color))
        print(colored("              {} file(s)      {} bytes".format(len(children), total_size), menu_color))
        print()
        return count, ""

# FUNCTION FOR MKDIR FUNCTION
def mkdir(new_user_input, counter):
    if "-p" != new_user_input[2]:
        print(colored("Invalid argument! Please enter a valid argument!", error_color))
        print()
        return counter + 1, ""
    else:
        user_path = ast.literal_eval(new_user_input[3])
        if type(user_path) != str:
            print(colored("Invalid Path! Please enter a valid path!", error_color))
            print()
            return counter + 1, ""
        elif "." in user_path:
            print(colored("Invalid directory name! Fullstops are reserved for files!", error_color))
            print()
            return counter + 1, ""
        else:
            if user_path[-1] == "/" and len(user_path) != 1:
                user_path = user_path[:len(user_path) - 1]
            count, dirstruct = read_dir_structure(counter)
            parent_user_path = "/" if user_path.count("/") == 1 else user_path[:user_path.rfind("/")]
            exists, parent_exists, _ = file_parent_exist(dirstruct, user_path, parent_user_path)
            if not parent_exists:
                print(colored("Invalid Parent Path! Please enter a valid path!", error_color))
                print()
                return counter + 1, ""
            if exists:
                print(colored("Directory already exists!", error_color))
                print()
                return counter + 1, ""
            if parent_exists and not exists:
                new_file = {dirstruct['lastNodeId'] + 1: dict(filesize=0, id=dirstruct['lastNodeId'] + 1,
                                                              last_acmtime=round((datetime.utcnow() - datetime(
                                                                  1970, 1, 1)).total_seconds() * 1000),
                                                              name=user_path[user_path.rfind("/") + 1:],
                                                              node_type="<DIR>", parent_name=parent_user_path,
                                                              permission=755, deleted=False)}
                count = update_dir_structure(counter, dir=new_file, trigger=True)
                count = update_dir_structure(count, dir={"lastNodeId": dirstruct["lastNodeId"] + 1}, trigger=False)
                count = update_dir_structure(count, dir={"numNodes": dirstruct["numNodes"] + 1}, trigger=False)
                print(colored("Directory has been created!", menu_color))
                print()
                return count + 1, ""

# FUNCTION TO LOAD A FILE INTO THE HDFS
def put(new_user_input, counter):
    if "-p" != new_user_input[2] or "-f" != new_user_input[4] or "-k" != new_user_input[6]:
        print(colored("Invalid arguments! Please enter valid arguments!", error_color))
        print()
        return counter + 1, ""
    else:
        user_path = ast.literal_eval(new_user_input[3])
        file_path = ast.literal_eval(new_user_input[5])
        filename = file_path.split("/")[-1]
        partitions = ast.literal_eval(new_user_input[7])
        if type(user_path) != str:
            print(colored("Invalid Path! Please enter a valid path!", error_color))
            print()
            return counter + 1, ""
        if type(file_path) != str:
            print(colored("Invalid Path! Please enter a valid path!", error_color))
            print()
            return counter + 1, ""
        if "." not in file_path:
            print(colored("Invalid Filename! Please enter a filename with an extension!", error_color))
            print()
            return counter + 1, ""
        if type(partitions) != int:
            print(colored("Invalid Value for Partitions! Please enter a valid value!", error_color))
            print()
            return counter + 1, ""
        if user_path[-1] == "/" and len(user_path) != 1:
            user_path = user_path[:len(user_path) - 1]
        count, dirstruct = read_dir_structure(counter)
        parent_user_path = user_path
        new_path = user_path + "/" + filename
        new_path = new_path[1:] if new_path[:2] == "//" else new_path
        exists, parent_exists, _ = file_parent_exist(dirstruct, new_path, parent_user_path)
        if exists:
            print(
                colored("File with the same name already exists in the DFS! Please enter a new filename!", error_color))
            print()
            return count + 1, ""
        if not (os.path.exists(file_path)):
            print(colored("File to be copied not found! Please enter the correct path!", error_color))
            print()
            return count + 1, ""
        count, config = read_config_structure(counter)
        file_size = os.path.getsize(file_path)
        mapper = {}
        chunk_num = 1
        new_file = {dirstruct['lastNodeId'] + 1: {"filesize": file_size,
                                                  "id": dirstruct['lastNodeId'] + 1,
                                                  "last_acmtime": round((datetime.utcnow() - datetime(1970, 1,
                                                                                                      1)).total_seconds() * 1000),
                                                  "name": filename,
                                                  "node_type": "<file>",
                                                  "parent_name": parent_user_path,
                                                  "permission": 644,
                                                  "deleted": False}}
        if file_path.split(".")[1] == "txt":
            with open(file_path) as f:
                cluster_loc = []
                chunk_size = (file_size // partitions) + 1
                chunk = f.read(chunk_size)
                while chunk:
                    cluster = random.randint(1, 3)
                    new_filename = config["cluster_" + str(cluster) + "_loc"] + "/" + filename.split(".")[
                        0] + "_" + str(round(time.time() * 1000)) + "___" + str(chunk_num) + "." + filename.split(".")[
                                       1]
                    cluster_loc.append(new_filename)
                    chunk_file = open(new_filename, "w")
                    chunk_file.write(chunk)
                    chunk_file.close()
                    chunk_num += 1
                    chunk = f.read(chunk_size)
        elif file_path.split(".")[1] == "csv":
            data = pd.read_csv(file_path)
            cluster_loc = []
            rows_per_file = (len(data) // partitions) + 1
            rows = 0
            for partition in range(partitions):
                cluster = random.randint(1, 3)
                new_filename = config["cluster_" + str(cluster) + "_loc"] + "/" + filename.split(".")[
                    0] + "_" + str(round(time.time() * 1000)) + "___" + str(chunk_num) + "." + filename.split(".")[1]
                cluster_loc.append(new_filename)
                df1 = data.iloc[rows:rows + rows_per_file, :]
                df1.to_csv(new_filename, index=False)
                chunk_num += 1
                rows += rows_per_file

        else:
            print(colored("File type not supported! Please provide either a '.txt' or a '.csv' file!", error_color))
            print()
            return count + 1, ""
        mapper[dirstruct['lastNodeId'] + 1] = {"file_id": dirstruct['lastNodeId'] + 1,
                                               "mapping": cluster_loc,
                                               "deleted": False
                                               }
        count = update_cluster_mapping_structure(count, mapper)
        count = update_dir_structure(count, dir=new_file, trigger=True)
        count = update_dir_structure(count, dir={"lastNodeId": dirstruct["lastNodeId"] + 1}, trigger=False)
        count = update_dir_structure(count, dir={"numNodes": dirstruct["numNodes"] + 1}, trigger=False)
        print(colored("File has been transferred!", menu_color))
        print()
        return count + 1, ""

# FUNCTION TO GET THE LOCATIONS OF THE PARTITIONS
def getPartitionLocations(new_user_input, counter):
    if "-p" != new_user_input[2]:
        print(colored("Invalid argument! Please enter a valid argument!", error_color))
        print()
        return counter + 1, ""
    else:
        file_path = ast.literal_eval(new_user_input[3])
        if type(file_path) != str:
            print(colored("Invalid Path! Please enter a valid path!", error_color))
            print()
            return counter + 1, ""
        if "." not in file_path:
            print(colored("Invalid Filename! Please enter a filename with an extension!", error_color))
            print()
            return counter + 1, ""
        count, dirstruct = read_dir_structure(counter)
        parent_path = file_path[:file_path.rfind("/")]
        exists, parent_exists, file_id = file_parent_exist(dirstruct, file_path, parent_path)
        if not exists:
            print(colored("File does not exist!", error_color))
            print()
            return counter + 1, ""
        count, map_struct = read_cluster_mapping_structure(count)
        mapping = []
        for item in map_struct.values():
            if not item["deleted"]:
                if item['file_id'] == file_id:
                    for cluster_node in item['mapping']:
                        mapping.append([cluster_node.split("___")[1].split(".")[0], cluster_node.split("/")[-2][-1]])
        print()
        print(colored(tabulate(mapping, headers=["Partition Number", "Cluster Number"]), menu_color))
        print()
        return count, ""

# FUNCTION TO DISPLAY A FILE => .TXT OR .CSV
def cat(new_user_input, counter):
    if "-p" != new_user_input[2]:
        print(colored("Invalid argument! Please enter a valid argument!", error_color))
        print()
        return counter + 1, ""
    else:
        file_path = ast.literal_eval(new_user_input[3])
        if type(file_path) != str:
            print(colored("Invalid Path! Please enter a valid path!", error_color))
            print()
            return counter + 1, ""
        if "." not in file_path:
            print(colored("Invalid Filename! Please enter a filename with an extension!", error_color))
            print()
            return counter + 1, ""
        count, dirstruct = read_dir_structure(counter)
        parent_path = file_path[:file_path.rfind("/")]
        exists, parent_exists, file_id = file_parent_exist(dirstruct, file_path, parent_path)
        if not exists:
            print(colored("File does not exist!", error_color))
            print()
            return counter + 1, ""
        count, map_struct = read_cluster_mapping_structure(count)
        print()
        df = pd.DataFrame()
        for item in map_struct.values():
            if not item["deleted"]:
                if item['file_id'] == file_id:
                    for file in item['mapping']:
                        if file_path.split(".")[1] == "txt":
                            f = open(file, 'r')
                            print(f.read(), end="")
                            f.close()
                        elif file_path.split(".")[1] == "csv":
                            df_i = pd.read_csv(file)
                            df = pd.concat([df, df_i])
                        else:
                            print(colored(
                                "File type not supported! Please provide either a '.txt' or a '.csv' file!",
                                error_color))
                            return count

        if file_path.split(".")[1] == "txt":
            print()
            print()
        else:
            if len(new_user_input) == 4:
                print(df)
                print()
            elif new_user_input[4] == "-ar":
                pd.set_option('display.max_rows', None)
                print(df)
                print()
                pd.reset_option("display.max_rows")
            elif new_user_input[4] == "-ac":
                pd.set_option('display.max_columns', None)
                print(df)
                print()
                pd.reset_option("display.max_columns")
            elif new_user_input[4] == "-arc":
                pd.set_option('display.max_columns', None)
                pd.set_option('display.max_rows', None)
                print(df)
                print()
                pd.reset_option("display.max_columns")
                pd.reset_option("display.max_rows")
            else:
                print(colored(
                    "Error! Check the arguments!",
                    error_color))
                return count, ""
        return count, ""

# FUNCTION TO DELETE A FILE
def rm(new_user_input, counter):
    if "-p" != new_user_input[2]:
        print(colored("Invalid argument! Please enter a valid argument!", error_color))
        print()
        return counter + 1, ""
    else:
        file_path = ast.literal_eval(new_user_input[3])
        if type(file_path) != str:
            print(colored("Invalid Path! Please enter a valid path!", error_color))
            print()
            return counter + 1, ""
        if "." not in file_path:
            print(colored("Invalid Filename! You cannot delete a directory!", error_color))
            print()
            return counter + 1, ""
        count, dirstruct = read_dir_structure(counter)
        parent_path = file_path[:file_path.rfind("/")]
        exists, parent_exists, file_id = file_parent_exist(dirstruct, file_path, parent_path)
        if not exists:
            print(colored("File does not exist!", error_color))
            print()
            return counter + 1, ""
        count, map_struct = read_cluster_mapping_structure(count)
        for item in map_struct.values():
            if not item["deleted"]:
                if item['file_id'] == file_id:
                    for file in item['mapping']:
                        os.remove(file)
        count = update_dir_structure(count, {str(file_id): {"deleted": True}}, True)
        count = update_cluster_mapping_structure(count, {str(file_id): {"deleted": True}})
        print(colored("File has been deleted!", menu_color))
        print()
        return count, ""

# FUNCTION TO READ THE CONTENT OF A PARTITION
def readPartition(new_user_input, counter):
    if "-p" != new_user_input[2] or "-k" != new_user_input[4]:
        print(colored("Invalid arguments! Please enter valid arguments!", error_color))
        print()
        return counter + 1, ""
    else:
        file_path = ast.literal_eval(new_user_input[3])
        partition = ast.literal_eval(new_user_input[5])
        if type(file_path) != str:
            print(colored("Invalid Path! Please enter a valid path!", error_color))
            print()
            return counter + 1, ""
        if "." not in file_path:
            print(colored("Invalid Filename! Please enter a filename with an extension!", error_color))
            print()
            return counter + 1, ""
        if type(partition) != int:
            print(colored("Invalid Value for Partition! Please enter a valid value!", error_color))
            print()
            return counter + 1, ""
        count, dirstruct = read_dir_structure(counter)
        parent_path = file_path[:file_path.rfind("/")]
        exists, parent_exists, file_id = file_parent_exist(dirstruct, file_path, parent_path)
        if not exists:
            print(colored("File does not exist!", error_color))
            print()
            return counter + 1, ""
        filename = file_path.split("/")[-1]
        if ".csv" not in filename:
            print(colored("This function is supported for only '.csv' files!", error_color))
            print()
            return counter + 1, ""
        count, map_struct = read_cluster_mapping_structure(count)
        partition_data = ""
        for item in map_struct.values():
            if not item["deleted"]:
                if item['file_id'] == file_id:
                    if partition > len(item["mapping"]):
                        print(colored(
                            "Partition Value provided is greater than the number of partitions in this file! Enter the correct value for partitions!",
                            error_color))
                        print()
                        return count + 1, ""
                    for cluster_node in item['mapping']:
                        if int(cluster_node.split("___")[1][0]) == partition:
                            partition_data = pd.read_csv(cluster_node)
                            print()
                            print(colored("Data has been read and stored in a variable!", menu_color))
                            print()
                    return count, partition_data
