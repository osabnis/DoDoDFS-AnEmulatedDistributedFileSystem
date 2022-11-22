# IMPORTING PYTHON PACKAGES
import ast
from tabulate import tabulate
from termcolor import colored
import time
import pandas as pd

# IMPORTING LOCAL PYTHON LIBRARIES
from utils.firebase_utils import read_cluster_mapping_structure, read_dir_structure
from utils.util_utils import file_parent_exist

# GLOBAL VARIABLES
menu_color = "green"
divider_color = "blue"
header_color = "yellow"
error_color = "red"

spec_operations = [[1, "Top X equity funds based on fund performance - higher risk funds"],
                   [2, "Top X debt funds based on fund performance - lower risk funds"],
                   [3, "Top X Funds which have given the highest returns over the last 1/3/5 years"],
                   [4, "Top X Funds which have the highest turnover ratio"],
                   [5, "Top X Funds which have the highest investment"]]

modes = [[1, "Partition Mode", "In this mode - you will see the way map-reduce has been simulated within DODFS"],
         [2, "Quick Mode", "In this mode - you will directly see the output without the intermediate steps"]]

finance_file = "/omkar/mutual_funds_dataset.csv"


# FUNCTION TO CHECK IF A NUMBER IS A FLOAT OR NOT
def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

# FUNCTION TO HANDLE QUERYING FOR NUMBERS
def int_float_check(df, left, operation, right):
    if operation == "=":
        df = df.loc[df[left] == right]
    elif operation == ">":
        df = df.loc[df[left] > right]
    elif operation == "<":
        df = df.loc[df[left] < right]
    elif operation == ">=":
        df = df.loc[df[left] >= right]
    elif operation == "<=":
        df = df.loc[df[left] <= right]
    elif operation == "!=":
        df = df.loc[df[left] != right]
    else:
        raise Exception("Not Supported!")
    return df

# FUNCTION TO HANDLE QUERYING FOR STRINGS
def string_check(df, left, operation, right):
    if operation == "=":
        df = df.loc[df[left] == right]
    elif operation == "!=":
        df = df.loc[df[left] != right]
    else:
        raise Exception("Not Supported!")
    return df


# FUNCTION FOR GENERAL QUERYING
def query_mode(counter):
    print()
    print(colored("Welcome to Query Mode!", menu_color))
    print(colored("DODFS allows you to do basic search queries on a CSV file of your choice!", menu_color))
    print(colored("Please enter the full path of the CSV file you want to query on:", menu_color))
    print(colored("File Name:", header_color), end=" ")
    csv_file_path = ast.literal_eval(input())
    if type(csv_file_path) != str:
        print(colored("Invalid Path! Please enter a valid path!", error_color))
        print()
        return counter + 1, ""
    if "." not in csv_file_path:
        print(colored("Invalid Filename! Please enter a filename with an extension!", error_color))
        print()
        return counter + 1, ""
    count, dirstruct = read_dir_structure(counter)
    parent_path = csv_file_path[:csv_file_path.rfind("/")]
    exists, parent_exists, file_id = file_parent_exist(dirstruct, csv_file_path, parent_path)
    count, map_struct = read_cluster_mapping_structure(count)
    if not exists:
        print(colored("File does not exist!", error_color))
        print()
        return counter + 1, ""
    print()
    print(colored("DODO DFS can show you your query results in two modes:", menu_color))
    print(colored(tabulate(modes, headers=["ID", "Mode Name", "Mode Description"]), menu_color))
    print()
    print(colored("Please enter the ID of the mode you want to use:", menu_color))
    print(colored("Mode:", header_color), end=" ")
    mode_input = input()
    if mode_input not in ["1", "2"]:
        print(colored("Invalid argument! Please enter a valid argument!", error_color))
        print()
        return counter + 1, ""
    if mode_input == "1":
        print()
        print(colored(
            "In an HDFS, all the files are partitioned and stored across various data nodes in 3 clusters!", menu_color))
        time.sleep(1)
        print(
            colored("So to process any file, we must first find where are the partitions located!", menu_color))
        time.sleep(1)
        print(colored("To find this, we use the GetPartitionLocations function!", menu_color))
        time.sleep(1)
        print(colored("For your dataset, these are the locations of the partitions:", menu_color))
        time.sleep(1)
    mapping = []
    file_mapping = []
    for item in map_struct.values():
        if not item["deleted"]:
            if item['file_id'] == file_id:
                for cluster_node in item['mapping']:
                    file_mapping.append(cluster_node)
                    mapping.append([cluster_node.split("___")[1].split(".")[0], cluster_node.split("/")[-2][-1],
                                    cluster_node[52:]])
    if mode_input == "1":
        print()
        print(tabulate(mapping,
                       headers=["Partition Number", "Cluster Number", "Directory Location on the Cluster"], tablefmt='psql'))
        time.sleep(1)
    temp_data = pd.read_csv(file_mapping[0])
    print()
    print(colored("DODFS has found that your dataset has the following columns:", menu_color))
    print(tabulate(pd.DataFrame(temp_data.dtypes), headers=['Column Name', 'Data Type'], tablefmt='psql'))
    print(colored("Please enter columns you want to project:", menu_color))
    print(colored("Columns:", header_color), end=" ")
    columns = input().split(",")
    columns = [x.strip() for x in columns]
    flag = True
    for name in columns:
        if name not in temp_data.columns:
            flag = False
    if not flag:
        print(colored("Invalid Column Names! Please enter column names in your data!", error_color))
        print()
        return counter + 1, ""
    print()
    print(colored("Conditions must be entered as a LIST of LISTS", menu_color))
    print(colored("Each LIST must have => 1. Column Name, 2. Operator, 3. Value", menu_color))
    print(colored("Example: [['Age', '>', '18], ['Name', 'in', ['Jack', 'Alex']]]", menu_color))
    print(colored("DODFS supports conditions on NUMBERS and STRINGS!", menu_color))
    print()
    print(colored("Please enter your where conditions (separated by commas):", menu_color))
    print(colored("Conditions:", header_color), end=" ")
    conditions = ast.literal_eval(input())
    left_flag, operation_flag, right_flag = True, True, True
    for name in conditions:
        if len(name) != 3:
            print(colored("Invalid Conditions! Please enter conditions in the required list form!", error_color))
            print()
            return counter + 1, ""
        left, operation, right = name[0], name[1], name[2]
        if left not in temp_data.columns:
            left_flag = False
        if operation not in ["=", "!=", ">", "<", ">=", "<=", "in", "not in"]:
            operation_flag = False
        if type(right) not in [list, int, float, str]:
            right_flag = False
    if not left_flag:
        print(colored("Invalid Conditions! Please enter conditions on column names in your data!", error_color))
        print()
        return counter + 1, ""
    if not operation_flag:
        print(colored('Invalid Conditions! Please enter valid operations => ["=", "!=", ">", "<", ">=", "<=", "in", "not in"]!', error_color))
        print()
        return counter + 1, ""
    if not right_flag:
        print(colored("Invalid Conditions! Please enter values that are either numbers or strings!", error_color))
        print()
        return counter + 1, ""
    if mode_input == "1":
        print(colored("To simulate a real HDFS - we will run the required operation on each partition separately!", menu_color))
        time.sleep(1)
        print(colored(f"We will first find the results in each partition and then combine the result together", menu_color))
        time.sleep(1)
        print(colored(f"Let's start viewing the results at each partition!", menu_color))
        print()
        time.sleep(1)
    final_df = pd.DataFrame()
    for i in range(len(file_mapping)):
        data = pd.read_csv(file_mapping[i])
        if mode_input == "1":
            print(colored(f"Entering Partition {i + 1}:", menu_color))
            print()
            time.sleep(1)
        for condition in conditions:
            left_x, operation_x, right_x = condition[0], condition[1], condition[2]
            if type(right_x) != list and (right_x.isnumeric() or isfloat(right_x)):
                right_x = float(right_x)
            if type(right_x) == list:
                if operation_x == "in":
                    temp_df = pd.DataFrame()
                    for name in right_x:
                        if name.isnumeric() or isfloat(num=name):
                            loop_df = int_float_check(df=data, left=left_x, operation="=", right=name)
                        else:
                            loop_df = string_check(df=data, left=left_x, operation="=", right=name)
                        temp_df = pd.concat([temp_df, loop_df])
                    data = temp_df
                elif operation_x == "not in":
                    temp_df = pd.DataFrame()
                    for name in right_x:
                        if name.isnumeric() or isfloat(num=name):
                            loop_df = int_float_check(df=data, left=left_x, operation="!=", right=name)
                        else:
                            loop_df = string_check(df=data, left=left_x, operation="!=", right=name)
                        temp_df = pd.concat([temp_df, loop_df])
                    data = temp_df
                else:
                    raise Exception("Not Supported!")
            elif type(right_x) == str:
                data = string_check(df=data, left=left_x, operation=operation_x, right=right_x)
            elif type(right_x) == float:
                data = int_float_check(df=data, left=left_x, operation=operation_x, right=right_x)
            else:
                raise Exception("Not Supported!")
        if mode_input == "1":
            print(colored(f"These are the results at this partition:", menu_color))
            time.sleep(1)
            print(tabulate(data[columns], headers=columns, tablefmt='psql'))
        final_df = pd.concat([final_df, data])
    if mode_input == "1":
        print(colored(f"Now let us REDUCE the combined result to get our final result!", menu_color))
    print(colored(f"The final result is:", menu_color))
    time.sleep(1)
    print(tabulate(final_df[columns], headers=columns, tablefmt='psql'))
    print()
    return counter, ""
