# IMPORTING PYTHON PACKAGES
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


# FUNCTION TO RETURN TOP X RESULTS FOR Y TYPE OF FUND
def top_x_fund_y_results(df, x, y, mode):
    setup = df.groupby('category')
    if mode == "1":
        print(colored(f"These are the results at this partition:", menu_color))
        time.sleep(1)
        if y not in setup.groups.keys():
            new_df = pd.DataFrame()
            print(tabulate(new_df, headers=['Mutual Fund Name', 'Category', 'Fund Rating', 'AuM(Cr)'], tablefmt='psql'))
        else:
            new_df = df.sort_values(['fund_rating', 'AuM(Cr)'], ascending=False).groupby('category').get_group(y).head(x)
            print(tabulate(new_df[['Mutual Fund Name', 'category', 'fund_rating', 'AuM(Cr)']], headers='keys',
                           tablefmt='psql'))
    else:
        if y not in setup.groups.keys():
            new_df = pd.DataFrame()
        else:
            new_df = df.sort_values(['fund_rating', 'AuM(Cr)'], ascending=False).groupby('category').get_group(y).head(x)
    return new_df

# FUNCTION TO RETURN TOP X RESULTS FOR RETURNS OVER 1/3/5 YEARS
def top_x_funds_returns_over_y_years(df, x, y, mode):
    if mode == "1":
        print(colored(f"These are the results at this partition:", menu_color))
        time.sleep(1)
        if y == "1":
            new_df = df.sort_values(['return_1yr'], ascending=False).head(x)
            print(tabulate(new_df[['Mutual Fund Name', 'category', 'risk_type', 'return_1yr']], headers='keys',
                           tablefmt='psql'))
        elif y == "3":
            new_df = df.sort_values(['return_3yr'], ascending=False).head(x)
            print(tabulate(new_df[['Mutual Fund Name', 'category', 'risk_type', 'return_3yr']], headers='keys',
                           tablefmt='psql'))
        else:
            new_df = df.sort_values(['return_5yr'], ascending=False).head(x)
            print(tabulate(new_df[['Mutual Fund Name', 'category', 'risk_type', 'return_5yr']], headers='keys',
                           tablefmt='psql'))
    else:
        if y == "1":
            new_df = df.sort_values(['return_1yr'], ascending=False).head(x)
        elif y == "3":
            new_df = df.sort_values(['return_3yr'], ascending=False).head(x)
        else:
            new_df = df.sort_values(['return_5yr'], ascending=False).head(x)
    return new_df

# FUNCTION TO RETURN TOP X RESULTS FOR TURNOVER RATIO
def top_x_funds_turnover_ratio(df, x, mode):
    if mode == "1":
        print(colored(f"These are the results at this partition:", menu_color))
        time.sleep(1)
        new_df = df.sort_values(['turnover_ratio'], ascending=False).head(x)
        print(tabulate(new_df[['Mutual Fund Name', 'category', 'risk_type', 'turnover_ratio']], headers='keys',
                       tablefmt='psql'))
    else:
        new_df = df.sort_values(['turnover_ratio'], ascending=False).head(x)
    return new_df

# FUNCTION TO RETURN TOP X RESULTS FOR MOST INVESTMENT
def top_x_funds_investment(df, x, mode):
    if mode == "1":
        print(colored(f"These are the results at this partition:", menu_color))
        time.sleep(1)
        new_df = df.sort_values(['AuM(Cr)'], ascending=False).head(x)
        print(tabulate(new_df[['Mutual Fund Name', 'category', 'risk_type', 'AuM(Cr)']], headers='keys',
                       tablefmt='psql'))
    else:
        new_df = df.sort_values(['AuM(Cr)'], ascending=False).head(x)
    return new_df

# FUNCTION FOR THE 5 INTERESTING INSIGHTS
def specops(new_user_input, counter):
    global years
    if "-d" != new_user_input[2]:
        print(colored("Invalid argument! Please enter a valid argument!", error_color))
        print()
        return counter + 1, ""
    else:
        if new_user_input[3] != "finance":
            print(colored(
                "Special Operations Function is only available for the financial mutual fund dataset as of now!",
                error_color))
            print()
            return counter + 1, ""
        else:
            print()
            print(colored("Welcome to the Special Operations Function!", menu_color))
            print(colored("DODFS provides you with 5 interesting insights from the Financial Mutual Funds Dataset!",
                          menu_color))
            print(colored(
                "This dataset has existing data on mutual funds and their past performance and we will use it to provide you with some interesting insights!",
                menu_color))
            print()
            print(colored("DODO DFS can show you the following insights:", menu_color))
            print(colored(tabulate(spec_operations, headers=["ID", "Insight"]), menu_color))
            print()
            print(colored("Please enter the ID of the insight you want to view:", menu_color))
            print(colored("Operation:", header_color), end=" ")
            spec_op_input = input()
            if spec_op_input not in ["1", "2", "3", "4", "5"]:
                print(colored("Invalid argument! Please enter a valid argument!", error_color))
                print()
                return counter + 1, ""
            print()
            if spec_op_input == "3":
                print(colored("Please enter the number of years you want to see the returns over:", menu_color))
                print(colored("Years:", header_color), end=" ")
                years = input()
                print()
                if years not in ["1", "3", "5"]:
                    print(colored("Invalid argument! Please enter a valid argument!", error_color))
                    print()
                    return counter + 1, ""
            print(colored("Please enter the value of X from the above selected query:", menu_color))
            print(colored("X:", header_color), end=" ")
            x_value = int(input())
            if x_value > 10:
                print(colored("X value too large! Please provide a value between 1-10!", error_color))
                print()
                return counter + 1, ""
            if x_value < 1:
                print(colored("X value too small! Please provide a value between 1-10!", error_color))
                print()
                return counter + 1, ""
            print()
            print(colored("DODO DFS can show you your requested insights in two modes:", menu_color))
            print(colored(tabulate(modes, headers=["ID", "Mode Name", "Mode Description"]), menu_color))
            print()
            print(colored("Please enter the ID of the mode you want to use:", menu_color))
            print(colored("Operation:", header_color), end=" ")
            mode_input = input()
            if mode_input not in ["1", "2"]:
                print(colored("Invalid argument! Please enter a valid argument!", error_color))
                print()
                return counter + 1, ""
            count, dirstruct = read_dir_structure(counter)
            parent_path = finance_file[:finance_file.rfind("/")]
            exists, parent_exists, file_id = file_parent_exist(dirstruct, finance_file, parent_path)
            count, map_struct = read_cluster_mapping_structure(count)
            if mode_input == "1":
                print()
                print(colored(
                    "In an HDFS, all the files are partitioned and stored across various data nodes in 3 clusters!",
                    menu_color))
                time.sleep(1)
                print(
                    colored("So to process any file, we must first find where are the partitions located!", menu_color))
                time.sleep(1)
                print(colored("To find this, we use the GetPartitionLocations function!", menu_color))
                time.sleep(1)
                print(colored("For our financial dataset, these are the locations of the partitions:", menu_color))
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
                               headers=["Partition Number", "Cluster Number", "Directory Location on the Cluster"]))
                time.sleep(1)
                print()
                print(colored(
                    "To simulate a real HDFS - we will run the required operation on each partition separately!",
                    menu_color))
                time.sleep(1)
                print(colored(
                    f"We will first find the top {x_value} results in each partition and then combine the result together",
                    menu_color))
                time.sleep(1)
                print(colored(f"We will then return the top {x_value} results for the COMBINED result!", menu_color))
                time.sleep(1)
                print(colored(f"Let's start viewing the results at each partition!", menu_color))
                print()
                time.sleep(1)
                if spec_op_input == "1" or spec_op_input == "2":
                    print(colored(f"To calculate Top X funds, we use the following algorithm:", menu_color))
                    time.sleep(1)
                    print(colored(f"We first find the top rated funds in the required category.", menu_color))
                    time.sleep(1)
                    print(colored(f"We resolve ties using the total investment column.", menu_color))
                    time.sleep(1)
                    print(colored(f"This is because more investment shows more confidence in the fund!", menu_color))
                    print()
                    time.sleep(1)
            final_df = pd.DataFrame()
            for i in range(len(file_mapping)):
                data = pd.read_csv(file_mapping[i])
                if mode_input == "1":
                    print(colored(f"Entering Partition {i + 1}:", menu_color))
                    print()
                    time.sleep(1)
                if spec_op_input == "1":
                    loop_df = top_x_fund_y_results(df=data, x=x_value, y="Equity", mode=mode_input)
                elif spec_op_input == "2":
                    loop_df = top_x_fund_y_results(df=data, x=x_value, y="Debt", mode=mode_input)
                elif spec_op_input == "3":
                    loop_df = top_x_funds_returns_over_y_years(df=data, x=x_value, y=years, mode=mode_input)
                elif spec_op_input == "4":
                    loop_df = top_x_funds_turnover_ratio(df=data, x=x_value, mode=mode_input)
                else:
                    loop_df = top_x_funds_investment(df=data, x=x_value, mode=mode_input)
                frames = [final_df, loop_df]
                final_df = pd.concat(frames)
            if mode_input == "1":
                print(colored(
                    f"Now that we performed the operation at each partition, let us COMBINE THE results together!",
                    menu_color))
                print(colored(f"The combined result is:", menu_color))
                time.sleep(1)
                if spec_op_input == "1":
                    print(tabulate(final_df[['Mutual Fund Name', 'category', 'fund_rating', 'AuM(Cr)']], headers='keys',
                                   tablefmt='psql'))
                elif spec_op_input == "2":
                    print(tabulate(final_df[['Mutual Fund Name', 'category', 'fund_rating', 'AuM(Cr)']],
                                   headers='keys', tablefmt='psql'))
                elif spec_op_input == "3":
                    if years == "1":
                        print(tabulate(final_df[['Mutual Fund Name', 'category', 'risk_type', 'return_1yr']],
                                       headers='keys',
                                       tablefmt='psql'))
                    elif years == "3":
                        print(tabulate(final_df[['Mutual Fund Name', 'category', 'risk_type', 'return_3yr']],
                                       headers='keys',
                                       tablefmt='psql'))
                    else:
                        print(tabulate(final_df[['Mutual Fund Name', 'category', 'risk_type', 'return_5yr']],
                                       headers='keys',
                                       tablefmt='psql'))
                elif spec_op_input == "4":
                    print(tabulate(final_df[['Mutual Fund Name', 'category', 'risk_type', 'turnover_ratio']],
                                   headers='keys',
                                   tablefmt='psql'))
                else:
                    print(tabulate(final_df[['Mutual Fund Name', 'category', 'risk_type', 'AuM(Cr)']], headers='keys',
                                   tablefmt='psql'))
                print()
                print(colored(f"Now let us REDUCE the combined result to get our final result!", menu_color))
                print(colored(f"The final result is:", menu_color))
                time.sleep(1)
            if spec_op_input == "1":
                final_final_df = top_x_fund_y_results(df=final_df, x=x_value, y='Equity', mode="2")
                print(tabulate(final_final_df[['Mutual Fund Name', 'category', 'fund_rating', 'AuM(Cr)']],
                               headers='keys', tablefmt='psql'))
            elif spec_op_input == "2":
                final_final_df = top_x_fund_y_results(df=final_df, x=x_value, y='Debt', mode="2")
                print(tabulate(final_final_df[['Mutual Fund Name', 'category', 'fund_rating', 'AuM(Cr)']],
                               headers='keys', tablefmt='psql'))
            elif spec_op_input == "3":
                final_final_df = top_x_funds_returns_over_y_years(df=final_df, x=x_value, y=years, mode="2")
                if years == "1":
                    print(tabulate(final_final_df[['Mutual Fund Name', 'category', 'risk_type', 'return_1yr']],
                                   headers='keys', tablefmt='psql'))
                elif years == "3":
                    print(tabulate(final_final_df[['Mutual Fund Name', 'category', 'risk_type', 'return_3yr']],
                                   headers='keys', tablefmt='psql'))
                else:
                    print(tabulate(final_final_df[['Mutual Fund Name', 'category', 'risk_type', 'return_5yr']],
                                   headers='keys',
                                   tablefmt='psql'))
            elif spec_op_input == "4":
                final_final_df = top_x_funds_turnover_ratio(df=final_df, x=x_value, mode="2")
                print(tabulate(final_final_df[['Mutual Fund Name', 'category', 'risk_type', 'turnover_ratio']],
                               headers='keys',
                               tablefmt='psql'))
            else:
                final_final_df = top_x_funds_investment(df=final_df, x=x_value, mode="2")
                print(tabulate(final_final_df[['Mutual Fund Name', 'category', 'risk_type', 'AuM(Cr)']], headers='keys',
                               tablefmt='psql'))
            return counter + 1, ""
