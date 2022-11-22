# IMPORTING PYTHON PACKAGES
from termcolor import colored
import colorama
from pyfiglet import Figlet

# IMPORTING LOCAL PYTHON LIBRARIES
# HDFS RELATED FUNCTIONS
from utils.hdfs_utils import help, displayconfig, quit, updateconfig
from utils.hdfs_utils import ls, mkdir, put, getPartitionLocations, cat, rm, readPartition
# SPECIAL OPERATIONS RELATED FUNCTIONS
from utils.specops_utils import specops
# QUERY RELATED FUNCTIONS
from utils.query_utils import query_mode

# GLOBAL VARIABLES
menu_color = "green"
divider_color = "blue"
header_color = "yellow"
error_color = "red"

# FUNCTION TO PARSE THE INPUT
def input_parser(counter, user_input):
    new_user_input = user_input.split(" ")

    if "dodfs" != new_user_input[0]:
        print(colored("Invalid Input! Please enter a valid command!", error_color))
        print()
        return counter + 1, ""

    else:
        if len(new_user_input) < 2:
            print(colored(
                f"Please provide the command. Please check the help section for the available commands!", error_color))
            print()
            return counter + 1, ""

        elif "help()" == new_user_input[1]:
            if len(new_user_input) != 2:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Use dodfs help() for the help section!", error_color))
                print()
                return counter + 1, ""
            count = help(counter)
            return count, ""

        elif "quit()" == new_user_input[1]:
            if len(new_user_input) != 2:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count = quit()
            return count, ""

        elif "displayconfig()" == new_user_input[1]:
            if len(new_user_input) != 2:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count = displayconfig(counter=counter)
            return count, ""

        elif "updateconfig" == new_user_input[1]:
            if len(new_user_input) != 4:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count, var = updateconfig(new_user_input=new_user_input, counter=counter)
            return count, var

        elif "ls" == new_user_input[1]:
            if len(new_user_input) != 4:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count, var = ls(new_user_input=new_user_input, counter=counter)
            return count, var

        elif "mkdir" == new_user_input[1]:
            if len(new_user_input) != 4:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count, var = mkdir(new_user_input=new_user_input, counter=counter)
            return count, var

        elif "put" == new_user_input[1]:
            if len(new_user_input) != 8:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count, var = put(new_user_input=new_user_input, counter=counter)
            return count, var

        elif "getPartitionLocations" == new_user_input[1]:
            if len(new_user_input) != 4:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count, var = getPartitionLocations(new_user_input=new_user_input, counter=counter)
            return count, var

        elif "cat" == new_user_input[1]:
            if len(new_user_input) > 5 or len(new_user_input) < 4:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count, var = cat(new_user_input=new_user_input, counter=counter)
            return count, var

        elif "rm" == new_user_input[1]:
            if len(new_user_input) != 4:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count, var = rm(new_user_input=new_user_input, counter=counter)
            return count, var

        elif "readPartition" == new_user_input[1]:
            if len(new_user_input) != 6:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count, var = readPartition(new_user_input=new_user_input, counter=counter)
            return count, var

        elif "query-mode()" == new_user_input[1]:
            if len(new_user_input) != 2:
                print(colored(
                    f"Please provide the correct parameters for {new_user_input[1]}. Use dodfs help() for the help section!",
                    error_color))
                print()
                return counter + 1, ""
            count, var = query_mode(counter)
            return count, var

        elif "spec-ops()" == new_user_input[1]:
            if len(new_user_input) != 4:
                print(colored(f"Please provide the correct parameters for {new_user_input[1]}. Please check the help section for the command!", error_color))
                print()
                return counter + 1, ""
            count, var = specops(new_user_input=new_user_input, counter=counter)
            return count, var

        else:
            print(colored("Invalid command! Please enter a valid command!", error_color))
            print()
            return counter + 1, ""

# LAUNCH FUNCTION
def launch():
    print(colored("============================================================", divider_color))
    print(colored("============================================================", divider_color))
    f = Figlet(font="slant")
    print(colored(f.renderText("  DODO DFS"), header_color))
    print(colored("          * A personal distributed file system *                                 ", menu_color))
    print(colored("    * DODO DFS uses a 3-cluster distributed system *", menu_color))
    print(colored("      * Initialized with '/' ROOT folder on first use *", menu_color))
    print(colored("           * DODO DFS supports CSV and TXT files *", menu_color))
    print(colored("============================================================", divider_color))
    print(colored("============================================================", divider_color))
    print()


# FUNCTION TO START DFS OPERATIONS
def start():
    # INDEX VARIABLE
    counter = 0
    # INFINITE LOOP UNTIL USER QUITS DFS APPLICATION
    while counter >= 0:
        # DIFFERENT GREETINGS FOR NEW/REPEAT USAGE
        if counter == 0:
            print(colored("Hello User!", menu_color))
            print(colored("For help - please type: ", menu_color), end="")
            print(colored("dodfs help()", header_color))
        else:
            print(colored("For help with commands - please type: ", menu_color), end="")
            print(colored("dodfs help()", header_color))
        print(colored("Please enter the command for the operation you would like to perform!", menu_color))
        # TAKE USER INPUT
        print(colored("Operation:", header_color), end=" ")
        user_input = input()
        # PASS THE INPUT TO THE PARSER
        counter, data = input_parser(counter=counter, user_input=user_input)


# MAIN FUNCTION
if __name__ == "__main__":
    colorama.init(autoreset=True)
    launch()
    start()
