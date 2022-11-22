# FUNCTION TO FIND THE PARENT AND THE FILE ALREADY EXISTS
def file_parent_exist(dirstruct, user_path, parent_user_path):
    exists = False
    parent_exists = False
    file_id = 0
    for inode in dirstruct["inodes"]:
        if inode["deleted"]:
            continue
        if inode["parent_name"]:
            if inode["parent_name"] == "/":
                inode_path = inode["parent_name"] + inode["name"]
            else:
                inode_path = inode["parent_name"] + "/" + inode["name"]
        else:
            inode_path = inode["name"]
        if inode_path == parent_user_path:
            parent_exists = True
        if inode_path == user_path:
            exists = True
            file_id = inode['id']
    return exists, parent_exists, file_id

# FUNCTION TO CONVERT OCTAL PERMISSIONS TO STRING: EACH DIGIT IS THE SUM OF ALL THE PERMISSIONS - 4 FOR READ, 2 FOR WRITE AND 1 FOR EXECUTE
def octal_to_readable(octal_form, file_type):
    # PERMISSIONS STRING
    string_permission = ""
    # VALUES FOR EACH PERMISSION TYPE
    values_dict = {'r': 4, 'w': 2, 'x': 1}
    # LOOP TO GO OVER THE OCTAL STRING
    for unit in [int(i) for i in str(octal_form)]:
        # LOOP TO CHECK EACH DIGIT WITH EACH PERMISSION TYPE
        for key, value in values_dict.items():
            # IF PERMISSION IS ALLOWED, ADD THE PERMISSION AND SUBTRACT THE VALUE
            if unit >= value:
                string_permission += key
                unit -= value
            else:
                string_permission += "-"
    # ADDING 'D' IF DIRECTORY OR '-' IF FILE
    if file_type == "<DIR>":
        return "d" + string_permission
    else:
        return "-" + string_permission

# FUNCTION TO CHECK IF A NUMBER IS A FLOAT OR NOT
def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False