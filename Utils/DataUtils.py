import os


def convert_element_list(*args):
    return  list(args)  if  isinstance(args,tuple) else args


def format_data(aws_access_secret_key,aws_access_key_id):
    return [{
        "aws_secret_access_key":aws_access_secret_key,
        "aws_secret_access_id":aws_access_key_id
    }]


def get_folder_size(folder_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if os.path.isfile(file_path):
                total_size += os.path.getsize(file_path)
    return total_size



