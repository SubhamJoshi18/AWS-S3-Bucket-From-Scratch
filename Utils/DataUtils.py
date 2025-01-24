


def convert_element_list(*args):
    return  list(args)  if  isinstance(args,tuple) else args


def format_data(aws_access_secret_key,aws_access_key_id):
    return [{
        "aws_secret_access_key":aws_access_secret_key,
        "aws_secret_access_id":aws_access_key_id
    }]