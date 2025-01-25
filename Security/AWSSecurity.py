import pandas as pd


class AWSSecurity:

    def __init__(self):
        pass

    def validate_config(self,client_df : pd.DataFrame,json_df : pd.DataFrame):
        is_valid_config = True
        try:
            valid_aws_secret_access_id = json_df['aws_secret_access_id'].tolist()
            valid_aws_secret_access_key = json_df['aws_secret_access_key'].tolist()
            for index , rows in client_df.iterrows():
                client_access_id = rows['aws_secret_access_id']
                client_access_key = rows['aws_secret_access_key']
                if valid_aws_secret_access_id.__contains__(client_access_id) and valid_aws_secret_access_key.__contains__(client_access_key):
                    return is_valid_config
                else:
                    is_valid_config = False
                    return is_valid_config
        except Exception as error:
            print(f'Error while Validating the Config , Error : {error}')