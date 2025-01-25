import json
import os
from Constants.Modules import BUCKET_FOLDER
from FileHelper.JsonUpgrade import JSONUpgrade


class FileUpgrade:


    def __init__(self):
        self.fileHelper = JSONUpgrade()



    def get_base_s3_bucket_path(self):
        return os.path.join(os.getcwd(),BUCKET_FOLDER)


    def check_file_exists(self,file_path):
        if isinstance(file_path,str) and len(file_path) > 0:
            return os.path.exists(file_path)

    def retrieve_aws_config(self):
        base_folder = self.base_folder()
        mock_json_path = os.path.join(os.getcwd(),base_folder,'MockAWSConfig.json')
        try:
                is_valid_path = self.check_file_exists(mock_json_path)

                if not is_valid_path:
                    raise FileNotFoundError(f'{mock_json_path} Does not Exists on your System')

                decoded_body = self.fileHelper.parse_json(file_path=mock_json_path)

                if isinstance(decoded_body,list) and str(len(decoded_body)).startswith('0'):
                    print(f'AWS IAM Identity Is having Internal Server Error, Please Wait')

                return decoded_body
        except FileNotFoundError as file_error:
                print(f'Error while Searching For the {mock_json_path} , Trying Again')

        except Exception as error:
                print(f'Un Expected Error : {error}')


    def concat_folders(self,path_to_concat,original_path):
        return os.path.join(original_path,path_to_concat)


    def create_buckets_folders(self,bucket_name):

        s3_bucket_path = self.get_base_s3_bucket_path()
        if os.listdir(s3_bucket_path).count(bucket_name) > 0 or bucket_name in os.listdir(s3_bucket_path):
            print(f'The Bucket You are Trying to Create Already Exists , Please Try Different Name')
            return
        concat_full_path = self.concat_folders(bucket_name,s3_bucket_path)
        os.makedirs(concat_full_path)
        print(f"Bucket '{bucket_name}' created successfully at '{concat_full_path}'.")
        return concat_full_path





    def base_folder(self):
        return 'MockData'

    def __repr__(self):
        return 'This is a class to upgrade the file'

