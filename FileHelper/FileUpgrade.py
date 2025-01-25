import json
import os
from Constants.Modules import BUCKET_FOLDER
from FileHelper.JsonUpgrade import JSONUpgrade
from Utils.DataUtils import get_folder_size


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



    def delete_buckets(self,bucket_name,file_path):
        valid_delete = True
        try:
            for index , buckets in enumerate(os.listdir(file_path)):
                if buckets.__contains__(bucket_name) or buckets == bucket_name:
                    if os.path.isdir(file_path):
                        size_folder = get_folder_size(file_path)
                        if isinstance(size_folder,int) and size_folder == 0:
                            os.rmdir(file_path)
                            print(f'The Bucket Has Been Deleted : {file_path}')
                        else:
                            self.delete_sub_folders(file_path=file_path)
                else:
                    valid_delete = False
                    raise Exception('Bucket Name Does not exists')

        except Exception as error:
            valid_delete = False
            raise error

        else:
            return valid_delete

    def  delete_sub_folders(self,file_path):
        for index , folder_prefix in enumerate(os.listdir(file_path)):
            actual_path = os.path.join(file_path,folder_prefix)
            os.remove(actual_path)

        if get_folder_size(file_path) == 0:
            os.rmdir(file_path)
            print(f'The Bucket Has Been Deleted : {file_path}')
            return





    def base_folder(self):
        return 'MockData'

    def __repr__(self):
        return 'This is a class to upgrade the file'

