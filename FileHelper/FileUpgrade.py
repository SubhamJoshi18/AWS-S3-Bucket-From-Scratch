import json
import os

from FileHelper.JsonUpgrade import JSONUpgrade


class FileUpgrade:


    def __init__(self):
        self.fileHelper = JSONUpgrade()


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






    def base_folder(self):
        return 'MockData'

    def __repr__(self):
        return 'This is a class to upgrade the file'

