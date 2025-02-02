import os
import hashlib
import shutil
from Exceptions.AWSException import AWSBucketCreationException
from Constants.Modules import BUCKET_FOLDER
from FileHelper.FileUpgrade import FileUpgrade
from datetime import datetime
from Exceptions.AWSException import AWSListObjectException
from FileHelper.JsonUpgrade import JSONUpgrade
from Utils.DataUtils import get_folder_size
from Exceptions.AWSException import AWSPutObjectException


class S3Bucket:

    def __init__(self):
        self.fileHelper = FileUpgrade()
        self.jsonUpgrade = JSONUpgrade()

    def create_buckets(self,bucket_name):
        try:

            s3_path = self.fileHelper.get_base_s3_bucket_path()
            if not self.fileHelper.check_file_exists(s3_path):
                os.mkdir(s3_path)
            is_created_path = self.fileHelper.create_buckets_folders(bucket_name=bucket_name)
            if isinstance(is_created_path,str) and len(is_created_path) > 0:
                json_info = self.jsonUpgrade.create_json_file_bucket_name(bucket_name,is_created_path)
                return

        except AWSBucketCreationException as aws_error:
            print(f'Error Creating the AWS Bucket : {bucket_name}, Please Try Again')
            return
        except Exception as error:
            print(f'An Unexpected Error has been En-countered : {error}')


    def delete_buckets(self,bucket_name):
        try:
            s3_bucket_path = self.fileHelper.get_base_s3_bucket_path()

            if str(len(os.listdir(s3_bucket_path))).startswith('0'):
                print(f'No Bucket Is Available to be Deleted')
                return

            is_valid_bucket = list(filter(lambda x : x == bucket_name,os.listdir(s3_bucket_path)))

            if len(is_valid_bucket) == 0:
                print(f'The {bucket_name} Does not Exists on the S3 Bucket, Please give the appropriate name')
                return

            buckets_to_be_deleted = is_valid_bucket[0] if is_valid_bucket.count(bucket_name) == 1 else ''


            is_valid_deleted =  self.fileHelper.delete_buckets(bucket_name=buckets_to_be_deleted,file_path=os.path.join(s3_bucket_path,buckets_to_be_deleted))

            if not is_valid_deleted:
                raise Exception(f'Issue Deleting the Bucket : {buckets_to_be_deleted}')

        except Exception as error:
            print(f'Error Deleting the Bucket, Please Try again, Error {error}')


    def download_buckets(self,bucket_name,bucket_prefix,download_path):
        re_enter = 0
        try:
            s3_bucket_path  = self.fileHelper.get_base_s3_bucket_path()

            if not self.fileHelper.check_file_exists(s3_bucket_path):
                print(f'S3 Bucket Cloud Service Does not Exists, Please Try again later')
                return


            is_bucket_exists =  bucket_name in os.listdir(s3_bucket_path) and os.listdir(s3_bucket_path).count(bucket_name) > 0

            if not is_bucket_exists:
                print(f'The Bucket Does not Exists, Please Enter the Bucket Appropriate Name {bucket_name}')


            valid_bucket_name = list(filter(lambda x : x == bucket_name,os.listdir(s3_bucket_path))).pop()

            valid_bucket_path = os.path.join(s3_bucket_path,valid_bucket_name)


            try:
                exists = []
                split_prefix = bucket_prefix.split('/') if isinstance(bucket_name,str) else None

                if split_prefix is None:
                    raise Exception('Error while Determining the Prefix, or the Prefix you provided is incorrect')

                if os.path.isdir(valid_bucket_path) and os.listdir(valid_bucket_path) > 0:
                     for prefix in split_prefix:
                         if os.listdir(valid_bucket_path).count(prefix):
                             re_enter += 1
                             exists.append(prefix)
                             break

                updated_path = os.path.join(valid_bucket_path, exists[0])
                exists.pop()
                print(f'Going Depth to the Folder by {re_enter}')

                if os.path.isdir(updated_path) and os.listdir(updated_path) > 0:
                    for prefix in split_prefix:
                        if os.listdir(updated_path).count(prefix):
                            re_enter += 1
                            exists.append(prefix)
                            break

                updated_path = os.path.join(updated_path,exists[0])

                if isinstance(updated_path,str) and updated_path.__contains__(bucket_prefix):
                    last_prefix = os.path.basename(updated_path)
                    print(f'Downloading The Object which are stored on {last_prefix} to {download_path}')
                    self.fileHelper.download_and_save(updated_path,download_path)
                    return
            except Exception as error:
                raise error


        except Exception as error:
            print(f'Error Downloading the Object From the Bucket : {bucket_prefix}, Please Try again..')



    def list_objects(self,bucket_name,bucket_prefix):
        depth_level = 0
        response = {
            "Contents": {

                f"{bucket_prefix}" : ""
            }
        }
        try:
            s3_bucket_path = self.fileHelper.get_base_s3_bucket_path()
            try:
                if str(os.listdir(s3_bucket_path).count(bucket_name)).startswith('0'):
                    print(f'The Bucket You Requested Does not Exists')

                retrieve_valid_bucket = list(filter(lambda  x : x.__contains__(bucket_name),os.listdir(s3_bucket_path))).pop()

                valid_file_path  = os.path.join(s3_bucket_path, retrieve_valid_bucket)

                try:
                    split_prefix = bucket_prefix.split('/') if isinstance(bucket_prefix,str) and len(bucket_prefix) > 0 else ''
                    print(f'Fetching the Object at the {split_prefix} Going to depth of {len(split_prefix)}')
                    for index , item in enumerate(split_prefix):
                        file_list = os.listdir(valid_file_path)
                        for files in file_list:
                            current_path = os.path.join(valid_file_path,files)

                            if  current_path.endswith('.json'):
                                print(f'Skipping Bucket Info JSON')
                                continue

                            if os.path.isdir(current_path):
                                depth_level += 1
                                dir_size = get_folder_size(current_path)
                                is_dir_empty = str(dir_size).startswith('0')
                                if is_dir_empty:
                                    print('The Directory is Empty')
                                    continue

                                for item in os.listdir(current_path):
                                    depth_level += 1
                                    updated_path = os.path.join(current_path,item)
                                    if os.path.isdir(updated_path) and get_folder_size(updated_path) > 0:

                                        final_path_array = os.listdir(updated_path)
                                        final_path = os.path.join(updated_path,final_path_array[0])

                                        if final_path.endswith('.json') or final_path.endswith('.csv'):

                                            if 'Contents' in response:
                                                response['Contents'][f'{bucket_prefix}'] = final_path
                                                break


                    print(response)


                except (ValueError,TypeError) as data_error:
                    raise Exception(data_error)

                except Exception as sub_error:
                    raise Exception(sub_error)




            except AWSListObjectException as aws_error:
                raise Exception(aws_error)

        except Exception as error:
            print(f'Error listing Object From {bucket_name} with the Prefix : {bucket_prefix}, Error : {error}')




    def delete_objects(self,bucket_name,key_prefix):
        try:
            depth_level = 0
            s3_bucket_path = self.fileHelper.get_base_s3_bucket_path()
            if os.listdir(s3_bucket_path).count(bucket_name) > 0:
                try:
                    bucket_path = os.path.join(s3_bucket_path,bucket_name)
                    if os.path.isdir(bucket_path):
                        split_prefix = key_prefix.strip().split('/')
                        base_url = f'{bucket_path}'
                        for index , prefix in enumerate(split_prefix):
                            depth_level = index + depth_level
                            base_url += f'/{prefix}'

                        get_valid_size = get_folder_size(base_url)

                        if get_valid_size == 0:
                            print(f'The {base_url} Does not Contain Anything Deleting it')
                            return

                        self.fileHelper.delete_all_items(base_url)
                        return
                except AWSListObjectException as aws_error:
                    raise Exception(aws_error)


        except Exception as error:
            print(f'Error while deleting object from the {key_prefix}')



    def put_objects(self,bucket_name,bucket_prefix,item):
        try:
            depth_level = 0
            s3_bucket_path = self.fileHelper.get_base_s3_bucket_path()
            if os.listdir(s3_bucket_path).count(bucket_name) > 0:
                try:
                    bucket_path = os.path.join(s3_bucket_path,bucket_name)
                    if os.path.isdir(bucket_path):
                        split_prefix = bucket_prefix.strip().split('/')
                        base_url = f'{bucket_path}'
                        for index , prefix in enumerate(split_prefix):
                            depth_level = index + depth_level
                            base_url += f'/{prefix}'

                        get_valid_size = get_folder_size(base_url)
                        valid_put = self.fileHelper.select_format_and_put_objects(base_url,item)
                        return f'Object Inserted Successfully on {bucket_prefix}' if valid_put else f'Object Inserted Failure on {bucket_prefix}'
                except AWSPutObjectException as aws_error:
                    raise Exception(aws_error)


        except Exception as error:
            print(f'Error Putting the Object on the {bucket_prefix} , Error : {error}')





    def get_objects(self,bucket_name,bucket_prefix):
        response = {
            "Contents":{
                "Keys": []
            }
        }
        try:
                depth_level = 0
                s3_bucket_path = self.fileHelper.get_base_s3_bucket_path()
                if os.listdir(s3_bucket_path).count(bucket_name) > 0:
                    try:
                        bucket_path = os.path.join(s3_bucket_path,bucket_name)
                        if os.path.isdir(bucket_path):
                            split_prefix = bucket_prefix.strip().split('/')
                            base_url = f'{bucket_path}'
                            for index , prefix in enumerate(split_prefix):
                                depth_level = index + depth_level
                                base_url += f'/{prefix}'
                            response['Contents']['Keys'].extend(os.listdir(base_url))
                            return response
                    except AWSPutObjectException as aws_error:
                        raise Exception(aws_error)

        except Exception as error:
            print(f'Error Putting the Object on the {bucket_prefix} , Error : {error}')



    def generate_pre_signed_url(self,bucket_name:str,prefix:str) -> dict:
        mock_payload = {
            "pre_signed_url" : ""
        }
        deep_folder = 0
        s3_bucket_path = self.fileHelper.get_base_s3_bucket_path()
        prefix_str = f'{prefix.strip()}'
        try:
            is_valid_dir = len(os.listdir(s3_bucket_path)) > 0
            if is_valid_dir:
                is_valid_bucket  = list(filter(lambda x : len(x) > 0 and x == bucket_name,os.listdir(s3_bucket_path))).pop()

                if not is_valid_bucket:
                    print('The Bucket Does not Exists, Please Enter the Valid Bucket Name')

                valid_bucket_path = os.path.join(s3_bucket_path,is_valid_bucket)


                print(f'Going Inside the Bucket {valid_bucket_path}')
                deep_folder += 1
                for index , folder in enumerate(os.listdir(valid_bucket_path)):
                    if len(folder.strip()) > 0 and prefix_str.__contains__(folder):
                        prefix_array = [prefix_str,folder]
                        prefix_str = '/'.join(prefix_array)


                    if os.path.isdir(folder) and get_folder_size(os.path.join(valid_bucket_path,folder)) > 0:
                        deep_folder += 1
                        for index , sub_folder in enumerate(os.listdir(os.path.join(valid_bucket_path,folder))):
                            if len(sub_folder.strip()) > 0 and prefix_str.__contains__(sub_folder):
                                prefix_array = [prefix_str,sub_folder]
                                prefix_str = '/'.join(prefix_array)
                                break
                            else:
                                print(f'The Prefix {prefix_str} Does not Exists in the {sub_folder} , Please Enter the Valid Prefix')
                                break
                        break

            json_files = os.listdir(prefix_str).pop()
            mock_sha = hashlib.sha256(json_files.encode()).hexdigest()
            prefix_str = f'{prefix_str}/{mock_sha}?ExpiresIn=3600'
            if 'pre_signed_url' in mock_payload :
                mock_payload['pre_signed_url'] = prefix_str

            return mock_payload

        except Exception as error:
            print(f'Error Generating the Pre-Signed URL for the prefix {prefix} , Error : {error}')