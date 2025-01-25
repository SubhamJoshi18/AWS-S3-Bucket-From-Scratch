import os
from Exceptions.AWSException import AWSBucketCreationException
from Constants.Modules import BUCKET_FOLDER
from FileHelper.FileUpgrade import FileUpgrade
from datetime import datetime

from FileHelper.JsonUpgrade import JSONUpgrade


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
