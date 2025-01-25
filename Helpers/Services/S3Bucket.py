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


    def test_s3(self):
        return 'working s3 bucket'
