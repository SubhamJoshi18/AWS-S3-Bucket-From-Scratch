import pandas as pd
from Constants.Modules import AWS_REGION
from Exceptions.AWSException import AWSConnectionError
from FileHelper.FileUpgrade import FileUpgrade
from Utils.DataUtils import format_data
from Security.AWSSecurity import AWSSecurity
from Constants.Services import AWSService
from Helpers.Services.S3Bucket import S3Bucket


class AWS:
    def __init__(self,service_name,aws_secret_access_id,aws_secret_access_key,aws_region=AWS_REGION):
        self.service_name = service_name
        self.aws_secret_access_id = aws_secret_access_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region = aws_region
        self.awsSecurity = AWSSecurity()
        self.fileHelper = FileUpgrade()
        self.s3_client = S3Bucket()


    def create_connection(self):
        retry_count = 0
        retry_status = True


        while retry_status and retry_count < 5:
            try:
                json_content = self.fileHelper.retrieve_aws_config()
                credential_list = format_data(self.aws_secret_access_key,self.aws_secret_access_id)
                credential_list_df = pd.DataFrame(credential_list)
                is_valid_config = self.awsSecurity.validate_config(credential_list_df,json_content)

                if isinstance(is_valid_config,bool) and not is_valid_config:
                    raise AWSConnectionError('Connection is Invalid, Due to Wrong Parameter')
                service = self.manage_services(self.service_name)
                return service

            except AWSConnectionError as connection_error:
                print(f'Error Connecting to the AWS , Please Try again, Increasing the retry count {retry_count} by 1 ')
                retry_count += 1
                continue
        else:
            retry_status = False
            print(f'Maximum Retry Count is Exceeded , Please Try again Later :{retry_count}')


    def manage_services(self,service_name):
        match service_name.lower():
            case AWSService.s3_bucket:
                return self.s3_client
            case _:
                return

