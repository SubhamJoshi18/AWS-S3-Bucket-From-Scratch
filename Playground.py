import os.path

from MainAWS.AWS import AWS
# Try How AWS Internally Work






if __name__ == "__main__":
    aws_instance = AWS(service_name='s3',aws_secret_access_key='b03be2ed1b17c030496413c69370019bf461da7a',aws_secret_access_id='31921fcdb41385221ce69060cb28bd22')
    s3_client = aws_instance.create_connection()
    result = s3_client.download_buckets('pim-etl-export-dev','Export/UnifiedEtl',os.path.join(os.getcwd(),'TestData'))

