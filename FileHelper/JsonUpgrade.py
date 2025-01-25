import json
import os.path

import pandas as pd
from datetime import datetime
from Utils.DataUtils import get_folder_size

class JSONUpgrade:

    def __init__(self):
        pass


    def parse_json(self,file_path):
        try:
            with open(file_path,'r',encoding='utf-8') as content:
                data_content = json.load(content)
                json_df = pd.json_normalize(data_content,sep='_',errors='ignore')
                return json_df
        except json.decoder.JSONDecodeError as json_error:
            print(f'Error while decoding the JSON Objects, Due to : {json_error}')

        except Exception as error:
            print(f'An Un-expected error has been occur : Error : {error}')



    def create_json_file_bucket_name(self,bucket_name,file_path):
        bucket_json = os.path.join(file_path,f'{bucket_name}.json')
        try:
            with open(bucket_json,'w',encoding='utf-8') as file:
                    extracted_json = self.extract_structure()
                    if 'Bucket_Name' not in extracted_json and 'Bucket_Size' not in extracted_json:

                        extracted_json['Bucket_Name'] = bucket_name
                        extracted_json['Bucket_Size'] = get_folder_size(file_path)

                        json.dump(extracted_json, file, ensure_ascii=False, indent=4)

        except Exception as error:
            print(f'Error Expected while trying to create the json file for the bucket name')
            return

        else:
            print('The Bucket Information Has been Dumped Successfully')
            return




    def extract_structure(self):
        json_struct = {
            "Bucket_Created_At":datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return json_struct