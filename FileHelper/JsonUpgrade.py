import json
import pandas as pd

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