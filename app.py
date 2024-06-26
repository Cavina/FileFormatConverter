import glob
import os 
import json
import re
import pandas as pd

'''
Schema example:

{
    "departments": [
        {
            "column_name": "department_id",
            "data_type": "integer",
            "column_position": 1
        },
        {
            "column_name": "department_name",
            "data_type": "string",
            "column_position": 2
        }
    ],.
'''


'''
def get_column_names -> list[str]
    Inputs -> A JSON schema, a dataset name, sorting key
    Outputs -> A list of the column names as strings
Retrieves the column names from a JSON schema.
Sorts based on the key passed.
'''
def get_column_names(schemas, ds_name, sorting_key='column_position') -> list[str]:
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file, schemas) -> pd.DataFrame:
    file_path_list = re.split('[/\\\]'. file)
    ds_name = file_path_list[-2]
    file_name = file_path_list[-1]
    columns = get_column_names(schemas, ds_name)
    return pd.read_csv(file, names=columns)

def to_json(df, tgt_base_dir, ds_name, file_name):
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{ds_name}', exist_ok=True)
    df.to_json(
        json_file_path,
        orient='records',
        lines=True
    )