import requests, json
import pandas as pd
from utils.exceptions import MergerError
from utils.commons import Endpoint, JoinCriteria


import time


joins = [
            {"columns" : [{"service1" : "id", "service2": "id", "operator": "="}]}]

service_key = "C0136D2B612C2B78C61E63AD8957989111D96EDC7B4A3D613EE7467B1284B88E"



def join_dataframes(dataframes, join_criteria_list):
    result_df = dataframes[join_criteria_list[0].endpoint1_name].copy()

    for join_criteria in join_criteria_list:
        # Rename columns in both DataFrames to avoid conflicts
        endpoint1_df = result_df.copy()
        endpoint1_df = endpoint1_df.rename(columns={col: f"{join_criteria.endpoint1_name}_{col}" for col in endpoint1_df.columns})
        
        endpoint2_df = dataframes[join_criteria.endpoint2_name].copy()
        endpoint2_df = endpoint2_df.rename(columns={col: f"{join_criteria.endpoint2_name}_{col}" for col in endpoint2_df.columns})
        
        # Perform join
        result_df = pd.merge(left=endpoint1_df, right=endpoint2_df, how="inner", 
                             left_on=[f"{join_criteria.endpoint1_name}_{col}" for col in join_criteria.endpoint1_columns], 
                             right_on=[f"{join_criteria.endpoint2_name}_{col}" for col in join_criteria.endpoint2_columns])
    
    return result_df



def rename_join_col(join_criteria):
    do_rename_join_cols(join_criteria, join_criteria.endpoint1_columns, join_criteria.endpoint1_name)
    do_rename_join_cols(join_criteria, join_criteria.endpoint2_columns, join_criteria.endpoint1_name)

def do_rename_join_cols(join_criteria, columns, prefix):
    i = 0
    for col in columns:
        columns[i]=f"{prefix}_{col}"
        i+=1

def rename_df_joined_columns (dataframe, cols, prefix):
    return dataframe.rename(columns={col: f"{prefix}_{col}" for col in cols})


def join_end_points (sample_request):
    endpoint_array, join_request_array  = fabric_join_request_object(sample_request) 
    data_frames = get_services_dataframes(endpoint_array)
    return join_dataframes(data_frames,join_request_array)

def call_service (endpoint):
    json_data = do_call_service(url=endpoint.url, params=endpoint.params, data=endpoint.body, headers=endpoint.headers)
    # Extract 'records' attribute
    records = json_data['Response']['records']
    # Create DataFrame using json_normalize
    return pd.json_normalize(records)

def do_call_service(url, params=None, data=None, headers=None): 
    json_data = json.dumps(data)
    response = requests.post(url, headers=headers, params=params, data=json_data)
    if response.status_code==200:
        return response.json()
    raise MergerError(response.status_code, response.text)
        
def get_services_dataframes(endpoints):
    endpoints_responses = {}
    for endpoint in endpoints:
        dataframe = call_service(endpoint)
        endpoints_responses[endpoint.name] = dataframe   
    return endpoints_responses


def fabric_join_request_object(request_data):
    try:
        None
        if 'endpoints' not in request_data or 'join_criteria' not in request_data:
            raise ValueError("Invalid request structure")
        # Parse endpoints
        endpoints = [Endpoint(**endpoint) for endpoint in request_data['endpoints']]
        # Parse join criteria
        join_criteria = [JoinCriteria(**criteria) for criteria in request_data['join_criteria']]
        return endpoints, join_criteria
    except json.JSONDecodeError as e:
        None
    except Exception as e:
        None



sample_request = {
    "endpoints": [
        {
            "url": "https://stagingpulse.anuvu.com:8885/engineWebServices/StarlinkFacilities",
            "name": "facilities",
            "body": {'search': 'BRA-PBR-PLATFORMA'},
            "params" : {'pageSize': 2000, 'page_number': 0},
            "headers": {"Content-Type": "application/json", 'api-key' : service_key, 'Content-Type' : 'application/json'}
        },
        {
            "url": "https://stagingpulse.anuvu.com:8885/engineWebServices/MaritimeCustomers",
            "name": "customers",
            "body": {'search': 'PETROLEO BRASILEIRO'},
            "params" : {'pageSize': 2000, 'page_number': 0},
            "headers": {"Content-Type": "application/json", 'api-key' : service_key, 'Content-Type' : 'application/json'}
        }
    ],
    "join_criteria": [
        {
            "endpoint1_name": "facilities",
            "endpoint1_columns": ["customerId"],
            "endpoint2_name": "customers",
            "endpoint2_columns": ["id"]
        }
    ]
}

def main(request_data):
    return join_end_points(request_data)
    

start_time = time.time()
main(sample_request)
end_time = time.time()

execution_time = end_time - start_time
print(f"Execution time: {execution_time:.2f} seconds")


