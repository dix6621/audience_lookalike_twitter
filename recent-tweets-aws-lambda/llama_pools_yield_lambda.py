import json
from functions_s3 import s3_pd_read_csv, s3_write_json
from functions_llama import llama_pools_yield

import datetime
end_date = datetime.date.today() + datetime.timedelta(days = 1)

def lambda_handler(event, context):
    
    data_dict = {'pools_yield': llama_pools_yield()['data']}

    s3_write_json('dataspartan-test-bucket', f'defillama-analytics/raw-data/pools-yield/results/llama_pools_yield_{end_date}.json', data_dict)
