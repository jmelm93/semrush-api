import requests
import csv
import io
import os
import logging
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Basic setup for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class SemrushApi:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv('SEMRUSH_API_KEY')
        self.base_url = "https://api.semrush.com/"
    
    def make_request(self, **kwargs):
        """Generic method to make requests to the SEMrush API with flexible parameters."""
        # Directly add the API key to kwargs, ensuring it's included in every request
        kwargs['key'] = self.api_key
        logging.info('Making request with params: {}'.format(kwargs))
        response = requests.get(self.base_url, params=kwargs)
        if response.status_code == 200:
            return response
        else:
            logging.error(f"Failed to fetch data from SEMrush API. Status Code: {response.status_code}")
            return None
    
    def process_csv_response(self, response, include_kwargs_in_output, **kwargs):
        """Convert a CSV response to a list of dictionaries."""
        content = io.StringIO(response.text)
        reader = csv.reader(content, delimiter=';')
        data = list(reader)
        keys = data[0]
        values = data[1:]
        processed_data = [dict(zip(keys, value)) for value in values]

        if include_kwargs_in_output:
            for item in processed_data:
                for key, value in kwargs.items():
                    # exclude "key" and "export_columns" from the output
                    if key not in ['key', 'export_columns']: 
                        item[f'api_{key}'] = value
        
        return processed_data

    def get_data(self, include_kwargs_in_output=False, **kwargs):
        """Generic method to get data from a specific SEMrush API endpoint."""
        response = self.make_request(**kwargs)
        if response:
            return self.process_csv_response(response, include_kwargs_in_output, **kwargs)
        else:
            return []


if __name__ == "__main__":
    job_type = os.getenv('JOB_TYPE')
    items = os.getenv('ITEMS').split(', ')
    api_key = os.getenv('SEMRUSH_API_KEY')
    semrush = SemrushApi(api_key)
    
    output = []
    for i in items:
        # https://developer.semrush.com/api/v3/analytics/url-reports/#url-organic-search-keywords
        if job_type == "url_organic":
            data = semrush.get_data(
                include_kwargs_in_output=True,
                type='url_organic',
                url=i,
                export_columns='Ph,Po,Nq,Tg,Tr,Tc,Nr',
                display_filter='Po<=6',
                display_limit=10,
                sort='tr_desc',
                database='us'
            )
            output.extend(data)
    
        elif job_type == "subfolder_rank_history":
            # https://developer.semrush.com/api/v3/analytics/subfolder-reports/#subfolder-overview-history
            data = semrush.get_data(
                include_kwargs_in_output=True,
                type='subfolder_rank_history',
                subfolder=i,
                export_columns='Or,Xn,Ot,Oc,Ad,At,Ac,Dt,FKn,FPn,Sr,Srb,St,Stb,Sc',
                display_daily=0, # if set to 1, you get daily (rather than monthly) data
                display_limit=12,
                database='us'
            )
            output.extend(data)

    df = pd.DataFrame(output)
    print(df.head())
    df.to_csv('output.csv', index=False)