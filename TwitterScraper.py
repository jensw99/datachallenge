import numpy as np
import requests
import os
import datetime
import json
import ast
import pandas as pd
import configparser
import time

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
import ExasolConnector


bearer_token = os.environ.get("BEARER_TOKEN")
search_url = "https://api.twitter.com/2/tweets/search/recent"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
# Date format: YYYY-MM-DDTHH:mm:ssZ
CONFIG_PATH = 'config.ini'

query_params_bahn = {'query': 'lang:de -is:retweet (#DeutscheBahn OR #Bahn OR #DB OR Bahn OR DB)',
                'tweet.fields': 'created_at,lang,public_metrics', 'max_results': 100}
query_params_flixbus = {'query': 'lang:de -is:retweet (#Flixbus OR Flixbus)',
                'tweet.fields': 'created_at,lang,public_metrics', 'max_results': 100}
query_params_ryanair = {'query': 'lang:de -is:retweet (#Ryanair OR Ryanair)',
                'tweet.fields': 'created_at,lang,public_metrics', 'max_results': 100}
query_params_auto = {'query': 'lang:de -is:retweet (#Auto OR Auto)',
                'tweet.fields': 'created_at,lang,public_metrics', 'max_results': 100}
query_params_oeffis = {'query': 'lang:de -is:retweet (#ÖPNV OR ÖPNV OR öffis OR Nahverkehr)',
                'tweet.fields': 'created_at,lang,public_metrics', 'max_results': 100}

def get_config():
    """
    Reads the config file
    :return: The paresd config
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config


def save_config(config):
    """
    Saves the config
    :param config: The config object
    :return: None
    """
    with open(CONFIG_PATH, 'w') as configfile:
        config.write(configfile)


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


def connect_to_endpoint(url, params):
    """
    Connects to url
    :param url: Url
    :param params: Parameter for request
    :return: Response
    """
    response = requests.get(url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def execute_query(query, config, company):
    """
    Executes a twitter query
    :param query: Query to be executed
    :param config: The config object
    :param company: The company
    :return: The response dataframe
    """
    start_at1 = (datetime.datetime.now(datetime.timezone.utc).astimezone() - datetime.timedelta(days=7)+datetime.timedelta(minutes=30))
    start_at2 = datetime.datetime.fromisoformat(config['times']['last_'+company])

    # Avoid duplicates by setting a start_time up to 7 days in the past
    query['start_time'] = max(start_at1, start_at2).isoformat()
    data = None
    json_response = connect_to_endpoint(search_url, query)
    data = pd.DataFrame(json_response['data'])
    while True:
        try:
            json_response = connect_to_endpoint(search_url, query)
        except:
            print('Number of results exceeded. Waiting 20 minutes now.')
            time.sleep(20*60)
            continue
        data_tmp = pd.DataFrame(json_response['data'])
        if json_response['meta'].get('next_token') is None:
            break
        query['next_token'] = json_response['meta']['next_token']
        # Concat responses
        data = pd.concat([data, data_tmp])
    query['next_token'] = None
    return data



def get_twitter_data(company, config):
    """
    Get twitter data for a company
    :param company: The company
    :param config: The config file
    :return: Twitter data of the company
    """
    df_data = None
    if company == 'bahn':
        df_data = execute_query(query_params_bahn, config, company)
    elif company == 'ryanair':
        df_data = execute_query(query_params_ryanair, config, company)
    elif company == 'auto':
        df_data = execute_query(query_params_auto, config, company)
    elif company == 'oeffis':
        df_data = execute_query(query_params_oeffis, config, company)
    else:
        df_data = execute_query(query_params_flixbus, config, company)

    # No newlines
    df_data = df_data.replace(r'\n', ' ', regex=True)

    # Expand json content
    df_json = pd.json_normalize(df_data['public_metrics'])
    df_json = df_json.reset_index(drop=True)

    df_data = pd.merge(df_data, df_json, left_index=True, right_index=True)

    df_data = df_data.rename(columns={
        'like_count': 'likes',
        'reply_count': 'replies'
    })
    df_data['id'] = df_data['id'].apply(lambda x: x + '_twitter')
    df_data['social_media'] = 'twitter'
    df_data['company'] = company
    df_data['rating'] = np.NaN
    df_data['created_at'] = pd.to_datetime(df_data['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    df_data = df_data[['id', 'likes', 'replies', 'social_media', 'rating', 'created_at', 'text', 'company']]
    df_data = df_data.drop_duplicates(('id'))

    return df_data


def main():
    config = get_config()
    companies = ['bahn', 'auto', 'flixbus', 'ryanair', 'oeffis']
    for company in companies:
        connector = ExasolConnector.ExasolConnector()
        connector.to_db(get_twitter_data(company, config), 'datachallenge')
        # Update last time in config
        config['times'][f'last_{company}'] = datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()
        if company == 'bahn':
            # Waiting after bahn to not exceed query response limit
            print('Waiting 16 mins')
            time.sleep(16*60)
    save_config(config)


if __name__ == "__main__":
    main()
