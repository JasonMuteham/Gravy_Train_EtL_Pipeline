from pathlib import Path
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import logging
import json
from tenacity import retry, stop_after_attempt, wait_fixed, TryAgain

def get_constituency_data(params):
    const_csv = params['file_csv']
    const_folder = params['constituency_rep_folder']
    geo_folder = params['constituency_geo_folder']

    logging.info(f"- custom.get_constituency_data: {params}")

    try:
        Path(const_folder).mkdir(exist_ok=True)
    except Exception as e:
        logging.critical(f"- custom.get_constituency_data: {e}")
        print(e)
        raise 
    try:
        Path(geo_folder).mkdir(exist_ok=True)
    except Exception as e:
        logging.critical(f"- custom.get_constituency_data: {e}")
        print(e)
        raise 
    get_constituency_json(const_csv, const_folder=const_folder, geo_folder=geo_folder) 

def get_constituency_json(const_csv, const_folder=None, geo_folder=None):
    take = 20
    skip = 0
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(2))
    def get_total_const():
    # Get total constituency on parliament database
    # This url will return no constituency but will return a total count
        url = "https://members-api.parliament.uk/api/Location/Constituency/Search?"
        params = {'skip': 10000, 'take': take}
        try:
            r = requests.get(url, params=params)
        except Exception as e:
            logging.critical(f"- custom.get_constituency: {e}")
            #print(e)
            raise TryAgain
            return

        json_data = r.json()
        total_const = json_data["totalResults"]
        logging.info(f"- custom.get_mps_json: {total_const} constituency")
        return total_const

    try:
        total_const = get_total_const()
    except TryAgain:
        logging.critical('- custom.get_constituency_json: Retry limit reached')
        print("Oh No! pipe went out...")
        return

    df = pd.DataFrame()
    url = 'https://members-api.parliament.uk/api/Location/Constituency/Search?'
    params = {'skip': skip, 'take': take}
 
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(2))
    def read_const_api(url,params):
        try:
            return requests.get(url, params=params)
        except Exception as e:
            logging.critical(f"- custom.get_constituency_json: {e}")
            #print(e)
            raise TryAgain
    
    while skip <= total_const:
        try:
            r = read_const_api(url, params)
        except TryAgain:
            logging.critical('- custom.get_constituency_json: Retry limit reached')
            print("Oh No! pipe went out...")
            return
        else:    
            json_data = r.json()
            skip += take
            params["skip"] = skip
            df_const = pd.json_normalize(json_data["items"],sep='_')
            df = pd.concat([df, df_const])    
            logging.info(f"- custom.get_constituency_json: {df['value_id'].count()} constituency extracted")
            
            for id in df_const["value_id"]:
                const_rep_url = f'https://members-api.parliament.uk/api/Location/Constituency/{id}/Representations'
                try:
                    r = requests.get(const_rep_url)
                    rep_data = r.json()
                except Exception as e:
                        logging.error(f"- custom.get_constituency_json: {e}")
                else:
                    const_df = pd.json_normalize(rep_data["value"] ,sep='_')
                    if not(const_df.empty):
                        #const_df['constituency_id'] = id
                        const_df.drop(['member_links'], axis=1, inplace=True)
                        const_df.to_csv(f'{const_folder}/rep_{str(id)}.csv', index=False)
                
                geo_url = f'https://members-api.parliament.uk/api/Location/Constituency/{id}/Geometry'
                try:
                    #r = requests.get(geo_url)
                    #geo_data = r.json()
                    df_geo = pd.read_json(geo_url)
                except Exception as e:
                        logging.error(f"- custom.get_geometry_json: {e}")
                else:
                    if not(df_geo.empty):
                        df_geo['constituency_id'] = id
                        df_geo.to_json(f'{geo_folder}/geo_{str(id)}.json', orient='records')


    logging.info(f"- custom.get_constituency_json: {df.shape}")
    if const_csv is not None:
        logging.info(f"- custom.get_constituency_json: Writing data locally to {const_csv}")
        try:
            df.to_csv(const_csv, index=False)
        except Exception as e:
            logging.error(f"- custom.get_constituency_json: {e}")
    return 

def get_mps_data(params):
    mp_csv = params['mp_csv']
    history_folder = params['name_history_folder']
    logging.info(f"- custom.get_mps_data: {params}")

    try:
        Path(history_folder).mkdir(exist_ok=True)
    except Exception as e:
        logging.critical(f"- custom.get_mps_data: {e}")
        print(e)
        raise 
  
    get_mps_json(mp_csv, history_folder=history_folder)  

def get_mps_json(mp_csv, history_folder=None):
    take = 20
    skip = 0
    #total_mps = 0
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(2))
    def get_total_mps():
    # Get total mps on parliament database
    # This url will return no mps but will return a total count
        url = "https://members-api.parliament.uk/api/Members/Search?"

        #params = {'House': 1, 'skip': 10000, 'take': take}
        params = {'skip': 10000, 'take': take}
        try:
            r = requests.get(url, params=params)
        except Exception as e:
            logging.critical(f"- custom.get_mps: {e}")
            #print(e)
            raise TryAgain
            return

        json_data = r.json()
        total_mps = json_data["totalResults"]
        logging.info(f"- custom.get_mps_json: {total_mps} mps")
        return total_mps

    try:
        total_mps = get_total_mps()
    except TryAgain:
        logging.critical('- custom.get_mps_json: Retry limit reached')
        print("Oh No! pipe went out...")
        return

    df = pd.DataFrame()
    url = 'https://members-api.parliament.uk/api/Members/Search?'
    params = {'skip': skip, 'take': take}
 
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(2))
    def read_mps_api(url,params):
        try:
            return requests.get(url, params=params)
        except Exception as e:
            logging.critical(f"- custom.get_mps_json: {e}")
            #print(e)
            raise TryAgain
    
    while skip <= total_mps:
        try:
            r = read_mps_api(url, params)
        except TryAgain:
            logging.critical('- custom.get_mps_json: Retry limit reached')
            print("Oh No! pipe went out...")
            return
        else:    
            json_data = r.json()
            skip += take
            params["skip"] = skip
            df_mp = pd.json_normalize(json_data["items"],sep='_')
            df = pd.concat([df, df_mp])    
            logging.info(f"- custom.get_mps_json: {df['value_id'].count()} mps extracted")

            history_name_url = 'https://members-api.parliament.uk/api/Members/History'
            
            for id in df_mp["value_id"]:
                # Code to get historical names
                hist_params = {'ids': id}
                try:
                    rh = requests.get(history_name_url, params=hist_params)
                    hist_data = rh.json()
                except Exception as e:
                        logging.error(f"- custom.get_mps_json: {e}")
                else:
                    #print(hist_data)
                    history_df = pd.json_normalize(hist_data[0]["value"]['nameHistory'] ,sep='_')
                    if not(history_df.empty):
                        history_df['mp_id'] = id
                        history_df.to_csv(f'{history_folder}/name_history_{str(id)}.csv', index=False)


    logging.info(f"- custom.get_mps_json: {df.shape}")
    if mp_csv is not None:
        logging.info(f"- custom.get_mps_json: Writing data locally to {mp_csv}")
        try:
            df.to_csv(mp_csv, index=False)
        except Exception as e:
            logging.error(f"- custom.get_mps_json: {e}")

    return 

