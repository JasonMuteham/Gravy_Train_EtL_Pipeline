import requests
import pandas as pd
import logging
from tenacity import retry, stop_after_attempt, TryAgain, wait_fixed

"""
Pandas has more features for excel & csv and will extract from api calls
so these helper functions are not used instead of Duckdb when required
"""

def excel(excel_url, excel_workbook, excel_skiprows, excel_usecols):
    df = pd.DataFrame()
    def get_excel(excel_url, excel_workbook, excel_skiprows, excel_usecols):
        try:
            return pd.read_excel( excel_url, 
                                excel_workbook, 
                                skiprows=excel_skiprows,
                                usecols=excel_usecols) 
        except Exception as e:
            logging.error(f'- get_excel: {excel_url}')        
            logging.error(f'- get_excel: {e}')
            raise TryAgain
    try:
        df = get_excel(excel_url, excel_workbook, excel_skiprows, excel_usecols)
    except TryAgain as e:
        logging.critical(f'- get_excel: Retry limit reached: {excel_url}')
    else:
        logging.info(f'- get_excel: Data extracted from excel: "{excel_url}" "{excel_workbook}"')
        logging.info(f'- get_excel: Data shape: {df.shape}')

    return df

def csv(url:str):
    df = pd.DataFrame()
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(2))
    def read_csv(url):
        try:
            return pd.read_csv(url)
        except Exception as e:
            logging.error(f'- get_csv: {url}')        
            logging.error(f'- get_csv: {e}')
            raise TryAgain
        
    try:
        df = read_csv(url)
    except TryAgain as e:
        logging.critical(f'- get_csv: Retry limit reached: {url}')
    else:
        logging.info(f'- get_csv: Data extracted from csv: "{url}"')
        logging.info(f'- get_csv: Data shape: {df.shape}')
    return df
                           
def csv_filelist(url: str):
    df1 = pd.DataFrame()
    filelist = csv(url)
    df1 = pd.DataFrame()
    for file in filelist['url']:
        df2 = csv(file)
        if df2 is not None:
            df1 = pd.concat([df1,df2])
    return df1

def csv_raw(url: str):
    """
    Extract raw data from a URL

    Parameters
    ----------
    url : str
        URL to extract data from

    """
    try:
        # Perform query
        csv_req = requests.get(url)
        # Parse content
        url_content = csv_req

        return url_content
    except requests.exceptions.HTTPError as errh:
        logging.critical(f'get_csv: {errh}')
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
        logging.critical(f'get_csv: {errc}')
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
        logging.critical(f'get_csv: {errt}')        
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)
        logging.critical(f"OOps: Something Else {err}")

def sqlfilter(conn, df_upload, sqlfilter):

        try:
            df_upload = conn.sql(sqlfilter).df()
        except Exception as e:
            logging.error(f"- Dataframe filter error: {e}")
            logging.error(f"- SQL Filter: {sqlfilter}")
            print(e)   
            return
        logging.info(f"- SQL filter Result: {df_upload.shape}")
        return df_upload