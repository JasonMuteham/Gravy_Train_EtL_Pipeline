from pathlib import Path
import requests
import pandas as pd
import logging
import get
from tenacity import retry, stop_after_attempt, wait_fixed, TryAgain

def get_mps_data(parms):
    mp_csv = parms['mp_csv']
    expense_csv = parms['expenses_csv']
    expense_folder = parms['expenses_folder']
    logging.info(f"- custom.get_mps_data: {parms}")

    try:
        Path(expense_folder).mkdir(exist_ok=True)
    except Exception as e:
        logging.critical(f"- custom.get_mps_data: {e}")
        print(e)
        raise 

#   get_mps(mp_csv, expense_csv, expense_folder)
    get_mps_json(mp_csv, expense_csv, expense_folder)    

def get_mps_json(mp_csv=None, expense_csv=None, expense_folder=None):
    take = 20
    skip = 0
    #total_mps = 0
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(2))
    def get_total_mps():
    # Get total mps on parliament database
    # This url will return no mps but will return a total count
        url = "https://members-api.parliament.uk/api/Members/Search?"

        params = {'House': 1, 'skip': 10000, 'take': take, 'IsEligible':'true', 'IsCurrentMember': 'true'}
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
    expenses_url = []
    url = 'https://members-api.parliament.uk/api/Members/Search?'
    params = {'House': 1, 'skip': skip, 'take': take, 'IsEligible':'true', 'IsCurrentMember': 'true'}

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
            logging.info(f"- custom.get_mps_json: {df["value_id"].count()} mps extracted")
            for id in df_mp["value_id"]:
                try:
                    expense_df = get.csv('https://www.theipsa.org.uk/api/mp/expenses?mpId=' + str(id))
                except Exception as e:
                    logging.error(f"- custom.get_mps_json: {e}")
                else:
                    expenses_url.append('https://www.theipsa.org.uk/api/mp/expenses?mpId=' + str(id))
                    expense_df.to_csv(f'{expense_folder}/expenses_{str(id)}.csv', index=False)

    logging.info(f"- custom.get_mps_json: {df.shape}")
    if mp_csv is not None:
        logging.info(f"- custom.get_mps_json: Writing data locally to {mp_csv}")
        try:
            df.to_csv(mp_csv, index=False)
        except Exception as e:
            logging.error(f"- custom.get_mps_json: {e}")

    if expense_csv is not None:
        logging.info(f"- custom.get_mps_json: Writing expense urls locally to {expense_csv}")
        try:
            pd.DataFrame(expenses_url, columns=["url"] ).to_csv(expense_csv, index=False)
        except Exception as e:
            logging.error(f"- custom.get_mps_json: {e}")
    logging.info(f"- custom.get_mps_json: Writing expense .csv locally to {expense_folder}")
    return 

def get_mps(mp_csv=None, expense_csv=None, expense_folder=None):
    take = 20
    skip = 0
    total_mps = 0

    # Get total mps on parliament database
    # This url will return no mps but will return a total count


    url = "https://members-api.parliament.uk/api/Members/Search?House=1&IsEligible=true&IsCurrentMember=true&skip=10000&take=20"
    r = requests.get(url)
    json_data = r.json()
    total_mps = json_data["totalResults"]
    logging.info(f"- custom.get_mps: {total_mps} mps")

    df = pd.DataFrame()
    expenses_url = []

    while skip <= total_mps:
        url = 'https://members-api.parliament.uk/api/Members/Search?House=1&IsEligible=true&IsCurrentMember=true&skip='+str(skip)+ '&take='+str(take)  # noqa: E501

        skip += take
        logging.info(f"- custom.get_mps: {skip} mps extracted")
        r = requests.get(url)
        json_data = r.json()

        for f in range(len(json_data["items"])):
            temp = json_data["items"][f]["value"]
            mydict = {}
            mydict["id"] = temp["id"]
            mydict["name"] = temp["nameDisplayAs"]
            mydict["full_title"] = temp["nameFullTitle"]
            mydict["gender"] = temp["gender"]
            mydict["party_id"] = temp["latestParty"]["id"]
            mydict["party_name"] = temp["latestParty"]["name"]
            mydict["constituency"] = temp["latestHouseMembership"]["membershipFrom"]
            mydict["constituency_id"] = temp["latestHouseMembership"]["membershipFromId"]
            mydict["start_date"] = temp["latestHouseMembership"]["membershipStartDate"]
            mydict["end_date"] = temp["latestHouseMembership"]["membershipEndDate"]
            mydict["end_reason"] = temp["latestHouseMembership"]["membershipEndReason"]

            if temp["latestHouseMembership"]["membershipStatus"] is None:
                mydict["membership_active"] = False
                mydict["membership_description"] = "Non Member"
            else:
                mydict["membership_active"] = temp["latestHouseMembership"]["membershipStatus"]["statusIsActive"]
                mydict["membership_description"] = temp["latestHouseMembership"][
                "membershipStatus"
            ]["statusDescription"]


            df1 = pd.DataFrame.from_records([mydict])
            df = pd.concat([df, df1])

            expenses_url.append('https://www.theipsa.org.uk/api/mp/expenses?mpId=' + str(mydict["id"]))
            expense_df = get.csv('https://www.theipsa.org.uk/api/mp/expenses?mpId=' + str(mydict["id"]))
            expense_df.to_csv(f'{expense_folder}/expenses_{str(mydict["id"])}.csv', index=False)


    logging.info(f"- custom.get_mps: {df.shape}")
    if mp_csv is not None:
        logging.info(f"- custom.get_mps: Writing data locally to {mp_csv}")
        try:
            df.to_csv(mp_csv, index=False)
        except Exception as e:
            logging.error(f"- custom.get_mps: {e}")

    if expense_csv is not None:
        logging.info(f"- custom.get_mps: Writing expense urls locally to {expense_csv}")
        try:
            pd.DataFrame(expenses_url, columns=["url"] ).to_csv(expense_csv, index=False)
        except Exception as e:
            logging.error(f"- custom.get_mps: {e}")
    logging.info(f"- custom.get_mps: Writing expense .csv locally to {expense_folder}")
    return 
