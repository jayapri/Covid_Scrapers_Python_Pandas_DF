sys.path.insert(0, joinpath(dirname(dirname(abspath(__file__)))))
import helpers as hp
import requests
import pandas as pd
import re

logger = hp.get_logger(__name__)
from collections import OrderedDict


def checkKey(dict, key):
    if key in dict.keys():
        return True
    else:
        return False


def convertStrtoAry(inputStr):
    strAry = []
    if ' / ' in inputStr:
        strAry = re.split(" / ", inputStr)
    elif ' ' in inputStr:
        strAry = re.split(" ", inputStr)
    else:
        strAry.append(inputStr)
    strAry = list(map(str.strip, strAry))
    return strAry


def processData(Helpline_last_data, fileName):
    #  print(Helpline_last_data)

    records = []
    # Loading helpline data from api
    hlApiResponse = requests.get("https://life-api.coronasafe.network/data/helpline_v2.json").json()
    helplineData = hlApiResponse["data"]

    for i in range(len(helplineData)):
        # checking state having value
        if checkKey(helplineData[i], "state") and helplineData[i]["state"] != '':
            # checking any phone number having value
            isSendRecord = False
            if checkKey(helplineData[i], "phone_1") and helplineData[i]["phone_1"] != '':
                isSendRecord = True
            if checkKey(helplineData[i], "phone_2") and helplineData[i]["phone_2"] != '':
                isSendRecord = True
            if isSendRecord:
                isSendRecord = False
                if checkKey(helplineData[i], "created_on") and helplineData[i]["created_on"] != '':
                    isSendRecord = True
                elif checkKey(helplineData[i], "last_verified_on") and helplineData[i]["last_verified_on"] != '':
                    isSendRecord = True
            if isSendRecord:
                # checking category having value
                category = ''
                if checkKey(helplineData[i], "category") and helplineData[i]["category"] != '':
                    category = helplineData[i]["category"]
                else:
                    category = fileName
                district = ''
                if checkKey(helplineData[i], "district") and helplineData[i]["district"] != '':
                    district = helplineData[i]["district"]
                phoneList = []
                if checkKey(helplineData[i], "phone_1") and helplineData[i]["phone_1"] != '':
                    phoneList = convertStrtoAry(helplineData[i]["phone_1"])
                if checkKey(helplineData[i], "phone_2") and helplineData[i]["phone_2"] != '':
                    phoneList = convertStrtoAry(helplineData[i]["phone_2"])

                addedOn = ''
                if checkKey(helplineData[i], "created_on") and helplineData[i]["created_on"] != '':
                    addedOn = helplineData[i]["created_on"]
                modifiedOn = ''
                if checkKey(helplineData[i], "last_verified_on") and helplineData[i]["last_verified_on"] != '':
                    modifiedOn = helplineData[i]["last_verified_on"]
                description = ''
                if checkKey(helplineData[i], "title") and helplineData[i]["title"] != '':
                    description = 'Title: ' + helplineData[i]["title"] + ' '
                if checkKey(helplineData[i], "description") and helplineData[i]["description"] != '':
                    description += 'Description: ' + helplineData[i]["description"]
                if checkKey(helplineData[i], "Description") and helplineData[i]["Description"] != '':
                    description += 'Description: ' + helplineData[i]["Description"]
                params = {
                    "description": description,
                    "category": category,
                    "state": helplineData[i]["state"],
                    "district": district,
                    "phoneNumber": phoneList,
                    "addedOn": addedOn,
                    "modifiedOn": modifiedOn
                }

                print(params)
                try:
                    response = hp.send(params)
                    records.append(params)
                except Exception as e:
                    hp.print_error(e)

    pd.DataFrame(records).to_csv(fileName + "Data.csv")
    # if(Helpline_last_data):
    #     records.append(Helpline_last_data)

    return records


def run():
    Helpline_last_data = hp.get('Helpline_prev_data')

    results = processData(Helpline_last_data, "Helpline")

    hp.save('Helpline_prev_data', results)


#    ambulanceData = requests.get("https://life-api.coronasafe.network/data/ambulance_v2.json").json()
#    processData(ambulanceData["data"], "Ambulance")

#    foodData = requests.get("https://life-api.coronasafe.network/data/food_v2.json").json()
#    processData(foodData["data"], "Food")

if __name__ == "__main__":
    run()
