#!/usr/bin/env python
# coding: utf-8

# In[6]:


from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.auth import impersonated_credentials, default
import pickle
from datetime import datetime;
import pytz
import os
import os.path
from pathlib import Path

#USER SPECIFICATIONS - INFO ABT SPREADSHEET
#assumption: a column is dedicated for the exchange
SPREADSHEET_ID = '1RNAo9JvtyNMQWrUKLaSmPVxNvSpHjw1ryWMv8KRl0hc' #'17U2ecbkYX4Ul1B1Kcz0O_37UiyKOs423pgzzeBAbQZw' ##'#
SHEET_NAME = 'KG Holdings Master'
STARTING_ROW_NUM = '3'
ENDING_ROW_NUM = '136'

TICKER_COLUMN = 'A'
EXCHANGE_COLUMN = 'B'
PREV_CLOSE_COLUMN = 'E'
DAY_MIN_COLUMN = 'G'
DAY_MAX_COLUMN = 'F'
TIME_STAMP_COLUMN = 'C'
STATUS_MARKER_COLUMN = 'D'

WRITE_START_COLUMN = 'C'
WRITE_END_COLUMN = 'G'



#CUSTOM CONSTANTS
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly https://www.googleapis.com/auth/spreadsheets'

TICKER_EXCHANGE_RANGE = SHEET_NAME+'!'+TICKER_COLUMN+STARTING_ROW_NUM+':'+TIME_STAMP_COLUMN+ENDING_ROW_NUM
WRITE_RANGE_NAME = SHEET_NAME+'!'+WRITE_START_COLUMN+STARTING_ROW_NUM+':'+WRITE_END_COLUMN+ENDING_ROW_NUM

def main():
    
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            #user_cred, _ = default()
            #credentials = Credentials(user_cred, sa, scopes)
            #credentials._source_credentials._scopes = user_cred.scopes
            
            if Path('token.json').exists():
                os.remove('token.json')
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret_1018816297994-fil2doubov5rppaeqtpmjmcv2788ak6s.apps.googleusercontent.com.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    
    
    
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    
    
    #READ TICKERS, EXCHANGES, TIMESTAMPS
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=TICKER_EXCHANGE_RANGE).execute()
    tickerExchangeList = result.get('values', [])

    if not tickerExchangeList:
        print('No data found.')
    else:
        writeValues = []
        
        classNames = ["hrc1Nd", "M2CUtd"]
        labels = ["Previous close", "Day range"]
        print('Ticker, Exchange:')
        for tickerExchange in tickerExchangeList:
            print(tickerExchange)
            values = retrieveData(tickerExchange[0], tickerExchange[1], classNames, labels, 3, "₹")
            
            #UPDATE STATUS & TIMESTAMP
            if '' in values:
                values.insert(0,"ERR")
                try:
                    values.insert(0, tickerExchange[2])
                except:
                    values.insert(0, "")
            else:
                values.insert(0,"OK")
                IST_timezone = pytz.timezone("Asia/Kolkata")
                IST_timestamp = datetime.now(IST_timezone)
                timestamp = str(IST_timestamp)
                values.insert(0, timestamp)
                
            writeValues.append(values)
        
        
        #WRITE TIMESTAMP, STATUS, PREV CLOSE, MAX, MIN TO SHEETS
        body = {
                'values': writeValues
            }
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=WRITE_RANGE_NAME,
            valueInputOption='USER_ENTERED', body=body).execute()

    

if __name__ == '__main__':
    main()


# In[1]:


import numpy as np
from bs4 import BeautifulSoup
import requests
import pandas as pd
import pprint


def getURL(ticker, exchange):
    template = "https://g.co/finance/"
    url = template+ticker+":"+exchange
    return url

def scrapePage(url):
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")
    return soup;

#labels = [label1, label2]
def getValue(labelList, valueList, labels):
    values = []
    
    for label in labels:
        labelIndex = labelList.index(label)
        value = valueList[labelIndex]
        values.append(value)
        
    return values;

#classNames = [className1, className2]
#labels = [label1, label2]
def retrieveData(ticker, exchange, classNames, labels, numValsToReturn, currencySymbol):
    
    try:
        url = getURL(ticker, exchange)
        html = scrapePage(url)

        classLists = []

        for className in classNames:
            classItems = html.findAll(class_=className)
            classItemsText = []
            for item in classItems:
                classItemsText.append(item.text)
            classLists.append(classItemsText)
        
        values = getValue(classLists[0], classLists[1], labels)
        
        for label in labels:
            if "range" in label:
                valueToSplit = values[labels.index(label)]
                splitValues = valueToSplit.split(" - ")
                values.remove(valueToSplit)
                values.append(splitValues[1])
                values.append(splitValues[0])
        
        values = [s.strip(currencySymbol) for s in values]
        values = [float(v.replace(',','')) for v in values]
        
    except:
        values = []
        i = 0
        while i<numValsToReturn:
            values.append("")
            i+=1
            
    return values;
            


ticker = "accelya"
exchange = "NSE"
classNames = ["hrc1Nd", "M2CUtd"]
labels = ["Previous close", "Day range"]
html = scrapePage(getURL(ticker, exchange))
#print(retrieveData(ticker, exchange, classNames, labels,3, "$"))

