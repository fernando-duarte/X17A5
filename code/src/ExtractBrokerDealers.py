#!/usr/bin/env python
# coding: utf-8

"""
ExtractBrokerDealers.py: Responsbile for parsing in dealer information with 
accompaning CIK code from archived dealer data dating back to 1993 in the SEC
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

# console and directory access
import os
import re
import time
import datetime

# structured data reading
import pandas as pd

# parsing SEC website for data  
import requests 
from bs4 import BeautifulSoup


##################################
# USER DEFINED FUNCTIONS
##################################

def companyName(cik:str) -> str:
    """
    Returns the company name for a given CIK number from 
    the SEC by parsing the EDGAR site for the current name 
    
    Parameters
    ----------
    cik : str
        The CIK number for a broker dealer e.g. 887767
    """
    
    # establishing base-url for company name search
    baseURL = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&'
    current_year = datetime.datetime.today().year
    url = baseURL+'CIK={}&type=X-17A-5&dateb={}1231'.format(cik, current_year)
    
    # we try requesting the URL and break only if response object returns status of 200 (i.e. success)
    for _ in range(20):
        res = requests.get(url, allow_redirects=True)
        time.sleep(1)
        if res.status_code == 200: 
            break
    
    # last check to see if response object is "problamatic" e.g. 403 after 10 tries
    if res.status_code != 200:
        print('\t\tERROR: Unable to retrieve response from %s, response object %d' % (cik, res.status_code))
        return None
    
    # parse HTML through BeautifulSoup object
    s1 = BeautifulSoup(res.text, 'html.parser')
    
    # select the company information from the SEC website for a particular CIK
    for val in s1.find_all('span', attrs={"class":"companyName"}):
        
        # retrieve the company name from info class in HTML
        comp_name = val.text.split('CIK')[0].split('/BD')[0]
        print('\t\tFound CIK %s, company-name %s' % (cik, comp_name))
        
        return comp_name

def dealerData(years:list, quarters:list=['QTR1', 'QTR2', 'QTR3', 'QTR4'], 
               cik2brokers:dict={'years-covered':[], 'broker-dealers':{}}) -> dict:
    """
    Retrieve dealer data from archived SEC directory, returns 
    a dictionary of CIK to Company Name mappings
    
    Parameters
    ----------
    years : list
        A list of years to check for additional dealer data to be 
        pulled e.g. [1993, 1994, 2000]. NOTE, that only the years 
        specified are checked for dealer information. 
        
    quarters : list
        A list of quarters to check for additional dealer data, 
        string must be of the form "QTRX", where X is an integer
        from 1-4 inclusive default = [QTR1, QTR2, QTR3, QTR4]. 
        
    cik2brokers : dict
        A nested dictionary for storing the broker-dealer data as 
        well as the years covered from the archive, we provide default
        template in the event no  
        e.g. {'years-reported':['2020/QTR1', '2020/QTR2'], 
        'broker-dealers':{782124: 'J.P. MORGAN SECURITIES LLC'}}. 
    """
    
    # archived data website for broker dealer data
    baseURL = 'https://www.sec.gov/Archives/edgar/full-index'
    
    # extract all the years covered from json form (we want to avoid uneccesary re-runs) 
    archiveDates = ['{}/{}'.format(yt, qt) for yt in years for qt in quarters]
    years_covered = cik2brokers['years-covered']
    
    print('EXTRACTING BROKER-DEALER INFORMATION')
    # itterate through years and quarters for archival search
    for coverage in archiveDates:
        
        if coverage in years_covered:
            print('We have covered %s' % coverage)
            pass

        else:
            searchURL = '%s/%s/form.idx' % (baseURL, coverage)
            
            # we try requesting the URL and break only if response object returns status of 200
            for _ in range(20):
                response = requests.get(searchURL, allow_redirects=True)
                time.sleep(1)
                if response.status_code == 200: break
            
            # if reponse type is active we return object with status code 200 (else error)
            if response.status_code == 200:
                print('\nSearching for broker dealers at %s' % searchURL)
                
                # append the coverage year for the cik in question
                cik2brokers['years-covered'].append(coverage)
                print('  Adding coverage for %s' % coverage)

                # extract only main text from body, selecting terms below dashes '---' 
                # we use triple dashes to avoid improper splits that exist locally with company names
                data = response.text.split('---')[-1]   

                # write contents to a temporary file to read information
                with open('main.txt', 'w') as file: file.write(data)
                
                # convert text data to dataframe object using a fixed-width-file convention
                df = pd.read_fwf('main.txt', header=None)
                cleanDf = df[~pd.isnull(df[0])]            # strip away rows with NaN from the Form Type column   

                # check to see if first column contains information on X-17A-5 filings (use regex for x-17a flag)
                x17_check = cleanDf[0].str.contains('^x-17a', 
                                                    regex=True, 
                                                    flags=re.IGNORECASE)
                x17File = cleanDf[x17_check]

                print('\tFound %d X-17A-5 filings in %s' % (x17File.shape[0], coverage))

                if not x17File.empty:

                    # CIK number is taken from the last column of the rows splitting url string by row 
                    # e.g. edgar/data/886475/0001019056-10-000046.txt -> 886475
                    last_column = x17File.columns[-1]
                    cikNumbers = x17File[last_column].apply(lambda x: x.split('/')[2]).values
                    
                    # compute dictionary mapping for the CIK and company name for each broker-dealer    
                    dictionary_update = dict(map(lambda x: (x, companyName(x)), 
                                                 cikNumbers))
                    cik2brokers['broker-dealers'].update(dictionary_update)
                
                os.remove('main.txt')
                
    return cik2brokers

def update_dealer_names(cik2broker:dict) -> dict:
    """
    Updates all the company names present within the 
    broker-dealer dictionary of CIK : Company Names
    
    Parameters
    ----------
    cik2brokers : dict
        A nested dictionary for storing the broker-dealer 
        data as well as the years covered from the archive 
        e.g. {'years-reported':['2020/QTR1', '2020/QTR2'],
        'broker-dealers':{1904: 'ABRAHAM SECURITIES CORPORATION'}}.
    """
    
    cik = cik2broker['broker-dealers']
    print('\nUpdating all company names for %.d CIKs' % len(cik))
    
    # update the company names for existing CIKs from the broker dealer dictionary
    updated_names = dict(map(lambda x: (x, companyName(x)), cik))
    cik2broker['broker-dealers'] = updated_names

    return cik2broker
