#!/usr/bin/env python
# coding: utf-8

"""
OCRTextract.py: Responsbile for reducing the size of FOCUS reports
to be easily read by AWS Textract, due to file constraints
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################:

import re
import os
import trp
import time
import boto3
import minecart

import numpy as np
import pandas as pd

from smart_open import open


##################################
# USER DEFINED FUNCTIONS
##################################


"""
AWS Asynchronous Textract Script (requesting Job)

The functions below were modified from the AWS Textract repository 
refer to the following URL for greater granular detail on the functions
https://docs.aws.amazon.com/textract/latest/dg/what-is.html
"""

def startJob(s3BucketName:str, objectName:str) -> str:
    """
    Starts a Textract job on AWS server 
    """
    # initialize return and client object
    response = None                         
    client = boto3.client('textract')
    
    # issue response to AWS to start Textract job for table analysis 
    response = client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3BucketName,     # location of data to be read from s3 bucket 
                'Name': objectName}},       # file name to be read from Textract  
        FeatureTypes=['FORMS', 'TABLES']    # selecting FORMS (key-values) and TABLES from the OCR
    )
    
    # return response job ID for service
    return response["JobId"]

def isJobComplete(jobId:str) -> str:
    """
    Tracks the completion status of the Textract job when queued
    """
    # allow for interal sleep timer (efficiency)
    time.sleep(1)                               
    
    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
    
    # job-status of the response object 
    status = response["JobStatus"]                        
    print("Job status: {}".format(status))
    
    # if job still running check current status every 5 seconds
    while(status == "IN_PROGRESS"):
        
        # time lag before reporting status
        time.sleep(5)                                         
        response = client.get_document_analysis(JobId=jobId)
        
        # job-status of the response object
        status = response["JobStatus"]                        
        print("Job status: {}".format(status))
    
    return status

def getJobResults(jobId:str) -> list:
    """
    Returns the contents of the Textract job, after job status is completed
    """
    # initialize list object to track pages read
    pages = []                    

    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
    
    # add first page response to list (length of pages will be arbitrary) 
    pages.append(response)      
    print("Resultset page received: {}".format(len(pages)))
    
    # if NextToken present we have a pointer to page (e.g. Response -> Page) 
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']
    
    # iterate through the pages and append to response figure (assuming nextToken not None)
    while(nextToken):
        response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)
        pages.append(response)
        print("Resultset page received: {}".format(len(pages)))
        
        # move along linked-list for presence of NextToken response
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']
    
    # return amalgamation of all page responses 
    return pages

def runJob(bucket:str, key:str):
    """
    Function designed to call an AWS Textract job (implements helper function above)
    """
    jobId = startJob(bucket, key)   
    print("Started job with id: {}".format(jobId))

    # if job is complete on AWS return page responses 
    if(isJobComplete(jobId)):
        response = getJobResults(jobId)
        
    return response


"""
AWS Extraction Scripts (Key-Value Pairs + Text)

The functions were modified from AWS to extract key-value pairs in form documents 
from Block objects that are stored in a map. Please refer to following URL 
https://docs.aws.amazon.com/textract/latest/dg/examples-extract-kvp.html for 
greater granular detail on function properties
"""

def find_value_block(key_block, value_map):
    """
    Retrieving value block from AWS textract job, this contains the value text 
    """
    # iterate through the key blocks in the FORM relationships (should have a VALUE and CHILD type, n=2)
    for relationship in key_block['Relationships']:
        
        # if our key block object type is a VALUE we examine the relationship ID
        # NOTE WE SHOULD HAVE ONLY ONE ID FOR THE VALUE RELATIONSHIP TYPE
        if relationship['Type'] == 'VALUE':
            
            # singular ID item stored in list object (return value block object)
            for value_id in relationship['Ids']:
                value_block = value_map[value_id]
            
    # return all corresponding value series
    return value_block

def get_kv_relationship(key_map, value_map, block_map):
    """
    Retrieving the Key-Value relationship from FORM OCR Textract 
    """
    # initialize key-map dictionary for lineitems and corresponding accounting values
    key_value_map = {}
    
    # unpack the key_map to retrieve the block id and key names
    for block_id, key_block in key_map.items():

        # retrieve value block provided the key_block from each block id
        value_block = find_value_block(key_block, value_map)

        # get text value from key and value blocks
        key = get_text(key_block, block_map)
        val = get_text(value_block, block_map)
        
        # map the key and value pairs (e.g. {'Total Assets':'$ 189,232'})
        key_value_map[key] = val
        
    return key_value_map

def get_text(result, blocks_map):
    """
    Retrieving text values from given block object
    """
    # initialize container for text
    text = ''
    
    # if relationships header exists we can extract CHILD header
    if 'Relationships' in result:
        
        # relationship maps to a list (iterate through to reveal a dictionary)
        # e.g. 'Relationships' : [{'Type' : 'CHILD', 'Ids': ['e2b3b12f-ebb7-4f6e-914f-97b315672530']}]
        for relationship in result['Relationships']:
            
            # if relationship type is CHILD we explore job-id (indicates good fit)
            if relationship['Type'] == 'CHILD':
                
                # iterate through Ids list
                for child_id in relationship['Ids']:
                    
                    # select corresponding CHILD_ID from block map, this is sub-dictionary
                    word = blocks_map[child_id]
                    
                    # if block type is a word then we append with a space
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                        
                    # if block type is a selection element (e.g. an option button/mark)
                    # note we treat these cases with an X to denote an optional field 
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '    
    
    # return string corresponding with word 
    return text


"""
OCR Wrapper Functions

The functions perform an OCR job from AWS Textract, and returning a well formated 
data set that matches the assumptions of a balance sheet from the read FOCUS 
reports. We include additional helper functions used to suplement logic found within
our balance sheet reader script. 
"""

def trp2df(table:trp.Table) -> pd.DataFrame:
    """
    Function designed to convert a trp table into a dataframe object
    Algorithm runtime complexity -> O(n^2) approx. 
    
    Parameters
    ----------
    table : trp.Table
        A trp table object parsed from a pdf using AWS Textract   
    """

    N = len(table.rows)               # number of rows in table
    M = len(table.rows[0].cells)      # number of columns in table
    arr = [0]*N                       # initialize matrix container
    
    # iterate through each row within the provided table
    for row in np.arange(N):
        
        # strip the text from the cell references to construct (N X M) matrix
        arr[row] = [table.rows[row].cells[col].text.strip() for col in np.arange(M)]    
    
    df = pd.DataFrame(arr)
    
    # remove columns that are completely empty
    empty_cols = [col for col in df.columns if (df[col] == '').all()]
    df = df.drop(empty_cols, axis=1)

    # reset the column names (avoid the column names)
    df.columns = np.arange(df.columns.size)
    
    return df

def check_dollar_sign(row:np.ndarray) -> bool:
    """
    Determines if there exists a dollar sign present within a given row.
    This is useful in determing whether a balance sheet is present.
    
    Parameters
    ----------
    row : numpy.ndarray
        A given row vector from a dataframe. Note this input is generally
        passed as a check in the balance sheet assumption. 
    """
    
    def re_dollar_check(x):
        # we search for the presence of a dollar sign ($) in a string followed by character
        dollar_search = re.search('\$[^\]]+', x, flags=re.IGNORECASE)
        
        if dollar_search is not None: return True
        return False

    vFunc = np.vectorize(re_dollar_check)      # vectorize function to apply to an array
    cleanValue = vFunc(row)                    # apply vector function
    
    # search each vector return for presence of True
    # if True we have found a dollar '$' character
    series = np.argwhere(cleanValue == True)
    if len(series) > 0: 
        return True
    
    return False

def get_balance_sheet(df:pd.DataFrame) -> tuple:
    """
    Determines if a read table is a balance sheet term, see 
    assumptions below for determination of a balance-sheet
    
    Parameters
    ----------
    df : pandas.DataFrame
        A given DataFrame object corresponding to some table
        read from a FOCUS report via AWS Textract
    """
    
    # number of columns in dataframe
    n = df.columns.size
    
    ##############################################################
    #                           NOTES
    #         a 'good' dataframe should have 2-3 columns
    #      anything more or less is a reading error we ignore
    ##############################################################

    # if the dataframe has more than 3 columns then we most likley have an issue in parsing, avoid
    if n > 3: 
        return None

    elif n > 1:

        ##############################
        # Balance Sheet Assummptions
        ##############################

        # this is the first column which should have all line items (e.g. Cash, Total Assets, Total Liabilites)
        lineIndex = df.columns[0]

        # check for the word "cash" or "asset" in a string at the begining, ignoring case sensitivity (asset check)
        assetCheck = df[lineIndex].str.contains('^Cash|asset', regex=True, flags=re.IGNORECASE)

        # check for the word "Liabilities" in a string at the end, ignoring case sensitivity (liability check)
        debtCheck = df[lineIndex].str.contains('liabilities|liability', regex=True, flags=re.IGNORECASE)

        # check for the presence of $ sign, we assume the balance sheet items should have at least one $ sign
        # this check is used to avoid reading the table of contents, which was flagged in prior reads
        dollarCheck = df.apply(check_dollar_sign, axis=1)

        ##############################
        # Balance Sheet Determination
        ##############################

        check1 = df[assetCheck == True].empty                  # check for asset table
        check2 = df[debtCheck == True].empty                   # check for liability & equity table
        check3 = df[dollarCheck == True].empty                 # check for presence of '$' sign  

        # make sure the cash term appears toward the top of the balance sheet
        if np.argmax(assetCheck==True) < assetCheck.shape[0]/2:

            # if either asset term or liability term is found, with a $ sign we append the dataframe
            if (check1 == False or check2 == False) and (check3 == False):
                return (df, check1, check2)

def readTable(response:list):
    """
    Function to transform AWS Textract object to a DataFrame, 
    by searching for tables that match our balance sheet assumptions
    
    Parameters
    ----------
    response : list
        An AWS Textract response object corresponding to pages 
        of a given document page  
    """
    
    catDF = []          # in the event multiple tables detected on one page (concat them)
    page_series = []    # keep track of page objects where balance sheet was flagged
    page_nums = []      # keep track of page numbers where balance sheet was found
    page_count = 0
    
    tb_diff_c1 = 0      # flag to help indicate if tables, immediately precede one another 
    tb_diff_c2 = 0
    
    prior_c1 = True     # keep track of previous asset flag 
    prior_c2 = True     # keep track of previous liability flag
    
    # format the Textract response type 
    doc = trp.Document(response)
    
    # iterate through document pages
    for page in doc.pages:
        
        # itterate through page tables
        for table in page.tables: 
            
            # convert trp-table into dataframe object
            df = trp2df(table)
            
            # retrieve balance sheet from table (if possible)
            balance_sheet = get_balance_sheet(df)
            
            if type(balance_sheet) is tuple:
                
                bs, c1, c2 = balance_sheet      # unpack the return object
                
                # we append pages since asset and liablility tables are often seperate
                # there is no loss of generality if asset and liability terms are in one table
                catDF.append(bs)                

                # we want to keep track of pages that have been deemed as balance sheet
                # this helps speed up the runtime for TEXT, FORMS and PNG extraction
                if page not in page_series:
                    page_series.append(page)      
                    page_nums.append(page_count)
                
                ##############################
                # Flag for split tables 
                ##############################
                
                # indicates no liability term read, no previous asset term was read
                # we are currenlty reading the asset term of the balance sheet
                if c2 == True and prior_c1 == True and prior_c2 == True and c1 == False:
                    print('Balance sheet line items have been split across table\n')
                    prior_c1 = False
                    tb_diff_c1 = 0
                    
                # indicates no asset term read, no previous asset term was read
                # we are currenlty reading the liability term of the balance sheet
                elif c1 == True and prior_c1 == True and c2 == False:
                    print('Asset line items may be read after liability line items\n')
                    prior_c2 = False
                    tb_diff_c2 = 0
                    
                ##############################
                # Balance Sheet Exportation 
                ##############################

                # 1) indicates both assets and liability terms were found in table
                if (c2 == False and c1 == False) or (c2 == False and prior_c1 == False and tb_diff_c1 == 1):
                    return (pd.concat(catDF), page_series, page_nums)
                
                # 2) indicates liability term read before assets 
                elif prior_c2 == False and c1 == False and tb_diff_c2 == 1:
                    catDF.reverse()
                    return (pd.concat(catDF), page_series, page_nums)
                    
                else: pass
            
            # table scope iteration
            tb_diff_c1 += 1
            tb_diff_c2 += 1
        
        # page scope iteration
        page_count += 1

def readPNG(pages:list, png_path:str, bucket=str):
    """
    Function to transform AWS Textract object to a dataframe, 
    by searching for tables
    
    Parameters
    ----------
    pages : list
        A numeric list storing the pages where a balance sheet 
        is most likley stored, according to our balance sheet 
        assumptions found in our get_balance_sheet() function
        
    png_path : str
        The path on the s3 that stores the PNG files corresponding
        to a particular broker-dealer
        
    bucket : str
        The s3 bucket where all data is stored   
    """
    
    subfolder = png_path.split('/')[-2]      # subfolder where PNG files are stored
    
    # construct PNG directories with relevant pages
    textract_paths = [png_path + subfolder + '-p{}.png'.format(idx) for idx in pages]
            
    catDF = []          
    prior_c1 = True     
    prior_c2 = True     
    
    # path iterates through each png image matching the page numbers found in PDFs
    for path in textract_paths:
        
        try:
            # temporary data frame object for balance sheet information
            res = runJob(bucket, path)
            
            # if Textract job did not fail we continue extraction
            if res[0]['JobStatus'] != 'FAILED':

                # format the Textract response type 
                doc = trp.Document(res)

                # iterate through document pages
                for page in doc.pages:
                    
                    # itterate through page tables
                    for table in page.tables: 
                        
                        # convert trp-table into dataframe object
                        df = trp2df(table)
                        
                        # retrieve balance sheet from table
                        balance_sheet = get_balance_sheet(df)
                        
                        if type(balance_sheet) is tuple:
                
                            bs, c1, c2 = balance_sheet      # unpack the return object
                            
                            # we append pages since asset and liablility tables are often seperate
                            # there is no loss of generality if asset and liability terms are in one table
                            catDF.append(bs)            
                            
                            ##############################
                            # Flag for split tables 
                            ##############################

                            # indicates no asset or liability term was read
                            # we are currenlty reading the asset term of the balance sheet
                            if c2 == True and prior_c1 == True and prior_c2 == True and c1 == False:
                                print('Balance sheet line items have been split across table\n')
                                prior_c1 = False

                            # indicates no asset term read, no previous asset term was read
                            # we are currenlty reading the liability term of the balance sheet
                            elif c1 == True and prior_c1 == True and c2 == False:
                                print('Asset line items may be read after liability line items\n')
                                prior_c2 = False

                            ##############################
                            # Balance Sheet Exportation 
                            ##############################

                            # 1) indicates both assets and liability terms were found in table
                            if (c2 == False and c1 == False) or (c2 == False and prior_c1 == False):
                                return pd.concat(catDF)

                            # 2) indicates liability term read before assets 
                            elif prior_c2 == False and c1 == False:
                                catDF.reverse()
                                return pd.concat(catDF)

                            else: pass
            
        # broad exeption to catch Textract parsing errors
        except:pass

def readForm(doc_pages:list) -> dict:
    """
    Function to transform AWS Textract object to a dictionary
    of key value pairs from read PDF
    
    Parameters
    ----------
    doc_pages : list
        TRP page(s) for a AWS Textract response object 
        corresponding to pages of a given document page   
    """
    
    # initializing dictionary maps for KEY and VALUE pairs
    key_map = {}
    value_map = {}
    block_map = {}

    # iterate through document pages
    for page in doc_pages:

        # itterate through page tables
        for block in page.blocks: 

            # store the block id in map to retrive information later
            block_id = block['Id']
            block_map[block_id] = block

            # if Key-value set has been seen we deconstruct each KEY and VALUE map
            if block['BlockType'] == "KEY_VALUE_SET":

                # if KEY is labeled as entity type then we found Key, else we found VALUE
                if 'KEY' in block['EntityTypes']:
                    key_map[block_id] = block
                else:
                    value_map[block_id] = block
    
    # convert block objects to text dictionary map
    return get_kv_relationship(key_map, value_map, block_map)

def readText(doc_pages:list) -> dict:
    """
    Function to transform AWS Textract object to a dictionary 
    of text values and accompanying prediction confidence
    from FOCUS reports
    
    Parameters
    ----------
    doc_pages : list
        TRP page(s) for a AWS Textract response object 
        corresponding to pages of a given document page
    """
    
    # initializing dictionary maps for text
    text_map = {}
    
    # iterate through document pages
    for page in doc_pages:
        
        # itterate through page tables
        for block in page.blocks: 
            
            # if our block type is a line, we map the line text and confidence
            if block['BlockType'] == "LINE":
                text_map[block['Text']] = block['Confidence']
    
    # return completed text to confidence map
    return text_map


"""
OCR Primary Function

This function is responsbile for running a Textract job and
returning a pandas DataFrame object that represents a balance-sheet
"""

def textractParse(pdf_path:str, png_path:str, bucket:str) -> dict:
    """
    Function runs a Textract job and returns a DataFrane object
    that matches the conditions to determine a balance sheet 
    
    Parameters
    ----------
    pdf_path : str
        The path on the s3 that stores the PDF files corresponding
        to a particular broker-dealer
        
    png_path : str
        The path on the s3 that stores the PNG files corresponding
        to a particular broker-dealer
        
    bucket : str
        The s3 bucket where all data is stored   
    """
    errors = ''
    
    # temporary data frame object for balance sheet information
    res = runJob(bucket, pdf_path)
    
    # if Textract job did not fail we continue extraction
    if res[0]['JobStatus'] != 'FAILED':

        # perform OCR and return balance sheet with corresponding page object(s)
        tb_response = readTable(res)           
        
        # checks for type of return, if none then we log an error
        if type(tb_response) == tuple:
            
            # deconstruct the table response tuple into dataframe and page object parts
            df1, page_obj, page_num = tb_response
            print('\nPage number(s) for extraction in PNG are {}\n'.format(page_num))
            
            # try to extract from a PNG (we can still return a None here)
            df2 = readPNG(page_num, png_path, bucket)
            
            # provided balance sheet page number we select FORM and TEXT data
            forms_data = readForm(page_obj)      
            text_data = readText(page_obj)        
            
            print('\nTextract-PDF dataframe')
            print(df1)
            
            print('\nTextract-PNG dataframe')
            print(df2)
            
            return (df1, df2, forms_data, text_data, None)
        
        else:
            error = 'No Balance Sheet found, or parsing error'
            return (None, None, None, None, error)
    else:
        error = 'Could not parse, JOB FAILED'
        return (None, None, None, None, error)
