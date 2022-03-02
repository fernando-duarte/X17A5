#!/usr/bin/env python
# coding: utf-8

"""
run_build_database.py: Script responsible for creating the structured database by 
aggregating individual balance sheets from broker-dealers. 

    1) DatabaseSplits.py
    2) DatabaseUnstructured.py
    2) DatabaseStructured.py
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import os
import json
import botocore

import pandas as pd
import numpy as np

from joblib import load
from sklearn.feature_extraction.text import HashingVectorizer

from DatabaseSplits import lineItems
from DatabaseUnstructured import unstructured_wrapper, reorder_columns, extra_cols, totals_check, unstructured_data
from DatabaseStructured import structured_wrapper

from run_file_extraction import brokerFilter


##################################
# MAIN CODE EXECUTION
##################################

def main_p3(s3_bucket, s3_pointer, s3_session, temp_folder, out_folder_clean_pdf, 
            out_folder_clean_png, out_folder_split_pdf, out_folder_split_png,
            out_folder, asset_model, liability_model, asset_ttset, liable_ttset, 
            rerun_job, broker_dealers):
    
    pdf_paths = s3_session.list_s3_files(s3_bucket, out_folder_clean_pdf)
    pdf_asset_split = s3_session.list_s3_files(s3_bucket, out_folder_split_pdf + 'Assets/')
    pdf_liability_split = s3_session.list_s3_files(s3_bucket, out_folder_split_pdf + 'Liability & Equity/')
  
    pdf_asset_folder = out_folder_split_pdf + "Assets/"
    pdf_liable_folder = out_folder_split_pdf + "Liability & Equity/"
   
    # ==============================================================================
    #       STEP 6 (Segregate Asset and Liability & Equity from FOCUS Reports)
    # ==============================================================================
    
    print('\n========\nStep 6: Determing Assets and Liabilities & Equity Splits\n========\n')
    
    # directory where we store the broker-dealer information for cleaned filings on s3
    pdf_clean_files = filter(lambda x: brokerFilter(broker_dealers, x), pdf_paths) 
    
    # --------------------------------------------
    # PDF PROCESSING (LINE-ITEM SPLIT)
    # --------------------------------------------
    
    print('\nBalance Sheets derived from PDFS')
    for counter,file in enumerate(pdf_clean_files):
        
        print('\n\t%s' % file)
        if counter%40 == 0:
            print(counter)
        fileName = file.split('/')[-1]                                             # file-name for a given path
        asset_name = out_folder_split_pdf + 'Assets/' + fileName                   # export path to assets
        liability_name = out_folder_split_pdf + 'Liability & Equity/' + fileName   # export path to liability and equity
        
        # check to see presence of split files 
        if (asset_name in pdf_asset_split) and (liability_name in pdf_liability_split) and (rerun_job > 6):
            print("\t\tWe've already performed split operation for %s" % fileName)
        
        else: 
            s3_pointer.download_file(s3_bucket, file, 'temp.csv')
            df = pd.read_csv('temp.csv')
            os.remove('temp.csv')
            
            n = df.columns.size   # the number of columns in read dataframe    

            if n > 1: # if there is more than 1 column we continue examination 

                # all line item for balance sheet (first column)
                arr = df[df.columns[0]].dropna().values     

                # extract line items if possible for both asset and liability terms
                response = lineItems(arr, df)

                # if response not None we decompose each
                if response is not None:
                    print('\t\tPerformed split operation for %s' % fileName)
                    
                    # unpack the response object to component parts
                    df_asset, df_liability = response

                    # writing data frame to .csv file (we overwrite file name to save space)
                    df_asset.to_csv(fileName, index=False)
                    with open(fileName, 'rb') as data:
                        s3_pointer.put_object(Bucket=s3_bucket, Key=asset_name, Body=data)

                    df_liability.to_csv(fileName, index=False)
                    with open(fileName, 'rb') as data:
                        s3_pointer.put_object(Bucket=s3_bucket, Key=liability_name, Body=data)

                    # remove local file after it has been created
                    os.remove(fileName)
                
                else: print('\n\t\tIssue with splitting balance-sheet table into asset and liability')

            else: print('\n\t\t%s incomplete dataframe' % file)
    
    # --------------------------------------------
    # PNG PROCESSING (LINE-ITEM SPLIT)
    # --------------------------------------------
    
    