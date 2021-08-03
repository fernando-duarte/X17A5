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
from DatabaseUnstructured import unstructured_wrapper, reorder_columns, extra_cols
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
    
    png_paths = s3_session.list_s3_files(s3_bucket, out_folder_clean_png)
    png_asset_split = s3_session.list_s3_files(s3_bucket, out_folder_split_png + 'Assets/')
    png_liability_split = s3_session.list_s3_files(s3_bucket, out_folder_split_png + 'Liability & Equity/')
    
    pdf_asset_folder = out_folder_split_pdf + "Assets/"
    pdf_liable_folder = out_folder_split_pdf + "Liability & Equity/"
    
    png_asset_folder = out_folder_split_png + "Assets/"
    png_liable_folder = out_folder_split_png + "Liability & Equity/"
    
    # ==============================================================================
    #       STEP 6 (Segregate Asset and Liability & Equity from FOCUS Reports)
    # ==============================================================================
    
    print('\n========\nStep 6: Determing Assets and Liabilities & Equity Splits\n========\n')
    
    # directory where we store the broker-dealer information for cleaned filings on s3
    pdf_clean_files = filter(lambda x: brokerFilter(broker_dealers, x), pdf_paths) 
    png_clean_files = filter(lambda x: brokerFilter(broker_dealers, x), png_paths) 
    
    # --------------------------------------------
    # PDF PROCESSING (LINE-ITEM SPLIT)
    # --------------------------------------------
    
    print('\nBalance Sheets derived from PDFS')
    for file in pdf_clean_files:
        
        print('\n\t%s' % file)
        fileName = file.split('/')[-1]                                             # file-name for a given path
        asset_name = out_folder_split_pdf + 'Assets/' + fileName                   # export path to assets
        liability_name = out_folder_split_pdf + 'Liability & Equity/' + fileName   # export path to liability and equity
        
        # check to see presence of split files 
        if (asset_name in pdf_asset_split) and (liability_name in pdf_liability_split) and (rerun_job == False):
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
    
    print('\nBalance Sheets derived from PNGS')
    for file in png_clean_files:
        
        print('\n\t%s' % file)
        fileName = file.split('/')[-1]                                             # file-name for a given path
        asset_name = out_folder_split_png + 'Assets/' + fileName                   # export path to assets
        liability_name = out_folder_split_png + 'Liability & Equity/' + fileName   # export path to liability and equity
        
        # check to see presence of split files 
        if (asset_name in png_asset_split) and (liability_name in png_liability_split) and (rerun_job == False):              
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
    
    # ==============================================================================
    #     STEP 7 (Develop an Unstructured Asset and Liability & Equity Database)
    # ==============================================================================
    
    print('\n========\nStep 7: Creating Unstructured Database\n========\n')
    
    # retrieving CIK-Dealers JSON file from s3 bucket
    s3_pointer.download_file(s3_bucket, temp_folder + 'CIKandDealers.json', 'temp.json')
    with open('temp.json', 'r') as f: cik2brokers = json.loads(f.read())
    os.remove('temp.json')      
  
    # s3 paths where asset and liability paths are stored
    asset_paths = s3_session.list_s3_files(s3_bucket, pdf_asset_folder)
    liable_paths = s3_session.list_s3_files(s3_bucket, pdf_liable_folder)
    
    # intialize list to store dataframes for asset and liability & equity
    asset_concat = [0] * len(asset_paths)
    liable_concat = [0] * len(liable_paths)
    
    # --------------------------------------------
    # Asset Unstructured Database
    # --------------------------------------------
    print('Assets Unstructured Database')
    for idx, csv in enumerate(asset_paths):
        
        # decompose csv name into filename
        filename = csv.split('/')[-1]
        
        # determine whether we could download dataframe from s3 from PDF or PNG
        data_flag = 0
        pdf_df = pd.DataFrame()
        png_df = pd.DataFrame()
        
        try: # try loading in the balance sheet extracted from PDF
            s3_pointer.download_file(s3_bucket, csv, 'temp.csv')
            pdf_df = pd.read_csv('temp.csv')
            os.remove('temp.csv')

        # in the event we can't download file from s3 (i.e. does not exist, we ignore)
        except botocore.exceptions.ClientError: data_flag += 1
        
        try: # try loading in the balance sheet extracted from PNG
            s3_pointer.download_file(s3_bucket, png_asset_folder + filename, 'temp.csv')
            png_df = pd.read_csv('temp.csv')
            os.remove('temp.csv')
        except botocore.exceptions.ClientError: data_flag += 1

        # --------------------------------------------
        if data_flag == 0:
            # stores the reported data frame passing both PNG and PDF
            asset_concat[idx] = unstructured_wrapper(pdf_df, png_df, csv, cik2brokers, "Total asset")
        
        elif data_flag == 2:
            # assign an empty DataFrame in the event neither PDF or PNG is represented
            asset_concat[idx] = pd.DataFrame()
            
            print('\tTextract Issue for %s\n\t\tRefer to OCR confluence page https://fernandoduarte.atlassian.net/wiki/spaces/NN/pages/1145929733/OCR\n' % filename)
            
        elif data_flag == 1:
            fileName, filing_date, fiscal_year, cik = extra_cols(csv)
            
            if not pdf_df.empty:
                temp_df, total_flag, total_amt = totals_check(pdf_df)
                
                export_df["Total asset"] = unstructured_data(temp_df, filing_date, fiscal_year, cik, cik2brokers)
                export_df["Total asset"] = total_amt
                
                asset_concat[idx] = export_df
                
            else:
                temp_df, total_flag, total_amt = totals_check(png_df)
                
                export_df["Total asset"] = unstructured_data(temp_df, filing_date, fiscal_year, cik, cik2brokers)
                export_df["Total asset"] = total_amt
                
                asset_concat[idx] = export_df  
        
        if (idx + 1) % 100 == 0:
            print('\tWe have integrated %d balance sheet(s) to the unstructured database\n' % (idx+1))
    
    # --------------------------------------------
    # Liability & Equity Unstructured Database
    # --------------------------------------------
    print('\nLiability & Equity Unstructured Database')
    for idx, csv in enumerate(liable_paths):
        
        # decompose csv name into filename
        filename = csv.split('/')[-1]
        
        # determine whether we could download dataframe from s3 from PDF or PNG
        data_flag = 0
        pdf_df = pd.DataFrame()
        png_df = pd.DataFrame()
        
        try: # try loading in the balance sheet extracted from PDF
            s3_pointer.download_file(s3_bucket, csv, 'temp.csv')
            pdf_df = pd.read_csv('temp.csv')
            os.remove('temp.csv')

        # in the event we can't download file from s3 (i.e. does not exist, we ignore)
        except botocore.exceptions.ClientError: data_flag += 1
        
        try: # try loading in the balance sheet extracted from PNG
            s3_pointer.download_file(s3_bucket, png_liable_folder + filename, 'temp.csv')
            png_df = pd.read_csv('temp.csv')
            os.remove('temp.csv')
        except botocore.exceptions.ClientError: data_flag += 1

        # --------------------------------------------
        if data_flag == 0:
            # stores the reported data frame passing both PNG and PDF
            liable_concat[idx] = unstructured_wrapper(pdf_df, png_df, csv, cik2brokers, "Total liabilities & shareholder's equity")
        
        elif data_flag == 2:
            # assign an empty DataFrame in the event neither PDF or PNG is represented
            liable_concat[idx] = pd.DataFrame()
            
            print('\tTextract Issue for %s\n\t\tRefer to OCR confluence page https://fernandoduarte.atlassian.net/wiki/spaces/NN/pages/1145929733/OCR\n' % filename)
            
        elif data_flag == 1:
            fileName, filing_date, fiscal_year, cik = extra_cols(csv)
            
            if not pdf_df.empty:
                temp_df, total_flag, total_amt = totals_check(pdf_df)
                
                export_df["Total liabilities & shareholder's equity"] = unstructured_data(temp_df, filing_date, fiscal_year, cik, cik2brokers)
                export_df["Total liabilities & shareholder's equity"] = total_amt
                
                liable_concat[idx] = export_df
                
            else:
                temp_df, total_flag, total_amt = totals_check(png_df)
                
                export_df["Total liabilities & shareholder's equity"] = unstructured_data(temp_df, filing_date, fiscal_year, cik, cik2brokers)
                export_df["Total liabilities & shareholder's equity"] = total_amt
                
                liable_concat[idx] = export_df  
        
        if (idx + 1) % 100 == 0:
            print('\tWe have integrated %d balance sheet(s) to the unstructured database\n' % (idx+1))
    
    # --------------------------------------------
    # Database exportation
    # --------------------------------------------
    
    # writing data frame to .csv file
    asset_df = pd.concat(asset_concat)        # asset dataframe combining all rows from 
    asset_df = reorder_columns(asset_df,      # re-order columns for dataframe
                               col_preserve=['CIK', 'Name', 'Filing Date', 'Filing Year'])      

    filename = 'unstructured_assets.csv'
    asset_df.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'unstructured_assets.csv', Body=data)
    os.remove(filename)
          
    # writing data frame to .csv file
    liable_df = pd.concat(liable_concat)     
    liable_df = reorder_columns(liable_df, 
                                col_preserve=['CIK', 'Name', 'Filing Date', 'Filing Year'])    

    filename = 'unstructured_liable.csv'
    liable_df.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'unstructured_liable.csv', Body=data)
    os.remove(filename)
               
    # ==============================================================================
    #      STEP 8 (Develop a Structured Asset and Liability & Equity Database)
    # ==============================================================================      
    
    print('\n========\nStep 8: Creating Structured Database\n========\n')
    
    # retrieving the old training-test sets for classification model
    s3_pointer.download_file(s3_bucket, asset_ttset, 'temp.csv')
    old_asset_training = pd.read_csv('temp.csv')[['Lineitems', 'Manual Classification']]
    s3_pointer.download_file(s3_bucket, liable_ttset, 'temp.csv')
    old_liable_training = pd.read_csv('temp.csv')[['Lineitems', 'Manual Classification']]
    
    # ------------------------------------------------------------------------
    # retrieving the unstructured asset values file from s3 bucket
    s3_pointer.download_file(s3_bucket, out_folder + 'unstructured_assets.csv', 
                             'unstructAsset.csv')
    s3_pointer.download_file(s3_bucket, out_folder + 'unstructured_liable.csv', 
                             'unstructLiable.csv')
    assetDF = pd.read_csv('unstructAsset.csv')
    liableDF = pd.read_csv('unstructLiable.csv')
    os.remove('unstructAsset.csv')
    os.remove('unstructLiable.csv')      
    
    # ------------------------------------------------------------------------
    # retrieving the asset and liability classification models from s3 bucket
    s3_pointer.download_file(s3_bucket, asset_model, 'asset_mdl.joblib')
    s3_pointer.download_file(s3_bucket, liability_model, 'liable_mdl.joblib')
    assetMDL = load('asset_mdl.joblib')
    liableMDL = load('liable_mdl.joblib')
    os.remove('asset_mdl.joblib')
    os.remove('liable_mdl.joblib')      
    
    # ------------------------------------------------------------------------
    # text vectorizer to format line items to be accepted in the model 
    str_mdl = HashingVectorizer(strip_accents='unicode', lowercase=True, analyzer='word', 
                                n_features=1000, norm='l2')
    
    # construct the asset/liability mapping alongside prediction probabilites (unpack tuple for data figures)
    data_response = structured_wrapper(assetDF, liableDF, old_asset_training, old_liable_training, 
                                       str_mdl, assetMDL, liableMDL)
    struct_asset_map, struct_liable_map, a_proba_df, l_proba_df, struct_asset_df, struct_liable_df = data_response
    
    # ------------------------------------------------------------------------------
    # Auxillary Database Files 
    # ------------------------------------------------------------------------------
    
    # concat the old and new asset training sets, where new predictions are greater than or equal to 85%    
    add_training = a_proba_df[a_proba_df['Max Prediction score'] >= 0.85][['Lineitems', 'Manual Classification']]
    joint_training = pd.concat([old_asset_training, 
                                add_training]).drop_duplicates(subset=['Lineitems'], 
                                                               keep='first')
    
    joint_training.to_csv('temp.csv', index=False)
    with open('temp.csv', 'rb') as data: s3_pointer.put_object(Bucket=s3_bucket, Key=asset_ttset, Body=data)
    
    # concat the old and new liability training sets, where new predictions are greater than or equal to 85%    
    add_training = l_proba_df[l_proba_df['Max Prediction score'] >= 0.85][['Lineitems', 'Manual Classification']]
    joint_training = pd.concat([old_liable_training, 
                                add_training]).drop_duplicates(subset=['Lineitems'], 
                                                               keep='first')
    
    joint_training.to_csv('temp.csv', index=False)
    with open('temp.csv', 'rb') as data: s3_pointer.put_object(Bucket=s3_bucket, Key=liable_ttset, Body=data)
    
    os.remove('temp.csv')
    
    filename = 'asset_name_map.csv'
    struct_asset_map.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'asset_name_map.csv', Body=data)
    os.remove(filename)
    
    filename = 'liability_name_map.csv'
    struct_liable_map.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'liability_name_map.csv', Body=data)
    os.remove(filename)
          
    # ------------------------------------------------------------------------------
    # Database Exportation 
    # ------------------------------------------------------------------------------
    
    filename = 'structured_asset.csv'
    struct_asset_df.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'structured_asset.csv', Body=data)
    os.remove(filename)
          
    filename = 'structured_liability.csv'
    struct_liable_df[struct_liable_df.columns[~np.isin(struct_liable_df.columns, 
                                                       ['Relative Error1', 'Relative Error2', 
                                                        'Relative Error3', 'Relative Error4'])]].to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'structured_liability.csv', Body=data)
    os.remove(filename)
    