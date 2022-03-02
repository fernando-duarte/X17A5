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
from joblib import Parallel, delayed

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
      
    # ==============================================================================
    #     STEP 7 (Develop an Unstructured Asset and Liability & Equity Database)
    # ==============================================================================
    
    print('\n========\nStep 7: Creating Unstructured Database\n========\n')
    
    # retrieving CIK-Dealers JSON file from s3 bucket
    s3_pointer.download_file(s3_bucket, temp_folder + 'CIKandDealers.json', 'temp.json')
    with open('temp.json', 'r') as f: cik2brokers = json.loads(f.read())
    os.remove('temp.json')      
    
    def paral_asset(csv_name_local,csv):
        pdf_df = pd.read_csv(csv_name_local)        
        fileName, filing_date, fiscal_year, cik = extra_cols(csv)

        temp_df, total_flag, total_amt = totals_check(pdf_df)
        export_df = unstructured_data(temp_df, filing_date, fiscal_year, cik, cik2brokers)
        export_df["Total asset"] = total_amt

        return export_df

    def paral_liabilities(csv_name_local,csv):
        pdf_df = pd.read_csv(csv_name_local)        
        fileName, filing_date, fiscal_year, cik = extra_cols(csv)
        try:
            temp_df, total_flag, total_amt = totals_check(pdf_df)
            export_df = unstructured_data(temp_df, filing_date, fiscal_year, cik, cik2brokers)
            export_df["Total liabilities & shareholder's equity"] = total_amt
        except:
            return pd.DataFrame()
        return export_df
  
    # s3 paths where asset and liability paths are stored
    #asset_paths = s3_session.list_s3_files(s3_bucket, pdf_asset_folder)
    #liable_paths = s3_session.list_s3_files(s3_bucket, pdf_liable_folder)
    
    # intialize list to store dataframes for asset and liability & equity
    # --------------------------------------------
    # Asset Unstructured Database
    # --------------------------------------------
    print('Assets Unstructured Database')
    li_asset = os.listdir('split_assets/')
    del li_asset[8792]
    
    m = len(li_asset)
    size_cut = 1000
    for cut in range(0,m,size_cut):
        print('Concatenating Asset DataFrames for: ' + str(cut))
        continue
        top = min(cut+size_cut,m)
        asset_concat = Parallel(n_jobs=-1)(delayed(paral_asset)('split_assets/' + li_asset[idx],
                                                            'split_assets/' + li_asset[idx]) for idx in range(cut,top))
        asset_df = pd.concat(asset_concat) 
        asset_df = reorder_columns(asset_df,      # re-order columns for dataframe
                               col_preserve=['CIK', 'Name', 'Filing Date', 'Filing Year']) 
        
        asset_df.to_csv('asset_dfs/asset_df_'+str(cut)+'.csv', index = False)
        
    print('\nLiability & Equity Unstructured Database')
                                  
    li_liable = os.listdir('split_liabilities/')
    del li_liable[49781]
    
    m = len(li_liable)
    for cut in range(0,m,size_cut):
        print('Concatenating liabilities DataFrames for: ' + str(cut))
        continue
        top = min(cut+size_cut,m)
        liable_concat = Parallel(n_jobs=-1)(delayed(paral_liabilities)('split_liabilities/' + li_liable[idx],
                                                            'split_liabilities/' + li_liable[idx]) for idx in range(cut,top))
                            
        liable_df = pd.concat(liable_concat) 
        liable_df = reorder_columns(liable_df,      # re-order columns for dataframe
                               col_preserve=['CIK', 'Name', 'Filing Date', 'Filing Year']) 
        
        liable_df.to_csv('liable_dfs/liable_df_'+str(cut)+'.csv', index = False)  
    """ This code won't run due to memory constraints
    li_asset_dfs = []
    print('Concatenating all Asset DFs')
    for cut in range(0,m,size_cut):
        asset_df = pd.read_csv('asset_dfs/asset_df_'+str(cut)+'.csv')
        li_asset_dfs.append(asset_df)
    # --------------------------------------------
    # Database exportation
    # --------------------------------------------
    # writing data frame to .csv file
    asset_df = pd.concat(li_asset_dfs)        # asset dataframe combining all rows from 
    asset_df = reorder_columns(asset_df,      # re-order columns for dataframe
                               col_preserve=['CIK', 'Name', 'Filing Date', 'Filing Year'])      

    filename = 'unstructured_assets.csv'
    asset_df.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'unstructured_assets.csv', Body=data)
    os.remove(filename)
    
    # --------------------------------------------
    # Liability & Equity Unstructured Database
    # --------------------------------------------
        
    li_liable_dfs = []
    print('Concatenating all liabilities DFs')
    for cut in range(0,m,size_cut):
        liable_df = pd.read_csv('liable_dfs/liable_df_'+str(cut)+'.csv')
        li_liable_dfs.append(liable_df)
                                                 
    # writing data frame to .csv file
    liable_df = pd.concat(li_liable_dfs)     
    liable_df = reorder_columns(liable_df, 
                                col_preserve=['CIK', 'Name', 'Filing Date', 'Filing Year'])    

    filename = 'unstructured_liable.csv'
    liable_df.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'unstructured_liable.csv', Body=data)
    os.remove(filename)
    """
    # ==============================================================================
    #      STEP 8 (Develop a Structured Asset and Liability & Equity Database)
    # ==============================================================================      
    
    print('\n========\nStep 8: Creating Structured Database\n========\n')
    for cut in range(0,m,size_cut):
        print('Stucture for cut: ' + str(cut))
        assetDF = pd.read_csv('asset_dfs/asset_df_'+str(cut)+'.csv')
        liableDF = pd.read_csv('liable_dfs/liable_df_'+str(cut)+'.csv')
        
        # retrieving the old training-test sets for classification model
        s3_pointer.download_file(s3_bucket, asset_ttset, 'temp.csv')
        old_asset_training = pd.read_csv('temp.csv')[['Lineitems', 'Manual Classification']]
        s3_pointer.download_file(s3_bucket, liable_ttset, 'temp.csv')
        old_liable_training = pd.read_csv('temp.csv')[['Lineitems', 'Manual Classification']]
    
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

        filename = 'struct_asset_map/asset_name_map_' +str(cut)+ '.csv'
        struct_asset_map.to_csv(filename, index=False)
        with open(filename, 'rb') as data:
            s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'asset_name_map.csv', Body=data)
        #os.remove(filename)

        filename = 'struct_liable_map/liability_name_map_' +str(cut)+ '.csv'
        struct_liable_map.to_csv(filename, index=False)
        with open(filename, 'rb') as data:
            s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'liability_name_map.csv', Body=data)
        #os.remove(filename)

        # ------------------------------------------------------------------------------
        # Database Exportation 
        # ------------------------------------------------------------------------------

        filename = 'structured_asset/structured_asset_' +str(cut)+ '.csv'
        struct_asset_df.to_csv(filename, index=False)
        with open(filename, 'rb') as data:
            s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'structured_asset.csv', Body=data)
        #os.remove(filename)

        filename = 'structured_liable/structured_liability_' +str(cut)+ '.csv'
        struct_liable_df[struct_liable_df.columns[~np.isin(struct_liable_df.columns, 
                                                           ['Relative Error1', 'Relative Error2', 
                                                            'Relative Error3', 'Relative Error4'])]].to_csv(filename, index=False)
        with open(filename, 'rb') as data:
            s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'structured_liability.csv', Body=data)
        #os.remove(filename)
    