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


def main_p3(s3_bucket, s3_pointer, s3_session, input_folder, temp_folder, out_folder_clean_pdf, 
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
    pdf_clean_files = list(filter(lambda x: brokerFilter(broker_dealers, x), pdf_paths))
    png_clean_files = list(filter(lambda x: brokerFilter(broker_dealers, x), png_paths))
    
    # --------------------------------------------
    # PDF PROCESSING (LINE-ITEM SPLIT)
    # --------------------------------------------
    
    print('\nBalance Sheets derived from PDFS')
    for counter,file in enumerate(pdf_clean_files):
        
        print('\n\t%s' % file)
        if counter%40 == 0:
            print((counter, len(pdf_clean_files)))
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
    
    print('\nBalance Sheets derived from PNGS')
    for file in png_clean_files:
        
        print('\n\t%s' % file)
        fileName = file.split('/')[-1]                                             # file-name for a given path
        asset_name = out_folder_split_png + 'Assets/' + fileName                   # export path to assets
        liability_name = out_folder_split_png + 'Liability & Equity/' + fileName   # export path to liability and equity
        
        # check to see presence of split files 
        if (asset_name in png_asset_split) and (liability_name in png_liability_split) and (rerun_job > 6):              
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
    
    # these functions are defined locally to reduce number of variables
    def paral_asset(csv_name_local, csv):
        pdf_df = pd.read_csv(csv_name_local)        
        fileName, filing_date, fiscal_year, cik = extra_cols(csv)

        temp_df, total_flag, total_amt = totals_check(pdf_df)
        export_df = unstructured_data(temp_df, filing_date, fiscal_year, cik, cik2brokers)
        export_df["Total asset"] = total_amt

        return export_df

    def paral_liabilities(csv_name_local, csv):
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
    asset_paths = s3_session.list_s3_files(s3_bucket, pdf_asset_folder)
    liable_paths = s3_session.list_s3_files(s3_bucket, pdf_liable_folder)
    
    # --------------------------------------------
    # Asset Unstructured Database
    # --------------------------------------------
    print('Assets Unstructured Database')
    for idx, csv in enumerate(asset_paths):
        # decompose csv name into filename
        filename = csv.split('/')[-1]
        try: # try loading in the balance sheet extracted from PDF
            s3_pointer.download_file(s3_bucket, csv, 'split_assets/'+ filename)
        except:
            pass
        
    for idx, csv in enumerate(liable_paths):
        # decompose csv name into filename
        filename = csv.split('/')[-1]
        try: # try loading in the balance sheet extracted from PDF
            s3_pointer.download_file(s3_bucket, csv, 'split_liabilities/'+ filename)
        except:
            pass
    
    li_asset = os.listdir('split_assets/')
    
    # usually when running script as a notebook, an automatic file (not a csv) is created that we do not need
    for c,l in enumerate(li_asset):
        if l[-3:] != 'csv':
            del li_asset[c]    
            
    m_asset = len(li_asset)
    
    # When running the code for many broker dealers memory usage becomes an issue. This is because the unstructured database
    # is one large dataframe with almost as many features as there are pdfs. To solve this problem we process pdfs 1000-per-1000. And then 
    # concatenate them all at the very end
    size_cut = 1000
    
    for cut in range(0,m_asset,size_cut):
        print('Concatenating Asset DataFrames for cut ' + str(cut) + ' to ' + str(cut+size_cut))
        top = min(cut+size_cut,m_asset)
        asset_concat = Parallel(n_jobs=-1)(delayed(paral_asset)('split_assets/' + li_asset[idx],
                                                            'split_assets/' + li_asset[idx]) for idx in range(cut,top))
        asset_df = pd.concat(asset_concat) 
        asset_df = reorder_columns(asset_df,      # re-order columns for dataframe
                               col_preserve=['CIK', 'Name', 'Filing Date', 'Filing Year']) 
        
        asset_df.to_csv('unstructured_asset/asset_df_'+str(cut)+'.csv', index = False)
        
        filename = 'unstructured_asset/asset_df_'+str(cut)+'.csv'
        with open(filename, 'rb') as data:
            s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + filename, Body=data)
       
    print('\nLiability & Equity Unstructured Database')

    li_liable = os.listdir('split_liabilities/')
    for c,l in enumerate(li_liable):
        if l[-3:] != 'csv':
            del li_liable[c]    
                
    m_liable = len(li_liable)
    for cut in range(0,m_liable,size_cut):
        print('Concatenating liabilities DataFrames for cut ' + str(cut) + ' to ' + str(cut+size_cut))
        top = min(cut+size_cut,m_liable)
        liable_concat = Parallel(n_jobs=-1)(delayed(paral_liabilities)('split_liabilities/' + li_liable[idx],
                                                            'split_liabilities/' + li_liable[idx]) for idx in range(cut,top))
                            
        liable_df = pd.concat(liable_concat) 
        liable_df = reorder_columns(liable_df,      # re-order columns for dataframe
                               col_preserve=['CIK', 'Name', 'Filing Date', 'Filing Year']) 
        
        liable_df.to_csv('unstructured_liable/liable_df_'+str(cut)+'.csv', index = False)  
        
        filename = 'unstructured_liable/liable_df_'+str(cut)+'.csv'
        with open(filename, 'rb') as data:
            s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + filename, Body=data)
    
    # ==============================================================================
    #      STEP 8 (Develop a Structured Asset and Liability & Equity Database)
    # ==============================================================================      
    
    print('\n========\nStep 8: Creating Structured Database\n========\n')
    
    # in the unlikely case where the number of asset dfs and number of liable dfs are different we take min for stability
    m = min(m_asset, m_liable)
                  
    ml_training_files = ['asset_lineitem_training_testing.csv','liability_log_reg_mdl_v2.joblib',
                     'asset_log_reg_mdl_v2.joblib','liable_lineitem_training_testing.csv']

    for f in ml_training_files:
        try:
            s3_pointer.download_file(s3_bucket, input_folder + f, 'temp.json')
            os.remove('temp.json')
        except:
            with open('ml_training/' + f, 'rb') as data:
                s3_pointer.put_object(Bucket=s3_bucket, Key=input_folder + f, Body=data)
          
    for cut in range(0,m,size_cut):
        print('Creating Structured Database for cut ' + str(cut) + ' to ' + str(cut+size_cut))
        assetDF = pd.read_csv('unstructured_asset/asset_df_'+str(cut)+'.csv')
        liableDF = pd.read_csv('unstructured_liable/liable_df_'+str(cut)+'.csv')
        
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
        joint_training.to_csv('ml_training/asset_lineitem_training_testing.csv', index=False)
                  
        # concat the old and new liability training sets, where new predictions are greater than or equal to 85%    
        add_training = l_proba_df[l_proba_df['Max Prediction score'] >= 0.85][['Lineitems', 'Manual Classification']]
        joint_training = pd.concat([old_liable_training, 
                                    add_training]).drop_duplicates(subset=['Lineitems'], 
                                                                   keep='first')

        joint_training.to_csv('temp.csv', index=False)
        with open('temp.csv', 'rb') as data: s3_pointer.put_object(Bucket=s3_bucket, Key=liable_ttset, Body=data)
        joint_training.to_csv('ml_training/liable_lineitem_training_testing.csv', index=False)

        os.remove('temp.csv')

        filename = 'asset_name_map_' +str(cut)+ '.csv'
        struct_asset_map.to_csv(filename, index=False)
        with open(filename, 'rb') as data:
            s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'asset_name_map.csv', Body=data)
        os.remove(filename)
        
        filename = 'liability_name_map_' +str(cut)+ '.csv'
        struct_liable_map.to_csv(filename, index=False)
        with open(filename, 'rb') as data:
            s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'liability_name_map.csv', Body=data)
        os.remove(filename)

        # ------------------------------------------------------------------------------
        # Database Exportation 
        # ------------------------------------------------------------------------------

        filename = 'structured_asset/structured_asset_' +str(cut)+ '.csv'
        struct_asset_df.to_csv(filename, index=False)

        filename = 'structured_liable/structured_liability_' +str(cut)+ '.csv'
        struct_liable_df[struct_liable_df.columns[~np.isin(struct_liable_df.columns, 
                                                           ['Relative Error1', 'Relative Error2', 
                                                            'Relative Error3', 'Relative Error4'])]].to_csv(filename, index=False)
       
    
    print('Concatenating partial structured databases into final structure database ')
    structure_asset = []
    for cut in range(0,m_asset,size_cut):
        filename = 'structured_asset/structured_asset_' +str(cut)+ '.csv'
        df_asset = pd.read_csv(filename)
        structure_asset.append(df_asset)
        os.remove(filename)

    structured_asset = pd.concat(structure_asset)
    structured_asset = structured_asset.sort_values(by = ['CIK','Filing Year'])
    
    
    structure_liable = []
    for cut in range(0,m_liable,size_cut):
        filename = 'structured_liable/structured_liability_' +str(cut)+ '.csv'
        df_liable = pd.read_csv(filename)
        structure_liable.append(df_liable)
        os.remove(filename)

    structured_liable = pd.concat(structure_liable)
    structured_liable = structured_liable.sort_values(by = ['CIK','Filing Year'])
    
    
    filename = 'structured_asset.csv'
    structured_asset.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'structured_asset.csv', Body=data)
    os.remove(filename)
          
    filename = 'structured_liability.csv'
    structured_liable.to_csv(filename, index=False)
    
    with open(filename, 'rb') as data:
        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder + 'structured_liability.csv', Body=data)
    os.remove(filename)
    
    
    
    # cleaning up local folders once the whole script has successfully ran. I didn't have the time to code it, but this local file 
    # structure would allow the code to not start over if it crashes for some AWS reasons

    for filename in li_asset:
        os.remove('split_assets/' + filename)
        
    for filename in li_liable:
        os.remove('split_liabilities/' + filename)
        
    for filename in os.listdir('unstructured_asset'):
        os.remove('unstructured_asset/' + filename)
        
    for filename in os.listdir('unstructured_liable'):
        os.remove('unstructured_liable/' + filename)
    
    
