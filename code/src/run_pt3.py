#!/usr/bin/env python
# coding: utf-8

"""
run_pt3.py: Script responsible for creating the structured database by 
aggregating individual b

    1) DatabaseSplits.py
    2) DatabaseUnstructured.py
    2) DatabaseStructured.py
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import os
import botocore

from DatabaseSplits import lineItems
from DatabaseUnstructured import extra_cols, totals_check, special_merge, unstructured_data
from DatabaseStructured import prediction_probabilites, structured_data, relative_indicator, relative_finder

from sklearn.feature_extraction.text import HashingVectorizer


##################################
# MAIN CODE EXECUTION
##################################

def main_p3(s3_bucket, s3_pointer, s3_session, temp_folder, out_folder_clean_pdf, 
            out_folder_clean_png, out_folder_split_pdf, out_folder_split_png,
            out_folder, asset_model, liability_model):
    
    pdf_paths = np.array(s3_session.list_s3_files(s3_bucket, out_folder_clean_pdf))[1:]
    pdf_asset_split = np.array(s3_session.list_s3_files(s3_bucket, out_folder_split_pdf + 'Assets/'))
    pdf_liability_split = np.array(s3_session.list_s3_files(s3_bucket, out_folder_split_pdf + 'Liability & Equity/'))
    
    png_paths = np.array(s3_session.list_s3_files(s3_bucket, out_folder_clean_png))[1:]
    png_asset_split = np.array(s3_session.list_s3_files(s3_bucket, out_folder_split_png + 'Assets/'))
    png_liability_split = np.array(s3_session.list_s3_files(s3_bucket, out_folder_split_png + 'Liability & Equity/'))
    
    pdf_asset_folder = out_folder_split_pdf + "Assets/"
    pdf_liable_folder = out_folder_split_pdf + "Liability & Equity/"
    
    png_asset_folder = out_folder_split_png + "Assets/"
    png_liable_folder = out_folder_split_png + "Liability & Equity/"
    
    # ==============================================================================
    #       STEP 6 (Segregate Asset and Liability & Equity from FOCUS Reports)
    # ==============================================================================
    
    # --------------------------------------------
    # PDF PROCESSING (LINE-ITEM SPLIT)
    # --------------------------------------------
    
    # iterate through files from s3 bucket 
    for file in pdf_paths:
        print('\n %s' % file)
        fileName = file.split('/')[-1]                                          # file-name for a given path
        asset_name = pdf_output_folder + 'Assets/' + fileName                   # export path to assets
        liability_name = pdf_output_folder + 'Liability & Equity/' + fileName   # export path to liability and equity
        
        # only want one name (cik) to be handled with re-run flag
        
        # check to see presence of split files 
        if (asset_name not in pdf_asset_split) or (liability_name not in pdf_liability_split):
        
            # download temporary file from s3 bucket
            s3_pointer.download_file(s3_bucket, file, 'temp.csv')
            df = pd.read_csv('temp.csv')

            n = df.columns.size   # the number of columns in read dataframe    

            if n > 1: # if there is more than 1 column we continue examination 

                # all line item for balance sheet (first column)
                arr = df[df.columns[0]].dropna().values     

                # extract line items if possible for both asset and liability terms
                response = lineItems(arr, df)

                # if response not None we decompose each
                if response is not None:
                    
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
                
                else: print('Issue with splitting balance-sheet table into asset and liability')

            else: print('%s incomplete dataframe' % file)
                
        else: print("We've already downloaded %s" % fileName)
    
    # --------------------------------------------
    # PNG PROCESSING (LINE-ITEM SPLIT)
    # --------------------------------------------
    
    # iterate through files from s3 bucket 
    for file in png_paths:
        print('\n %s' % file)
        fileName = file.split('/')[-1]                                          # file-name for a given path
        asset_name = png_output_folder + 'Assets/' + fileName                   # export path to assets
        liability_name = png_output_folder + 'Liability & Equity/' + fileName   # export path to liability and equity
        
        # check to see presence of split files 
        if (asset_name not in png_asset_split) or (liability_name not in png_liability_split):
        
            # download temporary file from s3 bucket
            s3_pointer.download_file(s3_bucket, file, 'temp.csv')
            df = pd.read_csv('temp.csv')

            n = df.columns.size   # the number of columns in read dataframe    

            if n > 1: # if there is more than 1 column we continue examination 

                # all line item for balance sheet (first column)
                arr = df[df.columns[0]].dropna().values     

                # extract line items if possible for both asset and liability terms
                response = lineItems(arr, df)

                # if response not None we decompose each
                if response is not None:
                    
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
                
                else: print('Issue with splitting balance-sheet table into asset and liability')

            else: print('%s incomplete dataframe' % file)
                
        else: print("We've already downloaded %s" % fileName)
    
    
    # remove local file for storing cleaned data  
    os.remove('temp.csv')
    
    print('\n===================\nStep 6: Determined Assets and Liabilities & Equity Sets\n===================')
    
    # ==============================================================================
    #     STEP 7 (Develop an Unstructured Asset and Liability & Equity Database)
    # ==============================================================================
          
    # retrieving CIK-Dealers JSON file from s3 bucket
    s3_pointer.download_file(s3_bucket, temp_folder + 'CIKandDealers.json', 'temp.json')
    with open('temp.json', 'r') as f: cik2brokers = json.loads(f.read())

    # remove local file after it has been created (variable is stored in memory)
    os.remove('temp.json')      
  
    # s3 paths where asset and liability paths are stored
    asset_paths = s3_session.list_s3_files(s3_bucket, pdf_asset_folder)
    liable_paths = s3_session.list_s3_files(s4_bucket, pdf_liable_folder)
    
    # intialize list to store dataframes for asset and liability & equity
    asset_concat = [0] * len(asset_paths)
    liable_concat = [0] * len(liable_paths)
    
    # --------------------------------------------
    # Asset Unstructured Database
    # --------------------------------------------
    print('Assets Unstructured Database')
    for idx, csv in enumerate(asset_paths):
        
        # decompose csv name into corresponding terms
        fileName, filing_date, fiscal_year, cik = extra_cols(csv)
        
        # first load in both the PNG and PDF split balance sheets
        # NOTE: All these balance sheets are cleaned numerical values
        try:
            s3.download_file(s3_bucket, csv, 'temp.csv')
            pdf_df = pd.read_csv('temp.csv')
            s3.download_file(s3_bucket, png_asset_folder + fileName, 'temp.csv')
            png_df = pd.read_csv('temp.csv')
            os.remove('temp.csv')

            print('Working on %s-%s' % (cik, filing_date))
            
            # run accounting check to remove sub-totals for each respective line-item
            temp_df1, total_flag1, total_amt1 = totals_check(pdf_df)
            temp_df2, total_flag2, total_amt2 = totals_check(png_df)
            
            #########################
            # Exporation assumption
            #########################
            
            # if no pdf or png returns a total asset flag then we want to merge
            # otherwise we simply see use the first value
            
            if (total_flag1 == 1) or (total_flag2 == 1):
                
                if total_flag1 == 1:
                    # construct row for the unstructured data frame 
                    export_df = unstructured_data(temp_df1, filing_date, fiscal_year, cik, cik2brokers)
                    
                    # we have that "total asset" was found and matches
                    export_df["Total asset"] = total_amt1
                    
                elif total_flag2 == 1:
                    # construct row for the unstructured data frame 
                    export_df = unstructured_data(temp_df2, filing_date, fiscal_year, cik, cik2brokers)

                    export_df["Total asset"] = total_amt2
                    
            # we have that "total asset" was found, but did not match correctly
            # we do not need to add a "total asset" column since we already have it somewhere
            elif (total_flag1 == 0) or (total_flag2 == 0):
                
                # do a special merge that combines unique line items names between PDF & PNG
                df = special_merge(temp_df1, temp_df2, '0')
                export_df = unstructured_data(df, filing_date, fiscal_year, cik, cik2brokers)
                
            # we have that no "total asset" figure was found, so we have nothing 
            elif (total_flag1 == 2) and (total_flag2 == 2):
                
                # do a special merge that combines unique line items names between PDF & PNG
                df = special_merge(temp_df1, temp_df2, '0')
                export_df = unstructured_data(df, filing_date, fiscal_year, cik, cik2brokers)

            # stores the reported data frame 
            asset_concat[idx] = export_df
            
        # in the event we can't download file from s3 (i.e. does not exist, we ignore)
        except botocore.exceptions.ClientError:
            
            # assign an empty DataFrame and print out error
            asset_concat[idx] = pd.DataFrame()
            
            print('\nCLIENT-ERROR: WE COULD NOT DOWNLOAD SPLIT DATA FOR %s\n' % fileName)
     
    print('\n\n\n\n')
        
    # --------------------------------------------
    # Liability & Equity Unstructured Database
    # --------------------------------------------
    print('\nLiability & Equity Unstructured Database')
    for idx, csv in enumerate(liable_paths):
        
        # decompose csv name into corresponding terms
        fileName, filing_date, fiscal_year, cik = extra_cols(csv)
        
        try:
            # first load in both the PNG and PDF split balance sheets
            # NOTE: All these balance sheets are cleaned numerical values
            s3.download_file(s3_bucket, csv, 'temp.csv')
            pdf_df = pd.read_csv('temp.csv')
            s3.download_file(s3_bucket, png_liable_folder + fileName, 'temp.csv')
            png_df = pd.read_csv('temp.csv')
            os.remove('temp.csv')

            print('Working on %s-%s' % (cik, filing_date))
            
            # run accounting check to remove sub-totals for each respective line-item
            temp_df1, total_flag1, total_amt1 = totals_check(pdf_df)
            temp_df2, total_flag2, total_amt2 = totals_check(png_df)
            
            # do a special merge that combines unique line items names between PDF & PNG
            df = special_merge(temp_df1, temp_df2, '0')
            
            #########################
            # Exporation assumption
            #########################
            
            # if no pdf or png returns a total asset flag then we want to merge
            # otherwise we simply see use the first value
            
            if (total_flag1 == 1) or (total_flag2 == 1):
                
                if total_flag1 == 1:
                    # construct row for the unstructured data frame 
                    export_df = unstructured_data(temp_df1, filing_date, fiscal_year, cik, cik2brokers)
                    
                    # we have that "total asset" was found and matches
                    export_df["Total liabilities & shareholder's equity"] = total_amt1
                    
                elif total_flag2 == 1:
                    # construct row for the unstructured data frame 
                    export_df = unstructured_data(temp_df2, filing_date, fiscal_year, cik, cik2brokers)
                    
                    export_df["Total liabilities & shareholder's equity"] = total_amt2
                    
            # we have that "total asset" was found, but did not match correctly
            elif (total_flag1 == 0) or (total_flag2 == 0):
                
                # do a special merge that combines unique line items names between PDF & PNG
                df = special_merge(temp_df1, temp_df2, '0')
                export_df = unstructured_data(df, filing_date, fiscal_year, cik, cik2brokers)
             
            # we have that no "total asset" figure was found
            elif (total_flag1 == 2) and (total_flag2 == 2):
                
                # do a special merge that combines unique line items names between PDF & PNG
                df = special_merge(temp_df1, temp_df2, '0')
                export_df = unstructured_data(df, filing_date, fiscal_year, cik, cik2brokers)

            # stores the reported data frame 
            liable_concat[idx] = export_df
        
        # in the event we can't download file from s3 (i.e. does not exist, we ignore the )
        except botocore.exceptions.ClientError:
            
            # assign an empty DataFrame and print out error
            liable_concat[idx] = pd.DataFrame()
            
            print('\nCLIENT-ERROR: WE COULD NOT DOWNLOAD SPLIT DATA FOR %s\n' % fileName)
    
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
        s3.put_object(Bucket=s3_bucket, Key=out_folder + 'unstructured_assets.csv', Body=data)
    os.remove(filename)
          
    # writing data frame to .csv file
    liable_df = pd.concat(liable_concat)     
    liable_df = reorder_columns(liable_df, 
                                col_preserve=['CIK', 'Name', 'Filing Date', 'Filing Year'])    

    filename = 'unstructured_liable.csv'
    liable_df.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3.put_object(Bucket=s3_bucket, Key=out_folder + 'unstructured_liable.csv', Body=data)
    os.remove(filename)
          
    print('\n===================\nStep 7: Unstructured Database has been Created\n===================')
               
    # ==============================================================================
    #      STEP 8 (Develop an Structured Asset and Liability & Equity Database)
    # ==============================================================================      
          
    # retrieving the unstructured asset values file from s3 bucket
    s3.download_file(s3_bucket, out_folder + 'unstructured_assets.csv', 'unstructAsset.csv')
    s3.download_file(s3_bucket, out_folder + 'unstructured_liable.csv', 'unstructLiable.csv')

    # load in asset and liability dataframes
    assetDF = pd.read_csv('unstructAsset.csv')
    liableDF = pd.read_csv('unstructLiable.csv')

    # remove local file after it has been created (variable is stored in memory)
    os.remove('unstructAsset.csv')
    os.remove('unstructLiable.csv')      
    
    # retrieving the asset and liability classification modesl from s3 bucket
    s3.download_file(s3_bucket, asset_model, 'asset_mdl.joblib')
    s3.download_file(s3_bucket, liability_model, 'liable_mdl.joblib')
    
    # load in asset and liability models to be used for prediction
    assetMDL = load('asset_mdl.joblib')
    liableMDL = load('liable_mdl.joblib')

    # remove local file after it has been created (variable is stored in memory)
    os.remove('asset_mdl.joblib')
    os.remove('liable_mdl.joblib')      
          
    # text vectorizer to format line items to be accepted in the classification model 
    str_mdl = HashingVectorizer(strip_accents='unicode', lowercase=True, analyzer='word', n_features=1000, norm='l2')
    
    # the non-prediction columns are stationary (we don't predict anything)
    non_prediction_columns = ['CIK', 'Name', 'Filing Date', 'Filing Year']
    a_columns = assetDF.columns[~np.isin(assetDF.columns)]
    l_columns = liableDF.columns[~np.isin(liableDF.columns)]
    
    # Use classification model to predict label names for each line item
    asset_label_predictions = assetMDL.predict(str_mdl.fit_transform(a_columns))
    liable_label_predictions = liableMDL.predict(str_mdl.fit_transform(l_columns))
    
    # structured database for asset and liability terms 
    struct_asset_map = pd.DataFrame([a_columns, asset_label_predictions], 
                                    index=['LineItems', 'Labels']).T

    struct_liable_map = pd.DataFrame([l_columns, liable_label_predictions], 
                                     index=['LineItems', 'Labels']).T
    
    # construct the line-item prediction classes with corresponding probabilites 
    a_proba_df = prediction_probabilites(a_columns, assetMDL, str_mdl)
    l_proba_df = prediction_probabilites(l_columns, liableMDL, str_mdl)
    
    # ------------------------------------------------------------------------------
    # Auxillary Database Files 
    # ------------------------------------------------------------------------------
    
    filename = 'asset_prediction_proba.csv'
    a_proba_df.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3.put_object(Bucket=s3_bucket, Key=out_folder + 'asset_prediction_proba.csv', Body=data)
    os.remove(filename)
    
    filename = 'liable_prediction_proba.csv'
    l_proba_df.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3.put_object(Bucket=s3_bucket, Key=out_folder + 'liable_prediction_proba.csv', Body=data)
    os.remove(filename)
    
    filename = 'asset_name_map.csv'
    struct_asset_map.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3.put_object(Bucket=s3_bucket, Key=out_folder + 'asset_name_map.csv', Body=data)
    os.remove(filename)
          
    filename = 'liability_name_map.csv'
    struct_liable_map.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3.put_object(Bucket=s3_bucket, Key=out_folder + 'liability_name_map.csv', Body=data)
    os.remove(filename)
          
    # ------------------------------------------------------------------------------
    # Database construction 
    # ------------------------------------------------------------------------------
    
    # structured database for asset terms 
    struct_asset_df = structured_data(unstructured_df=assetDF, 
                                      cluster_df=struct_asset_map, 
                                      col_preserve=non_prediction_columns)
    
    # we drop ammended releases, preserving unique CIKs with Filing Year (default to first instance)
    struct_asset_df = struct_asset_df.drop_duplicates(subset=['CIK', 'Filing Year'], keep='first')
    
    # extract all line items to reconstruct the appropriate total categories and compute relative differences
    asset_lines = struct_asset_df.columns[~np.isin(struct_asset_df.columns,
                                                   ['CIK', 'Name', 'Filing Date', 'Filing Year',  'Total assets'])]
    struct_asset_df['Reconstructed Total assets'] = struct_asset_df[asset_lines].sum(axis=1)
    
    # construct absolute relative error, differencing returned Total assets from our reconstructed values
    struct_asset_df['Relative Error'] = abs(struct_asset_df['Reconstructed Total assets'] - struct_asset_df['Total assets']) / struct_asset_df['Total assets']

    struct_asset_df['Total asset check'] = struct_asset_df['Relative Error'].apply(relative_indicator)
    
    filename = 'structured_asset.csv'
    struct_asset_df.to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3.put_object(Bucket=s3_bucket, Key=out_folder + 'structured_asset.csv', Body=data)
    os.remove(filename)
          
    # ------------------------------------------------------------------------------
          
    # structured database for liability terms 
    struct_liable_df = structured_data(unstructured_df=liableDF, 
                                       cluster_df=struct_liable_map, 
                                       col_preserve=non_prediction_columns)
    struct_liable_df = struct_liable_df.drop_duplicates(subset=['CIK', 'Filing Year'], keep='first')
    
    # extract all line items to reconstruct the appropriate total categories and compute relative differences
    liable_lines = struct_liable_df.columns[~np.isin(struct_liable_df.columns, 
                                            ['CIK', 'Name', 'Filing Date', 'Filing Year',  
                                             "Total liabilities and shareholder's equity"])]
    
    # we remove all other premature totals from the reconsturctured
    struct_liable_df["Reconstructed Total liabilities and shareholder's equity"] = struct_liable_df[liable_lines].sum(axis=1) 
    struct_liable_df["Reconstructed Total liabilities and shareholder's equity (less total liabilites)"] = struct_liable_df[liable_lines].sum(axis=1) - struct_liable_df['Total liabilities'].fillna(0)
    struct_liable_df["Reconstructed Total liabilities and shareholder's equity (less total equity)"] = struct_liable_df[liable_lines].sum(axis=1) - struct_liable_df["Total shareholder's equity"].fillna(0)
    struct_liable_df["Reconstructed Total liabilities and shareholder's equity (less total L+E)"] = struct_liable_df[liable_lines].sum(axis=1) - struct_liable_df['Total liabilities'].fillna(0) - struct_liable_df["Total shareholder's equity"].fillna(0)
    
    # constructing measures of relative erorrs against each different reconstruction frameworks
    struct_liable_df['Relative Error1'] = abs(struct_liable_df["Reconstructed Total liabilities and shareholder's equity"] - struct_liable_df["Total liabilities and shareholder's equity"]) / struct_liable_df["Total liabilities and shareholder's equity"]
          
    struct_liable_df['Relative Error2'] = abs(struct_liable_df["Reconstructed Total liabilities and shareholder's equity (less total liabilites)"] - struct_liable_df["Total liabilities and shareholder's equity"]) / struct_liable_df["Total liabilities and shareholder's equity"]
          
    struct_liable_df['Relative Error3'] = abs(struct_liable_df["Reconstructed Total liabilities and shareholder's equity (less total equity)"] - struct_liable_df["Total liabilities and shareholder's equity"]) / struct_liable_df["Total liabilities and shareholder's equity"]
          
    struct_liable_df['Relative Error4'] = abs(struct_liable_df["Reconstructed Total liabilities and shareholder's equity (less total L+E)"] - struct_liable_df["Total liabilities and shareholder's equity"]) / struct_liable_df["Total liabilities and shareholder's equity"]

    struct_liable_df["Total liabilities & shareholder's equity check"] = struct_liable_df[['Relative Error1', 'Relative Error2', 'Relative Error3', 'Relative Error4']].apply(relative_indicator, axis=1)
    struct_liable_df["Relative Error"] = struct_liable_df[['Relative Error1', 'Relative Error2', 'Relative Error3', 'Relative Error4']].apply(relative_finder, axis=1)
    
    filename = 'structured_liability.csv'
    struct_liable_df[struct_liable_df.columns[~np.isin(struct_liable_df.columns, 
                                                       ['Relative Error1', 'Relative Error2', 
                                                        'Relative Error3', 'Relative Error4'])]].to_csv(filename, index=False)
    with open(filename, 'rb') as data:
        s3.put_object(Bucket=s3_bucket, Key=out_folder + 'structured_liability.csv', Body=data)
    os.remove(filename)
          
    print('\n===================\nStep 8: Structured Database has been Created\n===================')
    