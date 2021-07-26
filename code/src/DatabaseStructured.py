#!/usr/bin/env python
# coding: utf-8

"""
DatabaseStructured.py: The script works to construct a large structured
database, where line items are created from our predictive model and our 
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import pandas as pd
import numpy as np


##################################
# USER DEFINED FUNCTIONS
##################################

def manual_cl_merge(prediction_df:pd.DataFrame, ttraing_df:pd.DataFrame) -> pd.DataFrame:
    """
    Constructs a merge to overwrite poor model classifications 
    
    Parameters
    ----------
    prediction_df : pandas.DataFrame
        a pandas dataframe highlighting the prediction for a set of line items
        according to classification model specifications
    
    ttraing_df : pandas.DataFrame
        a pandas dataframe of manually classified line items used for
        testing/training the classification model
    """
    
    pd.options.mode.chained_assignment = None  # default='warn' - we ignore for the remapping
    
    # dictionary mapping lineitems -> classification label
    remapping = dict(ttraing_df.values)
    
    # divide the prediction dataframe into rows that match and don't match the training-testing set
    top_half = prediction_df[np.isin(prediction_df.Lineitems, ttraing_df.Lineitems)]
    bot_half = prediction_df[~np.isin(prediction_df.Lineitems, ttraing_df.Lineitems)]
    
    # replace all predicted labels in the top-half with manual classifications
    top_half['Labels'] = top_half['Lineitems'].replace(remapping)
    
    return pd.concat([top_half, bot_half])

def structured_data(unstructured_df:pd.DataFrame, cluster_df:pd.DataFrame, col_preserve:list) -> pd.DataFrame:
    """
    Constructs a structured dataset from an unstructured column set
    
    Parameters
    ----------
    unstructured_df : pandas.DataFrame
        unstuructured pandas dataframe with loose column construction 
    
    cluster_df : pandas.DataFrame
        a pandas dataframe of clustered labels and corresponding line items
        
    col_preserve : list
        a list of columns to preserve when performing comprehension
    """
    
    structured_df = pd.DataFrame()
    label_names = np.unique(cluster_df.Labels.values)
    remap = {}
    
    # assume that the there exists columns 'CIK' and 'Year' for unstructured data
    structured_df = unstructured_df[col_preserve]
    
    for label in label_names:
        data = cluster_df[cluster_df['Labels'] == label]['Lineitems']     # filter by corresponding cluster
        
        # we first select all predicted columns, then sum across rows for only numeric figures
        selection = unstructured_df[data.values]
        
        sumV = selection.sum(axis=1, numeric_only=True)
        
        # we then select rows from the original unstructured dataframe with 
        # only np.nan and convert sumV index to np.nan
        sumV[selection.isnull().all(axis=1)] = np.nan
        
        # assign dictionary to have labels and matching vector
        remap[label] = sumV
    
    # return remapped structured dataframe 
    structured_df = structured_df.assign(**remap)   
    return structured_df

def prediction_probabilites(line_items:np.array, clf_mdl, vec_mdl) -> pd.DataFrame:
    """
    Constructs a mapping convention for the machine learning predictions 
    
    Parameters
    ----------
    line_items : pandas.DataFrame
        list of all unstructured line item names
    
    clf_mdl : pandas.DataFrame
        a classification model to convert a line item 
        
    vec_mdl : list
        a feature extraction model for string/text data 
    """
    
    # predict the corresponding class for each line item
    prediction = pd.DataFrame(data=clf_mdl.predict(vec_mdl.fit_transform(line_items)), 
                              columns=['Manual Classification'])    # actually a predicted class, but this naming
                                                                    # convention helps in concatination
    
    # the actual line items that are used as predictors
    lines = pd.DataFrame(line_items, columns=['Lineitems'])
    
    # compute the probability for each prediction to the accompanying classes
    prediction_probability = pd.DataFrame(data=clf_mdl.predict_proba(vec_mdl.fit_transform(line_items)),
                                          columns=clf_mdl.classes_)
    
    # compute the maximum value across prediction probabilites
    prediction_probability['Max Prediction score'] = prediction_probability.max(axis=1) 
    
    # sum across row, determines total class probability measure 
    # NOTE: each class is distributed 0.0-1.0, so total row wise sums equal 1
    prediction_probability['Total Prediction score'] = prediction_probability[prediction_probability.columns[:-1]].sum(axis=1) 
    
    # join the line items to the prediction probabilities
    return lines.join(prediction).join(prediction_probability)

def relative_indicator(pct):
    """
    Determines the level of matching accuracy for a particular firm/year
    """
    def indicator(x):
        
        # from an array determine the minimum relative error
        if type(x) is float: y = x
        else: y = min(x)                   
        
        if y == 0: return 'PERFECT MATCH'
        if 0 < y < 0.01: return 'BOUNDED MATCH'
        if y >= 0.01: return 'GROSS MISMATCH'
        if np.isnan(y): return 'NOT FOUND'
    
    vFunc = np.vectorize(indicator)      # vectorize function to apply to numpy array
    cleanValue = indicator(pct)          # apply vector function
    
    return cleanValue

def relative_finder(pct):
    """
    Determines the level of matching accuracy for a particular firm/year
    """
    def min_find(x): return min(x)
    
    vFunc = np.vectorize(min_find)       # vectorize function to apply to numpy array
    cleanValue = min_find(pct)           # apply vector function
    
    return cleanValue

def structured_wrapper(asset_df, liable_df, asset_training, liable_training, hashing_model, 
                       asset_model, liable_model) -> tuple:
    """
    Re-order the completed DataFrame by ordering the CIK, Name, 
    Filing Data and Filing Year. 
    
    Parameters
    ----------
    asset_df : pandas.DataFrame
        The asset side balance sheet for a broker-dealer derivied from 
        PDFs/PNGs
        
    liable_df : pandas.DataFrame
        The liability & equity side balance sheet for a broker-dealer 
        derivied from PDFs/PNGs
        
    asset_training : pandas.DataFrame
        The classification training set for asset line items 
        
    liable_training : pandas.DataFrame
        The classification training set for liability & equity line items 
        
    hashing_model : sklearn.HashingVectorizer
        A HashingVectorizer model for converting text/string to numerics
        
    asset_model : joblib
        A log-regression model for predicting asset class items
        
    liable_model : joblib
        A log-regression model for predicting liability & equity class items
    """
    
    # the non-prediction columns are stationary (we don't predict anything)
    non_prediction_columns = ['CIK', 'Name', 'Filing Date', 'Filing Year']
    
    # select columns that do not belong to the non-prediction columns list
    a_columns = asset_df.columns[~np.isin(asset_df.columns, non_prediction_columns)]
    l_columns = liable_df.columns[~np.isin(liable_df.columns, non_prediction_columns)]
    
    # Use classification model to predict label names for each line item
    asset_label_predictions = asset_model.predict(hashing_model.fit_transform(a_columns))
    liable_label_predictions = liable_model.predict(hashing_model.fit_transform(l_columns))
    
    # structured database for asset and liability terms 
    struct_asset_map = pd.DataFrame([a_columns, asset_label_predictions], 
                                    index=['Lineitems', 'Labels']).T

    struct_liable_map = pd.DataFrame([l_columns, liable_label_predictions], 
                                     index=['Lineitems', 'Labels']).T
    
    # assigning variables in accordance with manual classification sets
    struct_asset_map = manual_cl_merge(struct_asset_map, asset_training)
    struct_liable_map = manual_cl_merge(struct_liable_map, liable_training)
    
    # construct the line-item prediction classes with corresponding probabilites 
    a_proba_df = prediction_probabilites(a_columns, asset_model, hashing_model)
    l_proba_df = prediction_probabilites(l_columns, liable_model, hashing_model)
    
    # ------------------------------------------------------------------------
    
    # structured database for asset terms 
    struct_asset_df = structured_data(asset_df, struct_asset_map, non_prediction_columns)
    
    # we drop ammended releases, preserving unique CIKs with Filing Year (default to first instance)
    struct_asset_df = struct_asset_df.drop_duplicates(subset=['CIK', 'Filing Year'], keep='first')
    
    # extract all line items to reconstruct the appropriate total categories and compute relative differences
    asset_lines = struct_asset_df.columns[~np.isin(struct_asset_df.columns,
                                                   ['CIK', 'Name', 'Filing Date', 'Filing Year',  'Total assets'])]
    struct_asset_df['Reconstructed Total assets'] = struct_asset_df[asset_lines].sum(axis=1)
    
    # construct absolute relative error, differencing returned Total assets from our reconstructed values
    struct_asset_df['Relative Error'] = abs(struct_asset_df['Reconstructed Total assets'] - struct_asset_df['Total assets']) / struct_asset_df['Total assets']

    struct_asset_df['Total asset check'] = struct_asset_df['Relative Error'].apply(relative_indicator)
    
    # ------------------------------------------------------------------------
    
    # structured database for liability terms 
    struct_liable_df = structured_data(liable_df, struct_liable_map, non_prediction_columns)
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
    
    # export all neccessary dataframes constructed
    return struct_asset_map, struct_liable_map, a_proba_df, l_proba_df, struct_asset_df, struct_liable_df