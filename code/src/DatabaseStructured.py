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

from joblib import dump, load


##################################
# USER DEFINED FUNCTIONS
##################################

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
        data = cluster_df[cluster_df['Labels'] == label]['LineItems']     # filter by corresponding cluster
        
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
                              columns=['Predicted Class'])
    
    # the actual line items that are used as predictors
    lines = pd.DataFrame(line_items, columns=['Line Items'])
    
    # compute the probability for each prediction to the accompanying classes
    prediction_probability = pd.DataFrame(data=clf_mdl.predict_proba(vec_mdl.fit_transform(line_items)),
                                          columns=clf_mdl.classes_)
    
    # sum across row, determines total class probability measure 
    # NOTE: each class is bounded by 0.0-1.0, so total column wise sums can exceed 1.0
    prediction_probability['Total Prediction score'] = prediction_probability.sum(axis=1) 
    
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
