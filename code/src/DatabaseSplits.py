#!/usr/bin/env python
# coding: utf-8

"""
DatabaseSplits.py: The script distinguishes between both asset and liability
side items from the balance sheet, splitting each table by corresponding
portions to highlight either relationship. 
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import os
import re

import pandas as pd
import numpy as np


##################################
# USER DEFINED FUNCTIONS
##################################

def bsSplit(array: np.ndarray) -> tuple:
    """
    Function splits an array by bisection, into asset and liability 
    & equity terms. Assumes that line items are recorded according 
    to standard accounting practices in orientation.
    
    Parameters
    ----------
    array : numpy.ndarray
        An array of balance sheet line items for a given broker-dealer
        
    NOTE: We make the assumption that liability line items always fall 
          below asset line items and each balance sheet has both asset 
          and liability line items. If either are missing we avoid balance sheet
    """
    
    stop_idx1 = 0
    stop_idx2 = array.size        # default to length of line items array
    
    asset_idx = 0                 # asset and liability identifiers
    liable_idx = 0

    val1 = None
    val2 = None
    
    # iterate through the line items as provided by the array
    val1 = None
    val2 = None
    for i, item in enumerate(array):
        # search string for presence of word 'assets' and 'liability/liabilites'
        val1 = re.search('assets', item, flags=re.I)
        val2 = re.search('liability|liabilities', item, flags=re.I)
        
        # if we find the term "asset" we count this index
        if val1 is not None:
            asset_idx = i + 1    
        
        # if we find the term "liability" we count this index
        if val2 is not None:
            liable_idx = i + 1
        
        # if we find an asset and liability term, we check to see if the asset index appears before
        # the liability index, this is to prevent the JP Morgan 2012/2013 Textract Error
        if (asset_idx != 0) and (liable_idx != 0):
            
            if asset_idx < liable_idx:
                stop_idx1 = asset_idx
                stop_idx2 = liable_idx
                
    # we should always keep track of the asset term (this is our primary splitter)
    if (asset_idx != 0) and (liable_idx == 0):
        stop_idx1 = asset_idx

    # check the very last, in event our liability term created an early cut-off (e.g. 42352-2003-01-28)
    if (val1 is None) & (val2 is None):
        stop_idx2 = array.size
    
    # partition the array by the enumerated index for asset and liability portions
    lhs = array[:stop_idx1]
    rhs = array[stop_idx1:stop_idx2]
    
    # if either asset or liability side missing, we return None
    if lhs.size == 0 or rhs.size == 0: return None
    else:
        return (lhs, rhs, stop_idx1, stop_idx2)

def lineItems(vector:np.ndarray, df:pd.DataFrame):
    """
    Retrieving balance sheet information line item names from
    a given balance sheet, stripped from FOCUS reports
    
    Parameters
    ----------
    array : numpy.ndarray
        An array of balance sheet line items for a given broker-dealer
        
    df : pandas.DataFrame
        The full balance sheet table taken from FOCUS reports
    """
    
    # retrieve the asset and liability & equity terms from the dataframe
    response = bsSplit(vector)
    
    # if response is present we continue 
    if response is not None:
        lhs, rhs, index1, index2 = response     # decompose response object to retrieve index
        
        dfA = df.iloc[:index1]                  # asset dataframe
        dfL = df.iloc[index1:index2]            # liability and equity dataframe
        
        return (dfA, dfL)
    else:
        return None
