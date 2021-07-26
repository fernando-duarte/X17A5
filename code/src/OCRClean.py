#!/usr/bin/env python
# coding: utf-8

"""
OCRCleaning.py: Responsbile for cleaning the AWS Textract DataFrames. We also 
handle idiosyncratic Textract reads and convert to numerical values. 
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################:
 
import re
import os
import trp
import time
import minecart

import numpy as np
import pandas as pd

from fuzzywuzzy import fuzz
from smart_open import open


##################################
# USER DEFINED FUNCTIONS
##################################

def num_strip(number):
    """
    This function converts a string to a numeric quantity, handles weird 
    string format. We handle input arguments of a str, int or numpy.ndarray
    
    Parameters
    ----------
    number : str/int/numpy.ndarray
        An element that may contain either a numeric value
        or not, hidden behind additional non-numeric characters
    """
    
    numType = type(number)

    # if provided a non-empty string, perform regex operation 
    if (numType is str) and (len(number) > 0):

        # check for accounting formats that use parenthesis to signal losses 
        if number[0] == '(': number = '-' + number

        # case replacing to handle poor textract reading of numbers
        number = number.replace('I', '1').replace('l', '1')

        # --------------------------------------------------------------
        # Explanation of the Regex Expression:
        #      [^0-9|.|-]     = match all elements that are not numeric 0-9, periods "." or hyphens "-"
        #      (?<!^)-        = match all elements that are hyphens "-" not in the first index position
        #      \.(?=[^.]*\.)  = match all elements that are periods "." except the last instance
        # --------------------------------------------------------------

        check1 = re.sub("[^0-9|.|-]", "", number)         # remove all the non-numeric, periods "." or hyphens "-"
        check2 = re.sub("(?<!^)-", "", check1)            # removes all "-" that aren't in the first index 
        check3 = re.sub("\.(?=[^.]*\.)", "", check2)      # removes all periods except the last instance of "." 

        # --------------------------------------------------------------

        # we consider weird decimal values that exceed 2 spaces to the right (e.g. 432.2884)
        period_check = check3.find('.')                         # returns the location of the period 
        right_tail_length = len(check3) - period_check - 1      # right-tail length should not exceed 2

        # if more than 2 trailing digits to decimal point we assume incorrect placement
        if right_tail_length > 2:
            check3 = check3.replace('.', '')

        # last check against poor lagging formats e.g. "." or "-" to return nan or floating-point number
        if (check3 == '-') or (check3 == '.'):
            return 0.0
        else:
            # try to cast to floating point value, else flat NaN
            try: return float(check3)
            except ValueError: 
                return np.nan

    # if operator is an integer or float then simply return the value
    elif (numType is int) or (numType is float):
        return number

    else:
        return np.nan

def column_purge(df:pd.DataFrame) -> pd.DataFrame:
    """
    Column designed to filter out rows that are NaN (empty) 
    and reduce dataframe size from (N1xM) -> (N2xM) where
    N1 >= N2 in size
    
    Parameters
    ----------
    df : pandas.DataFrame
        A dataframe object that corresponds to the X-17A-5 filings
    """
    
    # begin by filtering out the NaN rows present in the first column
    first_col = df.columns[0]
    new_df = df[np.isin(df[first_col], df[first_col].dropna())]    # select subset of rows 
    
    # we reset the index of our new_df to recoup a consecutive index count
    new_df = new_df.reset_index()
    new_df = new_df[new_df.columns[1:]]    # skip the first column since we reset the index
    
    return new_df


"""
Table column merging
--
For tables with three columns we merge the last two columns into a once unique column
"""

def merge(df:pd.DataFrame) -> pd.DataFrame:
    """
    Function passes a special dataframe, and reduces its dimensions
    accordingly. Example releases include, but are note limited to, 
    1224385-2016 and 72267-2003 for FOCUS reports
    
    e.g.
    
    Converts a wide dataframe, balance sheet into a smaller rectangular form
                  0                                                 1                 2
            ====================================================================================
        0   Assets                                          | NaN            | NaN  
        1   Cash and cash equivalents                       | $ 606,278      |     
        2   Cash and securities segregated pursuant         | 273,083        | 
        3   Collateralized short-term financing agreements: | NaN            | $ 1,345
    
    
    Rectangular form of the the dataframe ->
                   0                                                 1          
            =====================================================================
        0   Assets                      
        1   Cash and cash equivalents                       | $ 606,278        
        2   Cash and securities segregated pursuant         | 273,083        
        3   Collateralized short-term financing agreements: | $ 1,345            
    
    Parameters
    ----------
    df : pandas.DataFrame
        A dataframe object that corresponds to the X-17A-5 filings
    """
    
    # work on itterative merging for rows, check left/right and top/bottom
    n = df.shape[0]
    trans = []

    for i in range(n):
        row = df.iloc[i]         # index into the row

        name = row.iloc[0]       # the line item name (e.g. Total Assets)
        col1 = row.iloc[1]       # the first value(s) column
        col2 = row.iloc[2]       # the second value(s) column 
        
        # ----------------------------------------------
        # NOTE: We say nothing if both col 1 and 2 are 
        #     both populated with a numeric value
        # ----------------------------------------------
        
        if num_strip(col1) is not np.nan:
            trans.append([name, col1])        # if column 1 has a numeric value we take it by default
        elif num_strip(col2) is not np.nan:
            trans.append([name, col2])        # if column 1 has no numeric value, but column 2 does, we take it
            
        # ----------------------------------------------
        
        # we want to check if there exists two NaNs - is it real or false flag
        if (col1 is np.nan) and (col2 is np.nan): 
            
            # look up one row (if possible to see if col1 and col2 are populated)
            try:
                # check the information for the above row
                indexer = i-1
                
                # we don't want to do reverse lookup with negatives
                if indexer > 0:
                    prior_row = df.iloc[indexer]                 # previous dataframe row 
                    prior_col1 = prior_row.iloc[1]               # first column from previous row
                    prior_col2 = prior_row.iloc[2]               # second column from previous row

                    # if both values present then we simply use the right hand side value above  
                    if (prior_col1 is not np.nan) and (prior_col2 is not np.nan):
                        trans.append([name, prior_col2])
            
            # IndexError if not possible to look up one row       
            except IndexError: pass
    
    return pd.DataFrame(trans)


"""
Table Row Split
--
Since many of the existing tables run the risk of overlapping rows we work to split these rows to appropriate values
"""

def dollar_check(num) -> bool:
    """
    A function to check the presence of a '$' or 'S'. 
    This function is used to complement our row splits 
    function to determine "True splits"
    
    Parameters
    ----------
    num : str/int
        An element either a str or int
    """
    
    if num not in ['$', 'S']: return True
    else: return False

def row_split(df:pd.DataFrame, text_file:dict) -> pd.DataFrame:
    """
    Function designed to split conjoined rows from balance 
    sheet dataframes into individual rows. Example releases 
    include, but are note limited to, 42352-2015, 58056-2009
    
    Parameters
    ----------
    df : pandas.DataFrame
        References the balance sheet read in from AWS Textract
        
    text_file : dict
        Stores text values with corresponding confidence level 
        from balance sheet pages read from AWS Textract
    """
    
    # ##############################################################
    # NESTED HELPER FUNCTIONS
    # ##############################################################
    
    def find_row_splits(val) -> bool:
        """
        Compute a boolean measure to assess whether a row is conjoined or not. We make
        the assumption that a row is conjoined or merged if there exists a space in the 
        first value column (omiting the dollar sign $ and S which may be read in)
        """
        try:
            # split the data figures for each balance sheet figure
            arr = val.split(' ')
            
            # remove the '$' sign or 'S' if present in the list (this helps avoid false pasitives) 
            arr = list(filter(dollar_check, arr))
            
            # if length of read list exceeds 1 then we know there exists a multi-row bunch
            if len(arr) > 1:
                return True
            else: return False
        
        # handle exception for NaN (no attribute to split) 
        except AttributeError: return False
    
    def extract_lineitems(line:list, value:list, dictionary:dict) -> list:
        """
        Extract the appropriate line items from each line value. We 
        use a set of assumptions with respect to left/right side splits
        to determine appropriate return values. 
        """
        splits = []
        
        # iterate through each line item
        for i in dictionary.keys():
    
            # we check for real key-value names avoiding single character keys
            if len(i) > 1: 
                idx = line.find(i)    # find the index of key-value (if possible) in line item array

                # if we find such a value we append the series (failure to find results idx = -1)
                if idx >= 0: splits.append(i)
        
        # check whether we have a one-to-one mapping between line items and line values, 
        # e.g. ['Assets', 'Cash', 'Recievables'] -> ['1,233', '4,819'] (3x2 mapping)
        n = len(splits) - len(value)
        
        if n == 0:
            return (splits, value)           # if n is equal to zero we have a "perfect" match
        elif n > 0:
            return (splits[n:], value)       # more line items terms, assume values is right
        elif n == -1:                        
            return (splits, values[1:])      # more value terms, assume value is wrong only if difference is 1 in size
        else: 
            return None                      # no specific rule paradigm (more values than items)
    
    def recursive_splits(values:list, lineName:list, sub=[]) -> pd.DataFrame:
        """
        Recursively breaks up merged rows for each split until no 
        merged row is left, applying recursion by split.
        """
        # if our list exceeds 1 in length, we continue to split
        if len(values) > 1:
            # construct a dataframe row of the first split term to append to sub list
            row = pd.DataFrame([lineName[0], values[0]]).T
            sub.append(row)
            
            # we pass the +1 index splits and line name, appending the first-most layer 
            return recursive_splits(values[1:], lineName[1:], sub=sub)
        else:
            row = pd.DataFrame([lineName[0], values[0]]).T
            sub.append(row)
            
            # we concatenate all DataFrames vertically to form a large DataFrame 
            return pd.concat(sub)
        
    # ##############################################################
    # ##############################################################    
    
    # select all the rows that match our description, where a space exists = row merge 
    selections = df[df[df.columns[1]].apply(lambda x: find_row_splits(x))]
    idxs = selections.index
    
    # iterate through each row that is determined to be conjoined
    for i in idxs:
        
        # find the index location od merged row
        row_idx = np.argmax(df.index == i)
        
        # slice dataframe according to the idx selection (we search for all periods were a break occurs)
        top = df.iloc[:row_idx]
        bottom = df.iloc[row_idx+1:]

        # divide the identified term from the selection e.g. "$ 9,112,943 13,151,663" -> ["$", "9,112,943", "13,151,663"] 
        # and filter out the $ sign in the list e.g. ["$", "9,112,943", "13,151,663"] -> [9,112,943", "13,151,663"]
        values = df[df.columns[1]].loc[i].split(' ')
        values = list(filter(dollar_check, values))
        
        # extract line names and corresponding values according to Text parsed list (requires parsed TEXT JSON)
        # e.g. ['Securities Held Total Assets'] -> ['Securities Held', 'Total Assets']
        lineName = df[df.columns[0]].loc[i]
        
        # return line items and values that should match in size
        response_extraction = extract_lineitems(lineName, values, text_file)
        
        # if we retun a lineitem then we can perform recursive splits (otherwise avoid)
        if type(response_extraction) is not type(None):
            
            clean_lineitems, clean_values = response_extraction
            
            # determine the splits for the corresponding row
            mid = recursive_splits(clean_values, clean_lineitems, sub=[])
            mid.columns = df.columns

            # re-assign the value of df2 to update across each iteration
            df = pd.concat([top, mid, bottom])
            
        else:
            # no need for mid, since we have removed it from existence
            df = pd.concat([top, bottom])
        
    return df


"""
Numeric Conversion
--
Convert elements in balance sheet to numeric quantities
"""

def numeric_scaler(text_dict:dict, key_value:str, old_cik:int, old_scale:float) -> float:
    """
    Function used for scaling accounting figures by reported unites
    
    Parameters
    ----------
    text_dict : dict
        Stores text values with corresponding confidence level 
        from balance sheet pages read from AWS Textract
        
    key_value : str
        The current lookup CIK to access contents from AWS
        Textract text dictionary, created previously from 
        the OCRTextract.py script
        
    old_cik : int
        The CIK of the previously examined broker-dealer
    
    old_scale : dict
        The old scaler of the previously examined broker-dealer
    """
    
    scalar = {'thousands': 1e3, 'hundreds':1e2, 'millions':1e6, 'billions': 1e9}
    
    text_data = text_dict[key_value]
    
    # iterate through each of the text values from dictionary map
    for text_value in text_data.keys():
        
        # we check to see whether the text is found in our scalar dictionary
        for scale_type in scalar.keys():
            
            # search for the presence of the scale identifier (e.g. millions) 
            # we use a "fuzzy-ratio" on string splits to handle embeeded keyes (e.g. Dollar in Millions)
            scale_search = [fuzz.ratio(scale_type.lower(), elm.lower()) for elm in text_value.lower().split(' ')]
            
            # we make the assumption that a score of 90 or greater, signals a match
            if max(scale_search) >= 90:
                return scalar[scale_type]             
    
    if old_cik == key_value.split('-')[0]:
        return old_scale
    
    # default to no multiplier (1)
    return 1

def numeric_converter(value):
    """
    This function is a wrapper for calling the numerical extraction 
    function handling case type and vectorization 
    
    Parameters
    ----------
    value : str/int/numpy.ndarray
        String with hidden numeric quanity (e.g. $ 19,225 = 19255)  
    """
    
    assert type(value) is str or int or np.ndarray, 'Value must be of type string, integer, float or numpy array'
    
    # checks to see what type of value is being provided
    operator = type(value)
    
    # if provided a string, perform regex operation 
    if (operator is str) and (len(value) > 0):
        return num_strip(value)
    
    # if operator is integer then simply return the value, no need to modify 
    elif (operator is int) or (operator is float):
        return value 
    
    # if operator is numpy array then we perform a extraction per element in array
    elif (operator is np.ndarray):
        vFunc = np.vectorize(num_strip)      # vectorize function to apply to numpy array
        cleanValue = vFunc(value)            # apply vector function
        return cleanValue 

    
"""
Idiosyncratic Changes
--
Handle idiosyncratic changes to balance-sheets providing slight modification
"""

def jpm_check(df:pd.DataFrame) -> pd.DataFrame:
    """
    A wrapper function that reduces the amount of rows present 
    within special J.P. Morgan releases that contain a special 
    sub-balance sheet for VIE figures (helps prevent errors)
    
    Parameters
    ----------
    df : pandas.DataFrame
        Original unfiltered pandas.DataFrame object representing 
        balance sheet figures
    """
    
    arr = df[df.columns[0]]
    
    # iterate through each line item
    for idx, line_item in enumerate(arr):
        
        try:
            # our key phrase is "(a) The following table..." found in J.P. Morgan filings with VIE
            check1 = re.search('\(a\) The following table', line_item, flags=re.I)
            check2 = re.search('\(a\) The follow', line_item, flags=re.I)
            
            if check1 is not None or check2 is not None:
                # remove all the line below the condition being met
                return df.iloc[:idx] 
            
        # trying to perform regex on a NaN object (not-compatible)
        except TypeError: pass
        
    return df

def idio_chg(df:pd.DataFrame, base_file:str) -> pd.DataFrame:
    """
    Function is responsible for handling idiosyncratic changes 
    for each Textract version we encounter 
    
    Parameters
    ----------
    df : pandas.DataFrame
        Original unfiltered pandas.DataFrame object representing 
        balance sheet figures
        
    base_file : str
        Base file for a particular broker-dealer recorded as 
        CIK-YYYY-MM-DD used to determine which modification should
        be made/used for a given balance sheet
    """
    
    if base_file == '356628-2006-03-02':
        # Textract fails to read top line items “Cash“ and “Cash and resale agreements segregated under federal regulation“, 
        # resulting in underestimation of total asset value
        temp_df = pd.DataFrame({'0':['Cash', 'Cash and resale agreements segregated under federal regulation'], 
                                '1':[32494000.0, 6813110000.0]})
        
        return pd.concat([temp_df, df])
        
    elif base_file == '318336-2018-03-01':
        # Our backward total checking algorithm removes “Customers“ item incorrectly since it very closely 
        # matches the lookback sum of 3 previous line items
        df = df.replace({13482000000.0 : 13482000111.0, 1030000000.0: 1030000111.0, 12876000000.0: 12876000111.0})
        return df
    
    elif base_file == '318336-2005-03-01':
        # Our backward total checking algorithm removes “Commercial paper“ item incorrectly since it very 
        # closely matches to the above item, “Derivatives contracts“
        df = df.replace({1171000000.0 : 1171000111.0})
        return df
    
    elif base_file == '87634-2020-02-27':
        # Our backward total checking algorithm removes “Goodwill“ item incorrectly since it very closely 
        # matches to the above item, “Equipment, office facilities, and property - net“
        df = df.replace({935000000.0 : 935000111.0})
        return df
    
    elif base_file == '91154-2015-03-02':
        # Our backward total checking algorithm removes “Brokers, dealers and clearing organizations“ item 
        # incorrectly since it very closely matches 'Customers'
        df = df.replace({7584000000.0 : 7584000111.0})
        return df
    
    elif base_file == '91154-2019-03-05':
        # Our backward total checking algorithm removes “Securities received as collateral, at fair value 
        # (all pledged to counterparties)“ item incorrectly since it very closely matches the lookback sum 
        df = df.replace({15877000000.0 : 15877000111.0})
        return df
    
    elif base_file == '89562-2006-01-30':
        # Our backward total checking algorithm removes “Property. equipment and leasehold improvements“ item 
        # incorrectly since it very closely matches 'Others'
        df = df.replace({163000000.0 : 163000111.0})
        return df
    
    elif base_file == '808379-2015-03-02':
        # Our backward total checking algorithm removes “Financial instruments owned, at fair value“ item incorrectly 
        # since it very closely matches the lookback sum of 4 previous line items
        df = df.replace({15263000000.0 : 15263000111.0})
        return df
    
    elif base_file == '356628-2008-02-29':
        # Textract error, where the top of the table (i.e. Cash line) is not read leading to an undercount of the
        # “Total Asset” figure by that amount.
        temp_df = pd.DataFrame({'0': ['Cash'], '1':[103017000]})
        
        return pd.concat([temp_df, df])
    
    elif base_file == '895502-2009-12-30':
        # Textract fails to read top line items “Cash“ resulting in underestimation of total asset value
        temp_df = pd.DataFrame({'0': ['Cash'], '1':[358998000]})
        
        return pd.concat([temp_df, df])
    
    elif base_file == '29648-2010-03-01':
        # Our backward total checking algorithm removes “Accumulated earnings“ item incorrectly since it very 
        # closely matches the lookback sum of 4 previous line items
        df = df.replace({1030000000.0 : 1030000111.0})
        return df

    elif base_file == '42352-2015-03-10':
        # Textract error, which understates the value of the “Securities loaned” returning a value for 
        # 4.151000e+10 instead of 8.151000e+10
        df = df.replace({4.151000e+10 : 8.151000e+10})
        return df
    
    elif base_file == '42352-2017-03-01':
        # Textract error, which understates the value of the “Securities loaned” returning a value for 
        # 4.151000e+10 instead of 8.151000e+10
        df = df.replace({4.340500e+10 : 4.340600e+10})
        return df
    
    elif base_file == '72267-2012-03-15':
        # Due to poor Textract reading we overlap parts of the liabilities values with some of the asset rows, 
        # grossly overestimating the totals
        df = df.drop([11])
        return df
    
    elif base_file == '87634-2010-03-01':
        # Our backward total checking algorithm removes “Retained earnings“ item incorrectly since it very 
        # closely matches “Additional paid-in capital“
        df = df.replace({1079000000.0 : 1079000111.0})
        return df
    
    elif base_file == '72267-2014-05-30':
        df = pd.concat([df.iloc[:12], df.iloc[14:]])  # remove a read-mistep with the "other category"
        
        # Due to poor Textract reading on the PNG file, we omit a singular row which complicates our balance sheet script
        if df[df[1] == 8.105411e+10].empty:
            temp_df1 = pd.DataFrame({0: ['Securities sold under agreements to repurchase'], 
                                     1:[8.105411e+10]})
            return pd.concat([temp_df1, df])
        
        return df
    
    elif base_file == '1146184-2021-02-25':
        # Issues with Textract grossly omitting many rows from the balance sheet table
        temp_df1 = pd.DataFrame({'0':['Cash', 'Securities owned, at fair value', 'Securities borrowed', 
                                      'Receivable from brokers and dealers', 'Receivable from clearing organizations and custodian',
                                      'Securities purchased under agreements to resell'],
                                 '1':[523000000, 66707000000, 1628000000, 841000000, 648000000, 492000000]})
        temp_df2 = pd.DataFrame({'0': ['Total Assets'], '1':[71004000000]})
        
        return pd.concat([temp_df1, df.iloc[:1], temp_df2, df.iloc[1:]])
    
    elif base_file == '91154-2009-03-02': 
        # Our backward total checking algorithm removes “Other financial instruments“ item incorrectly 
        # since it very closely matches “Foreign government securities“ 
        df = df.replace({125000000.0 : 125000111.0, 2.058200e+10: np.nan})
        return df
    
    elif base_file == '91154-2019-03-05':
        # Due to poor Textract reading on the PNG file, we omit a singular row which complicates our balance sheet script
        temp_df1 = pd.DataFrame({'0': ['Short-term borrowing'], 
                                     '1':[508000000]})
        return pd.concat([temp_df1, df])
    
    elif base_file == '808379-2007-03-01':
        # Due to Textract reading, we have double counted the total asset line item, we remove this to avoid 
        # complications in the liability and equity table
        df = df.drop([8])
        return df
    
    elif base_file == '895502-2002-02-28':
        # Due to Textract reading, we have double counted the total asset line item, we remove this to avoid 
        # complications in the liability and equity table
        df = df.replace({2.357964e+09: np.nan})
        return df
    
    elif base_file == '895502-2012-12-28' or base_file == '895502-2014-01-02':
        # Our backward total checking algorithm removes “Liabilities subordinated“ item incorrectly since it 
        # very closely matches “Long-term borrowing“ 
        df = df.replace({1400000000.0 : 1400000111.0, 167769234000.0: 67769234000.0})
        return df
    
    elif base_file == '867626-2013-02-28':
        # our numeric scaler scales by the wrong value using 1e6 as opposed to 1e3, we scale back everything
        df[df.columns[1]] = df[df.columns[1]].apply(lambda x: x / 1e3)
        return df 
    
    elif base_file == '890203-2020-03-02':
        # out numeric scaler can't find the correct value to scale the balance sheet by, we scale manually
        df[df.columns[1]] = df[df.columns[1]].apply(lambda x: x * 1e3)
        return df
    
    return df


"""
Wrapper Scripts designed to execute all checks sequentially
"""

def clean_wrapper(df: pd.DataFrame, textract_text: dict, key: str, file: str, 
                  old_scaler: str, old_cik: str) -> pd.DataFrame:
    """
    A wrapper function that sequentially calls each cleaning function 
    to fix issues that may arise post Textract reading (i.e. Column Merging, 
    Row Splitting, Numeric Conversion)
    
    Parameters
    ----------
    df : pandas.DataFrame
        Original unfiltered pandas.DataFrame object representing 
        balance sheet figures
    
    textract_text : dict
        Stores text values with corresponding confidence level 
        from balance sheet pages read from AWS Textract
        
    key : str
        Base file for a particular broker-dealer recorded as 
        CIK-YYYY-MM-DD used to determine which modification should
        be made/used for a given balance sheet
        
    file : str
        Filename for a particular broker-dealer recorded as 
        CIK-YYYY-MM-DD.csv mapped to a balance sheet
        
    old_scaler : str
        The old scaler of the previously examined broker-dealer
        
    old_cik : str
        The CIK of the previously examined broker-dealer
    """
    
    # re-assign dataframe of balance sheet after cleanse
    df = column_purge(df)
    
    # --------------------------------------------------------------------------------------------------
    # J.P. Morgan Special Case Handle
    # --------------------------------------------------------------------------------------------------
    
    # performs a check to remove uncessary rows for specific J.P. Morgan releases
    df = jpm_check(df)
    
    # --------------------------------------------------------------------------------------------------
    # COLUMN MERGING (IF NECESSARY)
    # --------------------------------------------------------------------------------------------------

    # if columns greater than 2, we have a weird data table that needs to be "merged"
    # NOTE: By construction we never have more than 3 columns present, thanks to our Textract check 
    if df.columns.size > 2:
        df = merge(df)
        print('\tWe merged the columns of %s' % file)

    # --------------------------------------------------------------------------------------------------
    # ROW SPLIT FOR MERGED ROWS (IF NECESSARY)
    # --------------------------------------------------------------------------------------------------

    # check for presence of row splits and correct any if found 
    tempDF = row_split(df, textract_text[key])

    # if difference is found in shape, then a transformation was done 
    if tempDF.shape != df.shape:
        print("\tFixed the merged rows for %s" % file)

    # --------------------------------------------------------------------------------------------------
    # NUMERIC CONVERSION
    # --------------------------------------------------------------------------------------------------

    # pass numeric converter to the column to convert string to numerics
    tempDF[tempDF.columns[1]] = tempDF[tempDF.columns[1]].apply(numeric_converter)

    # remove any NaN rows post numeric-conversion
    postDF = tempDF.dropna().copy()

    # check for potential scaler multipler on cash flows (adjust multiplier if possible)
    scale = numeric_scaler(textract_text, key, old_cik, old_scaler)
    postDF[postDF.columns[1]] = postDF[postDF.columns[1]].apply(lambda x: x * scale)

    print('\tWe converted to numeric figures for %s' % file)
    
    # --------------------------------------------------------------------------------------------------
    # Idiosyncratic changes (specific balance sheet)
    # --------------------------------------------------------------------------------------------------
    
    # performs modification to handle Textract specific errors
    out_df = idio_chg(postDF, key).dropna()
    
    # --------------------------------------------------------------------------------------------------
    # BALANCE SHEET EXPORTATION
    # --------------------------------------------------------------------------------------------------
    print(out_df)
    
    return out_df, scale, key.split('-')[0]
