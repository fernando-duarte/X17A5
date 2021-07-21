#!/usr/bin/env python
# coding: utf-8

"""
DatabaseUnstructured.py: The script works to construct a large unstructured
database, where line items are concatenated across broker-dealer and year
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import re
import requests

import pandas as pd
import numpy as np

from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz


##################################
# USER DEFINED FUNCTIONS
##################################

# numpy exception for handling invalid log10 RunTime error (we opt to not show)
# switch 'ignore' to 'warn', if you would like to flag the RunTime error 
np.seterr(invalid = 'ignore') 

def multiple_check(x1:float, x2:float) -> bool:
    """
    Determine whether the two values are the same number scaled 
    by a factor of 10, we check this by using log power 10
    
    Parameters
    ----------
    x1 : float
        A number corresponding to the current balance sheet value
    
    x2 : float
        A number corresponding to the lookup sum on the balance sheet 
    """
    
    # prevent zero division error since x1 is the denominator and log10 zero division error
    if (x1 == 0) or (x2 == 0): return (x1, False)
    else:
        # if our backward sum is a multiple of 10, we return True 
        # (e.g. Total Assets (x1) 745.2322 vs Backward Sum (x2) 7452322)
        check1 = np.log10(x2 / x1).is_integer()

        # if our backward sum is a substring of a line item, with a difference of one in length, 
        # we return True (e.g. Total Assets (x1) 174182935 vs Backward Sum (x2) 74182935)
        check2 = (str(x2) in str(x1) ) & (len(str(x2)) == len(str(x1)) - 1)

        if check1 or check2: 
            return (x2, True)
        else: 
            return (x1, False)

def epsilon_error(x1:float, x2:float, tol:float=0.01) -> bool:
    """
    Determine whether the two values are within a similar epsilon bound. 
    We default our error tolerance, implying that if two numbers are within 
    a specified toleracnce of one another we "ok" it 
    
    Parameters
    ----------
    x1 : float
        A number corresponding to the current balance sheet value
    
    x2 : float
        A number corresponding to the lookup sum on the balance sheet 
        
    tol : float
        The error tolerance we are willing to accept, default value is 
        set to 0.01 -> 1% (e.g. x1 = 100; x2 = 101; accept)
    """
    
    # avoid zero-division and inf errors
    if (x1 == 0) or (x2 == 0): 
        return False
    
    else:
        # first we convert the numeric quantities into strings
        current = str(x1)
        lookback = str(x2)

        # we only want to check against the relative difference if one element in the number is read wrong
        if len(current) == len(lookback):
            
            # we iterate linearly through each string and check to see the positional match 
            # if we catch a mismatch we flag it with a 1, othewise skip with 0
            changes = [0 if (current[i] == lookback[i]) else 1 for i in range(len(current))]

            # check set differences produce a set with exactly 1 in length
            if sum(changes) == 1:

                diff = abs(x1 - x2)      # compute numeric differences

                # check to see whether an accounting condition was met wihtin a boundary condition
                if abs(diff / x1) <= tol:
                    return True

        return False

def totals_check(df:pd.DataFrame) -> tuple:
    """
    Checks to see if a line row meets the conditon of a total, 
    if true we remove these rows as we make have checked the 
    terms before have meet our conditions 
    
    NOTE: These total strips include major and minor totals, where
          major totals are big ticket line items (e.g. total assets)
          and minor totals are smaller 
    
    Parameters
    ----------
    df : pandas.DataFrame
        A DataFrame that represents the Asset or Liability & Equity 
        portion of the balance sheet from the FOCUS reports
    """
    
    m, n = df.shape                  # unpack the shape of dataframe
    data_col = df.columns[1]         # the values column for balance sheet
    
    total_flag = 2       # default 2 (no measure found), 1 (sum is correct), 0 (sum is not correct)
    total_amt = np.nan
    
    # iterate through each of the line items
    for i in range(m):
        
        # check the value of line items at a given index (forward index)
        item1 = df.loc[i].values[1]
        name = df.loc[i].values[0]
      
        # ------------------------------------------------------------------
        # Perform regex search to determine "special" total rows
        # ------------------------------------------------------------------
        a_check = re.search('total assets$|^total assets\(|^total assets \(', name, flags=re.I)
        le_check = re.search('(?=.*(liability|liabilities))(?=.*(equity|deficit|capital))', 
                             name, flags=re.I)
        # ------------------------------------------------------------------
        
        # if we find either total measure we re-write indicators
        if a_check is not None or le_check is not None:
            total_flag = 0; total_amt = item1;
        
        # compute backward sum (lookback index) 
        for j in range(i):
            
            # check whether dataframe empty (if so we skip to avoid fitting errors)
            # NOTE: Index position (i-1)   = line above current line
            #                      (i-j-1) = trailing look up line 'j' lines above the line above current line
            lookback = df.loc[i-j-1:i-1][data_col]
            
            # we check whether the lookback period is empty (if so we most likely deleted the row)
            if not lookback.empty:
                
                # backward sum for line items (index minus j-periods before)
                item2 = lookback.sum()

                # if we achieve this then we strip totals and break, no need to continue backward sum
                check1 = item1 == item2
                val, check2 = multiple_check(item1, item2)
                check3 = epsilon_error(item1, item2, tol=0.01)
                
                if check1 or check2 or check3:
                    df = df.drop(index=i)
                    
                    # if we drop the "Total" line-item then we re-assign flag to 1
                    if a_check is not None or le_check is not None:
                        total_flag = 1
                        total_amt = val
                    
                    # Error Handling for row deletions (uncomment for when not in use)
                    print('\tWe dropped row %d, %s, with lookback window of %d.' % (i, name, j+1))
                    print('\t\tOur row is valued at %.2f, our lookback sum is %.2f' % (item1, item2))
                    
                    # we break from inner loop to avoid key error flag 
                    break     
                
    return (df, total_flag, total_amt)

def special_merge(df1:pd.DataFrame, df2:pd.DataFrame, col:str) -> pd.DataFrame:
    """
    Special type of merge for dataframes, combining all unique row 
    items for a specified column. This is designed to combine PDF 
    and PNG balance sheets that differ in one or more rows.
    
    Parameters
    ----------
    df1 : pandas.DataFrame
        DataFrame that represents either the balance seet retreived from
        the PDF of the FOCUS report
    
    df1 : pandas.DataFrame
        DataFrame that represents either the balance seet retreived from
        the PNG of the FOCUS report
        
    col : str
        A shared column name that exists in both df1 and df2
    """
    
    arr1 = df1[col].values
    arr2 = df2[col].values
    concat_pdf = []
    
    # find the sequences that match between either lineitems
    sm = SequenceMatcher(a=arr1, b=arr2)
    
    # ------------------------------------------------------------------
    # The SequenceMathcer returns a 5-tupled for each correspond "obj"
    # 'replace'     a[i1:i2] should be replaced by b[j1:j2].
    # 'delete'      a[i1:i2] should be deleted. Note that j1 == j2 in this case.
    # 'insert'      b[j1:j2] should be inserted at a[i1:i1]. 
    #                        NOTE that i1 == i2 in this case.
    # 'equal'       a[i1:i2] == b[j1:j2] (the sub-sequences are equal)
    # ------------------------------------------------------------------
    
    for (obj, i1, i2, j1, j2) in sm.get_opcodes():
        
        # implies that we want to "replace" the left side elements with the corresponding
        # right side element at the same index position (we perseve both)
        if obj == 'replace':
            
            # check the value of a fuzzy match, only insert both rows if they vastly different
            left_names = arr1[i1:i2]
            right_names = arr2[j1:j2]
            
            # iterate through each of the checks (we assume that left names = right names in size)
            for it, (left, right) in enumerate(zip(left_names, right_names)):
                
                # compute the fuzz match between string (how close are these values)
                score = fuzz.partial_ratio(left.lower(), right.lower())
                
                # if not close in match then we append both values
                if score < 90:
                    concat_pdf.append(df1.iloc[i1:i1+it+1])
                    concat_pdf.append(df2.iloc[j1:j1+it+1])
                else:
                    concat_pdf.append(df1.iloc[i1:i1+it+1])
        
        # implies that we want to "delete" the left side element (we preserve this side)
        elif obj == 'delete':
            concat_pdf.append(df1.iloc[i1:i2])
        
        # implied that we want to "insert" the right side element (we preserve this side)
        elif obj == 'insert':
            concat_pdf.append(df2.iloc[j1:j2])
            
        elif obj == 'equal':
            concat_pdf.append(df1.iloc[i1:i2])
        
    # return concated pandas.DataFrame and reset index, removing old index and dropping duplicates
    return pd.concat(concat_pdf).reset_index(drop=True).drop_duplicates()

def unstructured_data(df:pd.DataFrame, filing_d:str, fiscal_y:str, cik:str, cik2name:dict) -> pd.DataFrame:
    """
    Forms unstructured row for larger database to be stored in s3 bucket
    
    Parameters
    ----------
    df : pandas.DataFrame
        The balance sheet for a particular broker-dealer 
    
    filing_d : str
        The filing date for release of X-17A-5 filings for a 
        broker dealer e.g. 2013-03-21
        
    fiscal_y : str
        The fiscal year for the balance sheet to cover 
        e.g. 2012 (usually 1-year prior to filing date)
        
    cik : str
        The CIK number for a broker dealer e.g. 887767
        
    cik2name : dict
        A dictionary that maps CIK to broker dealer names 
    """
    
    # intialize the first column (line items)
    first_column = df.columns[0]
    
    # clean dataframe should be of size greater than 1
    if len(df.columns) > 1:
        
        # transpose split balance sheet figure (our line items are now columns for DataFrame)
        # we first groupby the first column (this become index) and sum to group congruent names
        row = df.groupby(first_column).sum(min_count=1).T
        
        # creating additional columns in row
        row['CIK'] = cik                                  # CIK number for firm 
        row['Filing Date'] = filing_d                     # Filing Date for firm filing
        row['Filing Year'] = fiscal_y                     # Year for balance sheet filing
        row['Name'] = cik2name['broker-dealers'][cik]     # returns the name of associated with the CIK
        
        return row
    
    else:
        print('%s-%s.csv - encountered issue reading PDF' % (cik, filing_d))
        return None

def extra_cols(csv_name:str) -> tuple:
    """
    Construct extra additional columns to attach to the unstructured
    database after development is complete.
    
    Parameters
    ----------
    csv_name : str
        The file directory on the s3 where data is stored
    """
    
    file_name = csv_name.split('/')[-1]            # e.g. '1224385-2005-03-01.csv'
    csv_strip = file_name[:-4]                     # ignore last four elements from the back (i.e. .csv)

    # construct a string measure of important data measures 
    data_split = csv_strip.split('-')              
    filing_date = '-'.join(data_split[1:])         # join YYYY-mm-dd component for filing date
    fiscal_year = int(data_split[1]) - 1           # fiscal year are generally the previous year of filing
    cik = data_split[0]                            # extract the CIK number  
    
    return (file_name, filing_date, fiscal_year, cik)   

def reorder_columns(df:pd.DataFrame, col_preserve:list) -> pd.DataFrame:
    """
    Re-order the completed DataFrame by ordering the CIK, Name, 
    Filing Data and Filing Year. 
    
    Parameters
    ----------
    df : pandas.DataFrame
        The unstructured database for balance sheet figures
    """
    
    # re-order the CIK and Year columns
    remap = df.columns[~np.isin(df.columns, col_preserve)]                             
    df = df[np.insert(remap,                                       # pass all other columns, not in preserve list
                      np.zeros(len(col_preserve), dtype=int),      # map the location to the first index (i.e. 0)
                      col_preserve)]                               # insert columns we wished to preserve 

    filterNaN = df.isnull().all()                                  # find if any column is all NaN 
    cleanCols = filterNaN[filterNaN == False].index                # select columns with at least one value

    # clean dataframe for unstructured asset terms
    return df[cleanCols]
