#!/usr/bin/env python
# coding: utf-8

"""

run_ocr.py: Script responsible for performing OCR via AWS Textract, 
and then "cleaning" the reported dataframes by handling special 
Textract errors and converting the read strings as numeric values

    1) OCRTextract.py
    2) OCRClean.py
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import os
import json
import numpy as np
import pandas as pd
import time
from OCRTextract import textractParse, textractParse_pdfs_parallel, startJob
from OCRClean import clean_wrapper

from run_file_extraction import brokerFilter


##################################
# MAIN CODE EXECUTION
##################################

def main_p2(s3_bucket, s3_pointer, s3_session, temp_folder, input_pdf, input_png, 
            out_folder_raw_pdf, out_folder_raw_png, textract_obj, out_folder_clean_pdf, 
            out_folder_clean_png, rerun_job, broker_dealers):
    
    print('\n============\nStep 4 & 5: Performing OCR via AWS Textract and Cleaning Operations\n============\n')
    
    # ==============================================================================
    #               STEP 4 (Perform OCR via Textract on FOCUS Reports)
    # ==============================================================================
    
    # csv directory where we store balance sheet information 
    output_pdf_csvs = s3_session.list_s3_files(s3_bucket, out_folder_raw_pdf)
    
    # temp directory where JSON files is stored
    temp = s3_session.list_s3_files(s3_bucket, temp_folder)
    
    # s3 directory where we store the broker-dealer sliced filings 
    raw_pdf_files = s3_session.list_s3_files(s3_bucket, input_pdf)
    
    # ---------------------------------------------------------------------------
    # Load in Temp JSON files (FORM, TEXT, ERROR) if present from s3
    # ---------------------------------------------------------------------------
    if (temp_folder + 'X17A5-FORMS.json' in temp) and (rerun_job > 4):
        # retrieving downloaded files from s3 bucket
        s3_pointer.download_file(s3_bucket, temp_folder + 'X17A5-FORMS.json', 'temp.json')
        
        # read data on KEY-VALUE dictionary (i.e Textract FORMS) 
        with open('temp.json', 'r') as f: forms_dictionary = json.loads(f.read())
        
        # remove local files for JSON
        os.remove('temp.json')
    else:
        forms_dictionary = {}
    
    if (temp_folder + 'X17A5-TEXT.json' in temp) and (rerun_job > 4):
        # retrieving downloaded files from s3 bucket
        s3_pointer.download_file(s3_bucket, temp_folder + 'X17A5-TEXT.json', 'temp.json')
        
        # read data on TEXT-Confidence dictionary
        with open('temp.json', 'r') as f: text_dictionary = json.loads(f.read())  
            
        # remove local files for JSON
        os.remove('temp.json')
    else:
        text_dictionary = {}
    
    if (temp_folder + 'ERROR-TEXTRACT.json' in temp) and (rerun_job > 4):
        # retrieving downloaded files from s3 bucket
        s3_pointer.download_file(s3_bucket, temp_folder + 'ERROR-TEXTRACT.json', 'temp.json')
        
        # read data on errors derived from Textract
        with open('temp.json', 'r') as f: error_dictionary = json.loads(f.read()) 
            
        # remove local files for JSON
        os.remove('temp.json')
    else:
        error_dictionary = {}
    
    # ---------------------------------------------------------------------------
    # Perform Textract analysis on PDFs and PNGs
    # ---------------------------------------------------------------------------
    
    # trailing scaler for firms, keep track of missing
    prior_pdf_scaler = 1.0
    prior_png_scaler = 1.0
    prior_pdf_cik = np.nan
    prior_png_cik = np.nan
    
    
    # pdf directory where we store the broker-dealer information 
    textract_files = list(filter(lambda x: brokerFilter(broker_dealers, x), raw_pdf_files))
    number_files = len(textract_files)
    
    
    if "job_ids.json" in os.listdir():
        with open("job_ids.json", 'r') as f: job_ids = json.loads(f.read())
    else:
        job_ids = {}
    
    # number of concurrent jobs sent to Textract services. 
    # Base on us-east-2 is 100, but our limit has been increased to 300 by asking AWS help desk
    num_concurr_jobs = 100
    
    # if retry_errors is True, the code will try running Textract on X17A files where it failed before
    retry_errors = False
    
    for c_min in range(0,len(textract_files), num_concurr_jobs):
        print('Running Textract from: ' + str(c_min) + ' -' + str(c_min + num_concurr_jobs))
        
        c_max = min(len(textract_files),c_min + num_concurr_jobs )
        for counter in range(c_min, c_max):
            pdf_paths = textract_files[counter]

            basefile = pdf_paths.split('/')[-1].split('-subset')[0]
            fileName = basefile + '.csv'
            
            #job_ids[basefile] = str(counter)
            
            if counter%40 == 0:
                print(counter)
               
            # determines if Textract has already been run for this file
            if retry_errors:
                already_done = out_folder_raw_pdf + fileName in output_pdf_csvs
            else:
                already_done = (out_folder_raw_pdf + fileName in output_pdf_csvs) or (basefile in error_dictionary.keys())
                                                
            if (already_done) and (rerun_job > 4):
                print('\t%s has already been Textracted, we pass ' % fileName)
            else:
                # while True structure is to wait until we're able to send a new textract job
                while True:
                    try:
                        job_ids[basefile] = startJob(s3_bucket, pdf_paths)
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(10)
       
        with open('job_ids.json', 'w') as file: 
            json.dump(job_ids,file)
            file.close()
            
        for counter in range(c_min,c_max):
            pdf_paths = textract_files[counter]
            
            # baseFile name to name export .csv file e.g. 1224385-2004-03-01.csv
            basefile = pdf_paths.split('/')[-1].split('-subset')[0]
            fileName = basefile + '.csv'
            print('\nPerforming OCR for %s (%d out of %s)' % (fileName,counter,number_files))

            # if file is not found in output directory we extract the balance sheet
            # WE LOOK TO AVOID RE-RUNNING OLD TEXTRACT PARSES TO SAVE TIME, but if 
            # rerun_job is < 5 (True) we re-run Textract again
            if retry_errors:
                already_done = out_folder_raw_pdf + fileName in output_pdf_csvs
            else:
                already_done = (out_folder_raw_pdf + fileName in output_pdf_csvs) or (basefile in error_dictionary.keys())

            if (already_done) and (rerun_job > 4):
                print('\t%s has been downloaded' % fileName)
       
            else:
                # run Textract OCR job and extract the parsed data 

                png_paths = input_png + basefile + '/'
                pdf_df, png_df, forms_data, text_data, error = textractParse_pdfs_parallel(pdf_paths, s3_bucket, job_ids[basefile])
            
                if error is not(None):
                    if 'Block' in error:
                        print('Going too fast, waiting 20s')
                        time.sleep(20)
                        pdf_df, png_df, forms_data, text_data, error = textractParse_pdfs_parallel(pdf_paths, s3_bucket, job_ids[basefile])

                # if no error is reported we save FORMS, TEXT, DataFrame
                if error is None:

                    # store accompanying information for JSONs
                    forms_dictionary[basefile] = forms_data
                    text_dictionary[basefile]  = text_data

                    # writing data table to .csv file
                    pdf_df.to_csv(fileName, index=False)
                    with open(fileName, 'rb') as data:
                        s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder_raw_pdf + fileName, Body=data)

                    # writing data frame to .csv file extracted from PNG
                    if png_df is not None:
                        png_df.to_csv(fileName, index=False)
                        with open(fileName, 'rb') as data:
                            s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder_raw_png + fileName, Body=data)

                    print('--------------------------------------------------------------------')
                    print('\tSaved %s file to s3 bucket' % fileName)

                    # ==============================================================================
                    #               STEP 5 (Perform Cleaning Operations on Textract Table)
                    # ==============================================================================

                    if pdf_df is not None:
                        print('\tWorking on PDF balance-sheet')
                        # perform cleaning operations on read balance sheets for PDF and PNGs

                        # adding following try structure. In rare cases clean_wrapper has an error due to invalid cleaning of pdf dataframe                               that raises an error (for dataframe '1139137-2006-02-28.csv')
                        try:
                            pdf_df_clean, prior_pdf_scaler, prior_pdf_cik = clean_wrapper(pdf_df, text_dictionary, basefile, fileName,
                                                                                          prior_pdf_scaler, prior_pdf_cik)

                            # export contents to the s3 directory
                            pdf_df_clean.to_csv(fileName, index=False)
                            with open(fileName, 'rb') as data:
                                s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder_clean_pdf + fileName, Body=data)

                        except Exception as e:
                            error_dictionary[basefile] = str(e)

                    if png_df is not None:
                        print('\tWorking on PNG balance-sheet')
                        png_df_clean, prior_png_scaler, prior_png_cik = clean_wrapper(png_df, text_dictionary, basefile, fileName,
                                                                                      prior_png_scaler, prior_png_cik)

                        png_df_clean.to_csv(fileName, index=False)
                        with open(fileName, 'rb') as data:
                            s3_pointer.put_object(Bucket=s3_bucket, Key=out_folder_clean_png + fileName, Body=data)

                    # remove local file after it has been created
                    if os.path.isfile(fileName):
                        os.remove(fileName)
                        print('--------------------------------------------------------------------\n')

                else:
                    print('\tError with Textract : '+ error)
                    error_dictionary[basefile] = error
                    
                if counter%200 == 0:
                    print('Intermediate saving of errors ')
                    with open('X17A5-FORMS.json', 'w') as file: 
                        json.dump(forms_dictionary, file)
                        file.close()

                    # save contents to AWS S3 bucket
                    with open('X17A5-FORMS.json', 'rb') as data: 
                        s3_pointer.upload_fileobj(data, s3_bucket, temp_folder + 'X17A5-FORMS.json')
                    os.remove('X17A5-FORMS.json')

                    # write to a JSON file for TEXT 
                    with open('X17A5-TEXT.json', 'w') as file: 
                        json.dump(text_dictionary, file)
                        file.close()

                    # save contents to AWS S3 bucket
                    with open('X17A5-TEXT.json', 'rb') as data: 
                        s3_pointer.upload_fileobj(data, s3_bucket, temp_folder + 'X17A5-TEXT.json')
                    os.remove('X17A5-TEXT.json')

                    # write to a JSON file for FORMS 
                    with open('ERROR-TEXTRACT.json', 'w') as file: 
                        json.dump(error_dictionary, file)
                        file.close()

                    # save contents to AWS S3 bucket
                    with open('ERROR-TEXTRACT.json', 'rb') as data: 
                        s3_pointer.upload_fileobj(data, s3_bucket, temp_folder + 'ERROR-TEXTRACT.json')
                    os.remove('ERROR-TEXTRACT.json')
          
    # ---------------------------------------------------------------------------
    # Save JSON files for updated figures (FORM, TEXT, ERROR)
    # ---------------------------------------------------------------------------
    
    # write to a JSON file for FORMS 
    with open('X17A5-FORMS.json', 'w') as file: 
        json.dump(forms_dictionary, file)
        file.close()
    
    # save contents to AWS S3 bucket
    with open('X17A5-FORMS.json', 'rb') as data: 
        s3_pointer.upload_fileobj(data, s3_bucket, temp_folder + 'X17A5-FORMS.json')
    os.remove('X17A5-FORMS.json')

    # write to a JSON file for TEXT 
    with open('X17A5-TEXT.json', 'w') as file: 
        json.dump(text_dictionary, file)
        file.close()
    
    # save contents to AWS S3 bucket
    with open('X17A5-TEXT.json', 'rb') as data: 
        s3_pointer.upload_fileobj(data, s3_bucket, temp_folder + 'X17A5-TEXT.json')
    os.remove('X17A5-TEXT.json')

    # write to a JSON file for FORMS 
    with open('ERROR-TEXTRACT.json', 'w') as file: 
        json.dump(error_dictionary, file)
        file.close()
    
    # save contents to AWS S3 bucket
    with open('ERROR-TEXTRACT.json', 'rb') as data: 
        s3_pointer.upload_fileobj(data, s3_bucket, temp_folder + 'ERROR-TEXTRACT.json')
    os.remove('ERROR-TEXTRACT.json')
          

