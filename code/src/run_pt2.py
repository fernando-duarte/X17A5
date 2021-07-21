#!/usr/bin/env python
# coding: utf-8

"""
run_pt2.py: Script responsible for performing OCR via AWS Textract, 
and then "cleaning" the reported dataframes by handling special 
Textract errors and converting the read strings as numeric values

    1) OCRTextract.py
    2) OCRClean.py
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import json

from OCRTextract import textractParse  
form OCRClean import clean_wrapper


##################################
# MAIN CODE EXECUTION
##################################

def main_p2(s3_bucket, s3_pointer, s3_session, temp_folder, input_pdf, input_png, 
            out_folder_raw_pdf, out_folder_raw_png, textract_obj, textract_files,
            out_folder_clean_pdf, out_folder_clean_png):
    
    # ==============================================================================
    #               STEP 4 (Perform OCR via Textract on FOCUS Reports)
    # ==============================================================================
    
    # csv directory where we store balance sheet information 
    output_pdf_csvs = np.array(session.list_s3_files(s3_bucket, out_folder_raw_pdf))
    output_png_csvs = np.array(session.list_s3_files(s3_bucket, out_folder_raw_png))
    
    # temp directory where JSON files is stored
    temp = np.array(session.list_s3_files(s3_bucket, temp_folder))
    
    # pdf directory where we store the broker-dealer information 
    pdf_files = np.array(session.list_s3_files(s3_bucket, input_pdf))[1:]
    
    # ---------------------------------------------------------------------------
    # Load in Temp JSON files (FORM, TEXT, ERROR) if present from s3
    # ---------------------------------------------------------------------------
    
    if temp_folder + 'X17A5-FORMS.json' in temp:
        # retrieving downloaded files from s3 bucket
        s3_pointer.download_file(bucket, 'Temp/X17A5-FORMS.json', 'temp.json')
        
        # read data on KEY-VALUE dictionary (i.e Textract FORMS) 
        with open('temp.json', 'r') as f: forms_dictionary = json.loads(f.read())
        
        # remove local files for JSON
        os.remove('temp.json')
    else:
        forms_dictionary = {}
    
    if temp_folder + 'X17A5-TEXT.json' in temp:
        # retrieving downloaded files from s3 bucket
        s3_pointer.download_file(bucket, 'Temp/X17A5-TEXT.json', 'temp.json')
        
        # read data on TEXT-Confidence dictionary
        with open('temp.json', 'r') as f: text_dictionary = json.loads(f.read())  
            
        # remove local files for JSON
        os.remove('temp.json')
    else:
        text_dictionary = {}
    
    if temp_folder + 'ERROR-TEXTRACT.json' in temp:
        # retrieving downloaded files from s3 bucket
        s3_pointer.download_file(bucket, 'Temp/ERROR-TEXTRACT.json', 'temp.json')
        
        # read data on errors derived from Textract
        with open('temp.json', 'r') as f: error_dictionary = json.loads(f.read()) 
            
        # remove local files for JSON
        os.remove('temp.json')
    else:
        error_dictionary = {}
    
    # ---------------------------------------------------------------------------
    # Perform Textract analysis on PDFs and PNGs
    # ---------------------------------------------------------------------------
    
    # if no files were provided by the user, we default to the full sample
    if len(textract_files) == 0:
        textract_files = pdf_files
    
    # trailing scaler for firms, keep track of missing
    prior_pdf_scaler = 1.0
    prior_png_scaler = 1.0
    prior_pdf_cik = np.nan
    prior_png_cik = np.nan
    
    for pdf_paths in textract_files:
        
        # baseFile name to name export .csv file e.g. 1224385-2004-03-01.csv
        basefile = pdf_paths.split('/')[-1].split('-subset')[0]
        fileName = basefile + '.csv'
        print('\nPerforming OCR for %s' % fileName)
        
        # if file is not found in output directory we extract the balance sheet
        # WE LOOK TO AVOID RE-RUNNING OLD TEXTRACT PARSES TO SAVE TIME
        if (out_folder_raw_pdf + fileName not in output_pdf_csvs):
            
            # run Textract OCR job and extract the parsed data 
            png_paths = input_png + basefile + '/'
            pdf_df, png_df, forms_data, text_data, error = textractParse(pdf_paths, png_paths, s3_bucket)

            # if no error is reported we save FORMS, TEXT, DataFrame
            if error is None:

                # store accompanying information for JSONs
                forms_dictionary[basefile] = forms_data
                text_dictionary[basefile]  = text_data
                
                # writing data table to .csv file
                pdf_df.to_csv(fileName, index=False)
                with open(fileName, 'rb') as data:
                    s3_pointer.put_object(Bucket=bucket, Key=out_folder_raw_pdf + fileName, Body=data)
                
                # writing data frame to .csv file extracted from PNG
                if png_df is not None:
                    png_df.to_csv(fileName, index=False)
                    with open(fileName, 'rb') as data:
                        s3_pointer.put_object(Bucket=bucket, Key=out_folder_raw_png + fileName, Body=data)

                print('-----------------------------------------------------')
                print('Saved %s file to s3 bucket' % fileName)
                
                # ==============================================================================
                #               STEP 5 (Perform Cleaning Operations on Textract Table)
                # ==============================================================================
                
                # perform cleaning operations on read balance sheets for PDF and PNGs
                pdf_df_clean, prior_pdf_scaler, prior_pdf_cik = clean_wrapper(pdf_df, text_dictionary, base_file, fileName,
                                                                              prior_pdf_scaler, prior_pdf_cik)
                
                png_df_clean, prior_png_scaler, prior_png_cik = clean_wrapper(png_df, text_dictionary, base_file, fileName,
                                                                              prior_png_scaler, prior_png_cik)
                
                # export contents to the s3 directory
                pdf_df_clean.to_csv(fileName, index=False)
                with open(fileName, 'rb') as data:
                    s3.put_object(Bucket=bucket, Key=out_folder_clean_pdf + fileName, Body=data)
                    
                png_df_clean.to_csv(fileName, index=False)
                with open(fileName, 'rb') as data:
                    s3.put_object(Bucket=bucket, Key=out_folder_clean_png + fileName, Body=data)
                
                
                # remove local file after it has been created
                os.remove(fileName)
                
            else:
                error_dictionary[basefile] = error
                
        else:
            print('%s has been downloaded' % fileName)
    
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
    
    print('\n===================\nStep 4 & 5: Peformed OCR via AWS Textract and Cleaned Data Tables\n===================\')
          