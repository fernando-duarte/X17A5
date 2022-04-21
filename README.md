# X-17A-5 Optical Character Recognition (OCR)

## 1	Introduction
The project scrapes the SEC for X-17A-5 filings published by registered broker-dealers historically, and constructs a database for asset, liability and equity line items. The project runs on Amazon Web Services (AWS) in a SageMaker instance and stores the scraped information into an s3 bucket. 

## 2	Software Dependencies
**All code is executed using Python 3.7.12 as of current release. We make no claim for stability on other version of Python.**

We use [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) to interact with the SEC website and EDGAR archive, to extract data files (e.g. X-17A-5). 
```
pip install bs4
```

We use [PyPDF2](https://pythonhosted.org/PyPDF2/), [PyMuPDF](https://github.com/pymupdf/PyMuPDF), [pdf2image](https://pypi.org/project/pdf2image/), [fitz](https://pypi.org/project/fitz/), pikepdf (https://pypi.org/project/pikepdf/), [pillow](https://pillow.readthedocs.io/en/stable/) as used in retrieval & slicing operations. 
```
pip install PyPDF2
pip install PyMuPDF
pip install pdf2image 
pip install pillow
pip install fitz 
pip install cv2  
pip install pikepdf
```

PLEASE READ THE DOCUMENTATION FROM pdf2image provided at the following [link](https://github.com/Belval/pdf2image). You will need to install poppler on your machine (e.g. Windows, Mac, Linux) to execute slicing operations. For additional details on poppler see [doc](https://poppler.freedesktop.org/) on use cases.
```
# to install poppler PDF backend for analysis on Jupyter environment  
conda install -c conda-forge poppler  -y
```

We use [smart_open](https://pypi.org/project/smart-open/), [minecart](https://pypi.org/project/minecart/), and [textract-trp](https://pypi.org/project/textract-trp/) to analyze X-17A-5 filings using AWS Textract to perform OCR on both PDFs and PNGs.   
```
pip install smart_open
pip install minecart
pip install textract-trp
```

We use [python-Levenshtein](https://pypi.org/project/python-Levenshtein/), and [fuzzywuzzy](https://pypi.org/project/fuzzywuzzy/) for performing "fuzzy" string matching for particular strings
```
pip install python-Levenshtein
pip install fuzzywuzzy
```
### 2.1 	Pip freeze

The file "frozen-requirements.txt" is a list of packages installed on the last instance used to commit the present version of this code. In particular it has version numbers for all the packages discussed above.


## 3	File Structure

### 3.1 	Resource Files

* `CIKandDealers.json` JSON file storing CIK numbers for firms and company names as key/value pairs respectively ({"broker-dealers" : {"356628": "NATIONAL FINANCIAL SERVICES LLC", "815855": "MERRILL LYNCH GOVERNMENT SECURITIES OF PUERTO RICO INC"}}), with accompanying years covered {'years-covered': ["1993/QTR1", "1993/QTR2", "1993/QTR3", "1993/QTR4"]}. All CIK numbers are taken from the EDGAR [archive](https://www.sec.gov/Archives/edgar/full-index/) from the SEC. 

* `X17A5-FORMS.json` JSON file storing the CIK numbers with the accompanying [FORMS](https://docs.aws.amazon.com/textract/latest/dg/how-it-works-kvp.html) data retrieved from AWS Textract.

* `X17A5-TEXT.json` JSON file storing the CIK numbers with the accompanying [TEXT](https://docs.aws.amazon.com/textract/latest/dg/how-it-works-lines-words.html) data retrieved from AWS Textract.

### 3.2 	Error Files

* `ERROR-TEXTRACT.json` JSON file storing CIK numbers with accompanying year that were unable to be read via Textract. There are two types of errors that are raised:
    * *No Balance Sheet found, or parsing error*, where there may be an issue with Textract reading the page
    * *Could not parse, JOB FAILED*, where there may be an issue with Textract parsing the pdf file   
    * *Blocks*, Textract didn't complete the job and threw a Blocks Error. If the code ran properly this error should not be present.   

    
### 3.3 	Input Files

* `asset_log_reg_mdl_v2.joblib` & `liability_log_reg_mdl_v2.joblib` logistic regression classification models used for constructing the structured database, for asset and liablity & equity line items respectively
    
### 3.4 	Code Files

The code files are divided into three sub-groups that are responsible for executing a portion of the database construction and are found within the `/code/src` folder.  

#### 3.4a 

   * `GLOBAL.py` Script storing essential global variables regarding mapping conventions for s3 directories
   
   * `run_main.py` the primary scipt for execution of all code-steps Part 1, Part 2 and Part 3 (see below 3.3b) 

   * `run_file_extraction.py` runs all execution for Part 1 (see below 3.3b), responsible for gathering FOCUS reports and building list of broker-dealers 

   * `run_ocr.py` runs all execution for Part 2 (see below 3.3b), responsible for extracting balance-sheet figures by OCR via AWS Textract (`run_ocr_blocks.py` does the same thing)
   
   * `run_build_database.py` runs all execution for Part 3 (see below 3.3b), responsible for developing structured and unstructured database

#### 3.4b 	

##### Part 1: Broker-Dealer and FOCUS Report Extraction

   * `ExtractBrokerDealers.py` responsible for creating the `CIKandDealers.json` file, which stores all CIK-Name information for broker-dealers that file an X-17A-5.   
   * `FocusReportExtract.py` responsible for extracting the X-17A-5 pdf files from broker-dealer URLs
   * `FocusReportSlicing.py` reduces the size of the X-17A-5 pdf files to a "manageable" subset of pages with corresponding PNG(s)

##### Part 2: Optical Character Recognition

   * `OCRTextract.py` calls the AWS asynchronous Textract API to perform OCR on the reduced X-17A-5 filings, selecting only the balance sheet and uploading it to a s3 bucket
   * `OCRClean.py` refines the scraped balance sheet data from Textract, handling case exemptions such as merged rows, multiple columns and numeric string conversions 

##### Part 3: Database construction

   * `DatabaseSplits.py` divides the balance sheet into asset terms and liability & equity terms for pdf(s) and png(s)
   * `DatabaseUnstructured.py` constructs a semi-finished database that captures all unique line items by column for the entirety of each bank and year
   * `DatabaseStructured.py` constructs a finished database that aggregates columns by a predicted class for assets and liability & equity terms

### 3.5 	Output Files

   * `asset_name_map.csv` & `liability_name_map.csv` represent the asset and liability & equity line item mappings, as determined by our logistic regression classifier model - mapping balance-sheet line items to one of our pre-defined accounting groups.   
   
   * `unstructured_assets` & `unstructured_liable` folders: represent the asset and liability & equity balance sheets respectively, non-aggregated by line items for each broker-dealers per filing year - where our values correspond to parsed FOCUS reports. This is done for chunks ('cuts') of 1 000 balance sheets at a time

   * `structured_assets.csv` & `structured_liability.csv` represent the asset and liability & equity balance sheets respectively, aggregating by line items for each broker-dealers per filing year - where aggregation is determined by a logistic regression classifier model and values correspond to parsed FOCUS reports.  

## 4	Running Code

Our code file runs linearly in a SageMaker or EC2 instance via batch on AWS. We allow for the user to provide specifications prior to runnign the code base, but enable defaults in the event the user is not pariticular. We assume that you are able to `clone` this repository to a local instance on AWS, either the EC2 or SageMaker, and will not discuss operations centered around this action. 
If running the code for all broker_dealers it is necessary to do so on an EC2 instance as Sagemaker jobs are limited in time (approximately 30 hours before the code crashes). See EC2 specificity below.

### If running on EC2
Make sure that the EC2 instance's configuration are linked to the right region (e.g us-east-2 )
For a new instance, you have to change configuration files by using this command:
```
$  aws configure
```
and changing the Default region

If aws is not installed (aws: command not found) follow these instructions: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html. These are to be run at the root of the project and require unzip (sudo yum install unzip).

### For all
1. Open the GLOBAL.py file, this stores all global variables under the `GlobVars` class. We will selectively modify these to match the corresponding folders on our s3 where we would like for our data files to be stored. 

2. Open the run_main.py file, this executes all steps (parts) of our code base and accepts parameters under the `Parameters` class.
   
   1.    Modify the static variable `bucket` to the designated s3 bucket you intend to store data materials
   2.    Modify the `parse_years` list with the numerical years look back historically for broker dealers (refer to inline doc)
   3.    Modify the `broker_dealers_list` list with broker-dealer CIKs you'd like to operate on (refer to inline doc)
   4.    Modify the `job_rerun` with an integer flag to indicate a preference to ignore existing files (refer to inline doc)
   5.    Modify the `fed_proxy` with the adequate proxy address if working on the NIT (refer to inline doc, empty string "" if no proxy) 


3. On your terminal, whether on the EC2 or SageMaker, run the shell-script by evoking the `sh`. If running with a proxy, use run_main_batch_proxy.sh instead.

```
$  sh run_main_batch.sh
```
To save outputs in a file called "SaveOutput.txt", run:

```
$  sh run_main_batch.sh 2>&1 | tee SaveOutput.txt

```


## 5	Hardware
The code takes advantage of having multiple processors. For maximum speed it is recommended to use at least 8 cores (this was especially true when processing the PNGs, less true without that step). In addition having a decent amount of RAM (16GB) guarantees that there are no memory crashes. 

For those reasons, we recommend using a "ml.t3.2xlarge" or equivalent, with 25GB of memory.

Note: the PNG processing (heritage code) benefitted massively from running with a "ml.m5.4xlarge" instance. The speed increase made up for more then the difference in price.


### EC2 settings
The run_main_batch.sh file is coded for a Red Hat Linux AMI. Specifically it has been tested for: "AMI ID
RHEL-8.4.0_HVM-20210504-x86_64-2-Hourly2-GP2 (ami-0ba62214afa52bec7)".

## 6	Possible Extensions
* Extend and modify idiosyncratic changes as deemed appropriate for when Textract fails. This could be selective processing with Tables + Forms, or with PNGs.

* Re-code Textract for PNGs by taking advantage of asynchronous Textract to greatly increase speed as was done for PDFs

* run_ocr_block.py is almost identical to run_ocr.py. Having a way of passing a to do list to run_ocr.py would be a way of solving that (the list would then be all 'block' errors for the second pass)


## 7	Contributors
* [Mathias Andler](https://github.com/mathias-andler) (Sr. Research Analyst)
* [Rajesh Rao](https://github.com/raj-rao-rr) (Sr. Research Analyst)
* [Fernando Duarte](https://github.com/fernando-duarte) (Sr. Economist)
