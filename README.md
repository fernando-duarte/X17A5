# X-17A-5 Optical Character Recognition (OCR)

## 1	Introduction
The project scrapes the SEC for X-17A-5 filings published by registered broker-dealers historically, and constructs a database for asset, liability and equity line items. The project runs on Amazon Web Services (AWS) in a SageMaker instance and stores the scraped information into an s3 bucket. 

## 2	Software Dependencies
**All code is executed using Python 3.6**

We use [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) to interact with the SEC website and EDGAR archive, to extract data files (e.g. X-17A-5). 
```
%pip install bs4
```

We use [PyPDF2](https://pythonhosted.org/PyPDF2/), [PyMuPDF](https://github.com/pymupdf/PyMuPDF), [pdf2image](https://pypi.org/project/pdf2image/), [fitz](https://pypi.org/project/fitz/), [pillow](https://pillow.readthedocs.io/en/stable/) and [poppler](https://poppler.freedesktop.org/) to manipulate PDFs (i.e. X-17A-5) as used in retrieval & slicing operations. 
```
%pip install PyPDF2
%pip install PyMuPDF
%pip install pdf2image 
%pip install pillow
%pip install fitz 
%pip install cv2 

# to install poppler PDF backend for analysis on Jupyter environment  
%conda install -c conda-forge poppler      
```

We use [smart_open](https://pypi.org/project/smart-open/), [minecart](https://pypi.org/project/minecart/), and [textract-trp](https://pypi.org/project/textract-trp/) to analyze X-17A-5 filings using AWS Textract to perform OCR on both PDFs and PNGs.   
```
%pip install smart_open
%pip install minecart
%pip install textract-trp
```

We use [python-Levenshtein](https://pypi.org/project/python-Levenshtein/), and [fuzzywuzzy](https://pypi.org/project/fuzzywuzzy/) for performing "fuzzy" string matching for particular strings
```
%pip install python-Levenshtein
%pip install fuzzywuzzy
```

## 3	File Structure

### 3.1 	Resource Files

* `CIKandDealers.json` JSON file storing CIK numbers for firms and company names as key/value pairs respectively ({"broker-dealers" : {"356628": "NATIONAL FINANCIAL SERVICES LLC", "815855": "MERRILL LYNCH GOVERNMENT SECURITIES OF PUERTO RICO INC"}}), with accompanying years covered {'years-covered': ["1993/QTR1", "1993/QTR2", "1993/QTR3", "1993/QTR4"]}. All CIK numbers are taken from the EDGAR [archive](https://www.sec.gov/Archives/edgar/full-index/) from the SEC. 

* `X17A5-FORMS.json` JSON file storing the CIK numbers with the accompanying [FORMS](https://docs.aws.amazon.com/textract/latest/dg/how-it-works-kvp.html) data retrieved from AWS Textract.

* `X17A5-TEXT.json` JSON file storing the CIK numbers with the accompanying [TEXT](https://docs.aws.amazon.com/textract/latest/dg/how-it-works-lines-words.html) data retrieved from AWS Textract.

### 3.2 	Error Files

* `ERROR-TEXTRACT.json` JSON file storing CIK numbers with accompanying year that were unable to be read via Textract. There are two types of errors that are raised:
    * *No Balance Sheet found, or parsing error*, where there may be an issue with Textract reading the page
    * *Could not parse, JOB FAILED*, where there may be an issue with Textract parsing the pdf file   
    
### 3.3 	Code Files

The code files are divided into four sub-groups that are responsible for executing a portion of the database construction. 

#### Broker-Dealer Gathering

   * `readDealerData.ipynb` responsible for creating the `CIKandDealers.json` file, which stores all CIK-Name information for broker-dealers that file an X-17A-5.   

#### X17A5 File Retrieval & Slicing

   * `pdfFileExtract.ipynb` responsible for extracting the X-17A-5 pdf files from broker-dealer URLs

   * `pdfFileSlicing.ipynb` reduces the size of the X-17A-5 pdf files to "manageable" ~15-page pdf(s) as well as individual PNG(s)

#### Optical Character Recognition

   * `ocrTextract.ipynb` calls the AWS asynchronous Textract API to perform OCR on the reduced X-17A-5 filings, selecting only the balance sheet and uploading it to a s3 bucket

   * `ocrClean.ipynb` refines the scraped balance sheet data from Textract, handling case exemptions such as merged rows, multiple columns and numeric string conversions 

#### Database construction

   * `databaseLineitems.ipynb` divides the balance sheet into asset terms and liability & equity terms for pdf(s) and png(s)

   * `databaseUnstructured.ipynb` constructs a semi-finished database that captures all unique line items by column for the entirety of each bank and year

   * `databaseStructured.ipynb` constructs a finished database that aggregates columns by a predicted class for assets and liability & equity terms

### 3.4 	Output Files

   * `unstructAsset.csv` & `unstructLiable.csv` both represent the unstructured asset and liability & equity terms respectively, from all of the broker-dealers per year 

   * `structAsset.csv` & `structLiable.csv` both represent the structured asset and liability & equity terms respectively, from all of the broker-dealers per year 

## 4	Running Code

Our code file runs linearly via SageMaker instance, though we will make an effort in the future to streamline these processes via batch. 

1. Begin by first running `readDealerData.ipynb`, this constructs a list of broker-dealers that file a X-17A-5 
2. Second we run `pdfFileExtract.ipynb` to extract all relevant X-17A-5 filings that correspond with each year
3. Follow by running `pdfFileSlicing.ipynb` to reduce the size of these "raw" X-17A-5 filings to be compatible with Textract's file size constraint. **Note this algorithm takes a while to run, due to the PNG file conversion.**
4. We now extract the balance sheet from each of the X-17A-5 filings and perform "cleaning" operations. We run `ocrTextract.ipynb` to perform OCR with AWS Textract and proceed with `ocrClean.ipynb` to remove potential issues that may arise from Textract. **Note the Textract algorithm takes a while to run, due to the time for AWS to perform Textract.**
5. We then run `databaseLineitems.ipynb` to divide the "cleaned" balance sheets in asset and liability & equity terms
6. We then run `databaseUnstructured.ipynb` and follow by running `databaseStructured.ipynb` to complete each database 

## 5	Possible Extensions
* Convert all .ipynb files into .py files for use in batch execution (.py calls python compiler)
* Streamline the runtime of each file via batch job on AWS

## 6	Contributors
* [Rajesh Rao](https://github.com/Raj9898) (Sr. Research Analyst 22’)
* [Fernando Duarte](https://github.com/fernando-duarte) (Sr. Economist)
