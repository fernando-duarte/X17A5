#!/bin/bash

# Batch file for running script on local SageMaker instance (0)
# versus on remote EC2 instance (other than 0)

if [ $1 == 0 ]  # SageMaker instance terminal

then

    # update the conda environment 
    conda update -n base -c defaults conda -y 

    # downloading poppler backend to support pdf2image package
    conda install -c conda-forge poppler -y
    
    ####################################
    # INSTALL LIBRARIES (pip-install)
    ####################################
    pip install --upgrade pip
    pip install bs4
    pip install PyPDF2
    pip install pdf2image
    pip install fitz
    pip install pillow
    pip install PyMuPDF==1.16.14
    pip install smart_open
    pip install minecart
    pip install textract-trp
    pip install python-Levenshtein
    pip install fuzzywuzzy
    pip install joblib
    pip install scikit-learn==0.24.1    # log-reg model stability verison
    
    # run the main-script for the X-17A-5 project (run_main_focus.py)
    ipython run_main.py

else           # EC2 instance terminal
    
    # install conda for local use on the EC2 
    sudo yum install python3 -y
    sudo yum install libXcomposite libXcursor libXi libXtst libXrandr alsa-lib mesa-libEGL libXdamage mesa-libGL libXScrnSaver -y  
    
    sudo wget https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh 
    echo '\n'             # press enter to move forward and accept value
    yes                   # accept license for Anaconda3 distribution   
    
    sh Anaconda3-2020.02-Linux-x86_64.sh -y

    # create a new anaconda environment and update envrionment
    export PATH=~/anaconda3/bin:$PATH
    conda update -n base -c defaults conda -y 

    # downloading poppler backend to support pdf2image package
    conda install -c conda-forge poppler -y
    
    ####################################
    # INSTALL LIBRARIES (conda-install)
    ####################################
    conda install bs4 -y
    conda install PyPDF2 -y
    conda install pdf2image -y
    conda install fitz -y
    conda install pillow -y
    conda install PyMuPDF==1.16.14 -y
    conda install smart_open -y
    conda install minecart -y
    conda install textract-trp -y
    conda install python-Levenshtein -y
    conda install fuzzywuzzy -y
    conda install joblib -y
    conda install scikit-learn==0.24.1 -y    # log-reg model stability verison
    
    # run the main-script for the X-17A-5 project (run_main_focus.py)
    python3 run_main.py

fi