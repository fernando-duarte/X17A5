#!/bin/bash

# Batch file for running script on local SageMaker instance (0)
# versus on remote EC2 instance (other than 0)

if [ $1 == 0 ]  # SageMaker instance terminal

then
# update the conda environment 
conda update -n base -c defaults conda -y 

# downloading poppler backend to support pdf2image package
conda install -c conda-forge poppler -y

# run the main-script for the X-17A-5 project (run_main_focus.py)
ipython run_main.py

else           # EC2 instance terminal
   
    echo "Trying to work with the ec2"

fi