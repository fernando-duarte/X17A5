#!/bin/bash 

# update the conda environment 
conda update -n base -c defaults conda -y 

# downloading poppler backend to support pdf2image package
conda install -c conda-forge poppler -y

# run the main-script for the X-17A-5 project (run_main_focus.py)
ipython run_main.py
