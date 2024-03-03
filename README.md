# City of Cape Town - Data Science Unit Code Challenge
## Data Engineering submission

This repository outlines the steps taken to complete the various challenges for the data engineering position. 

### Set up
Before Running the various files, please follow the following steps to set up the environment:
* Use a Python version uses 3.10.0 (Note this was developed on a windows 10 computer)
* Clone or fork this repository
* Set up a virtual environment by following the steps below:
* cd into this directory
* Run `python -m venv .venv`
* Activate your environment if not activated by running ` . .\.venv\Scripts\activate`
* Once activate add modules by running `pip install -r requirements.txt`
* Your environment should now be ready to run the various scripts

### Structure of submission
The script to be executed is labelled according to the headings in the readme file. Therefore:
1. Data Extraction --> `1_data_extraction.py`
2. Data Transformation --> `2_data_transformation.py`
5. Further Data Transformation --> `5_further_transformation.py`

In addition, once run, the script will not write much information to the console, but rather log all details into a .log file with the name of the script that was run. 

### Activity 1: Data Extraction 
To run this file, execute the following command in the file location:
`python 1_data_extraction.py`

Output:
Log file with the details of the run called: `1_data_extraction.log`

issues:
This script downloads the file from the s3 bucket as expected, however, on validation, we are seeing a slight mismatch of the most significant values in some of the records in the dataframe. This is due to parsing the file with a polygon function which changes these values. To remedy this, I aimed to ingest the data to a JSON format and then do the comparison. I was, however, unable to do this due to time constraints. however, the script works. 

### Activity 2: Data Transformation 
To run this file, execute the following command in the file location:
`python 2_data_transformation.py`

Output:
Log file with the details of the run called: `2_data_transformation.log`

### Activity 3: Further Data Transformation
To run this file, execute the following command in the file location:
`python 5_further_transformation.py`

Output:
Log file with the details of the run called: `5_further_transformation.py.log`

issues:
I was unable to finish this activity, however, I have outlined the steps I would have followed in the script. 

### To be added
This is by no means the full solution, however, given to limitation of time, I was unable to complete some of the items that I would have added if I had more time. These are:
* Validation of all the methods within the CityAwsHelper class.
* Unit tests of all the methods in the CityAwsHelper class using the unittest framework within python
* Removal of all downloaded files on completion of the script
* Completion of validation of activity 1
* Completion of activity 5
