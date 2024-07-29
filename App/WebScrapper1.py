
import cdsapi
import os
import boto3
import numpy as np
import numba
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score


# Load environment variables (only needed if running locally with a .env file)
if not os.getenv("GITHUB_ACTIONS"):
    from dotenv import load_dotenv
    load_dotenv()

CDS = os.getenv("CDS")

# Your existing code here


# Create a client instance
c = cdsapi.Client()

# Retrieve data
c.retrieve(
    'satellite-carbon-dioxide',
    {
        'processing_level': 'level_2',
        'variable': 'xco2',
        'sensor_and_algorithm': 'sciamachy_wfmd',
        'year': [
            '2002', '2003', '2004',
            '2005', '2006', '2007',
            '2008', '2009', '2010',
            '2011', '2012',
        ],
        'month': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
        ],
        'day': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28', '29', '30',
            '31',
        ],
        'version': '4.0',
        'format': 'zip',
    },
    'download.zip'
)
