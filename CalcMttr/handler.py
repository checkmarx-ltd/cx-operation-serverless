import json
import subprocess
import os
import sys


subprocess.call('pip3 install cryptography -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.call('pip3 install pandas -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.call('pip3 install elasticsearch -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.call('pip3 install elasticsearch-dbapi -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.call('pip install python-dotenv -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

sys.path.insert(1, '/tmp/')
from cryptography.fernet import Fernet
import CalculateRollingMTTR

def calcMttr(event, context):

    CalculateRollingMTTR.main() 

    body = {

        "message": "Calculate MTTR executed successfully",

        "input": event

    }

 

    response = {

        "statusCode": 200,

        "body": json.dumps(body)

    }

 

    return response

 

    # Use this code if you don't use the http event with the LAMBDA-PROXY

    # integration

    """

    return {

        "message": "Go Serverless v1.0! Your function executed successfully!",

        "event": event

    }

    """
	
	
	
