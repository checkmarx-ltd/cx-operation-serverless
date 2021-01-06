import json, sys, os
import subprocess
import threading
import boto3

subprocess.call('pip3 install pandas -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.call('pip3 install elasticsearch -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.call('pip install python-dotenv -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


sys.path.insert(1, '/tmp/')

from ec2 import EC2
from aws_service import AwsService
from db_service import DbService
from performance_counters import PerformanceCounters
from dotenv import load_dotenv

def collect_ec2_utilization(ec2, metric_list):

    aws_service = AwsService()    
    db_service = DbService()

    frames = []
                
    for metric_name in metric_list:
        statistics = 'Average'
        namespace = 'AWS/EC2'
        instance_id = ec2.instance_id
        period = 3600
        start_time = '2020-12-06T00:00:00'
        end_time = '2020-12-07T00:00:00'
                                            
        df = aws_service.get_aws_metric_statistics(ec2, metric_name, period, start_time, end_time, namespace, statistics)               
        
        if not df.empty:
            frames.append(df)           
        
    # merge the different dataframes (cpu_utilization, network_in...) into one dataframe based on start_time        
    if not frames == []:
        df_merged = db_service.merge_ec2_metrics_on_start_time(frames)     
                
        #convert the merged dataframe to class members to ease insert to Elasticsearch
        ec2.performance_counters_list = db_service.create_performance_counters_list(df_merged, metric_list)

        #insert the data into proper elastic index
        response =  db_service.ec2_bulk_insert_elastic(ec2)   
      

        

    
def collect_ec2_all():
    try:
        ec2_metric_list = ['CPUUtilization', 'NetworkOut', 'NetworkIn','DiskWriteBytes','DiskReadBytes','NetworkPacketsOut','NetworkPacketsIn','DiskWriteOps','DiskReadOps']            
        ec2_instances = []
        chunk_size = 10       
        
        aws_service = AwsService()    

        ec2_list = aws_service.get_aws_describe_instances()

        threads = []

        for i in range(0,len(ec2_list), chunk_size):
            chunk = ec2_list[i:i+chunk_size]
            for ec2 in chunk:
                x = threading.Thread(target=collect_ec2_utilization, args=(ec2, ec2_metric_list,))
                threads.append(x)
                x.start()
            for index, thread in enumerate(threads):                
                thread.join()                

            threads = []    
        
        for ec2 in ec2_list:
            print(f"instance_id: {ec2.instance_id} , owner_id: {ec2.instance_owner_id}, launch_time: {ec2.launch_time}")            
                
    except Exception as e:
        print(e)

def collect_accounts_cost(account_number, accounts_visibility_last_update):

    aws_service = AwsService() 
    db_service = DbService()
    

    start = '2020-06-01'
    end = '2020-12-01'
    granularity = 'MONTHLY'
    metrics = 'AMORTIZED_COST'
    groupby = 'SERVICE'

    # get cost per account on the last month
    response = aws_service.get_aws_cost_and_usage(account_number, start, end, granularity, metrics, groupby)

    #create objects to hold the accounts data
    account_list = db_service.create_account(account_number, response)

    #insert accounts to elastic
    db_service.account_bulk_insert_elastic(account_list)

    
def calcBillingOptimizations(event, context):

    client = boto3.client('sts')
    response = client.get_caller_identity()
    account_number = str(response['Account'])

    collect_accounts_cost(account_number, os.environ.get('ACCOUNTS_VISIBILITY_LAST_UPDATE'))
    collect_ec2_all(account_number, os.environ.get('EC2_WASTE_DETECTION_LAST_UPDATE'))
   
     
    body = {'message':'Go Serverless v1.0! Your function executed successfully!',  'input':event}
    response = {'statusCode':200, 'body':json.dumps(body)}
    return response