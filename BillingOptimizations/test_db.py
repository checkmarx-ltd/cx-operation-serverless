import sqlite3
import json
import pandas 
from functools import reduce
from db_service import DbService
from aws_service import AwsService
import asyncio
from performance_counters import PerformanceCounters
import boto3
import pprint
import numpy as np
from thresholds import Thresholds
import handler
import traceback 
import threading
from elasticsearch import Elasticsearch
import elasticsearch.helpers as helpers
from dateutil.relativedelta import *

import datetime 
from datetime import date

FILES_LOCATION = 'metric_files/'


def run_parallel():
    try:
        ec2_metric_list = ['CPUUtilization', 'NetworkOut', 'NetworkIn','DiskWriteBytes','DiskReadBytes','NetworkPacketsOut','NetworkPacketsIn','DiskWriteOps','DiskReadOps']    
        region = 'eu-west-1'
        ec2_instances = []

        chunk_size = 1
        task_number = 1
        tasks = []
        
        aws_service = AwsService()    

        ec2_list = aws_service.get_aws_describe_instances(region)

        threads = []

        for i in range(0, len(ec2_list), chunk_size):
            chunk = ec2_list[i:i+chunk_size]
            for ec2 in chunk:
                print("Main    : create and start thread %d.", i)
                x = threading.Thread(target=handler.collect_ec2_utilization, args=(ec2, ec2_metric_list, region,))
                threads.append(x)
                x.start()
            for index, thread in enumerate(threads):
                print("Main    : before joining thread %d.", i)
                thread.join()
                print("Main    : thread %d done", i)

            threads = []   

        for ec2 in ec2_list:
            print(ec2.instance_id + "." + ec2.instance_owner_id)
            print(ec2.performance_counters_list)
        
        
    except Exception as e:
        print(e)
    

def using_boto3():

    '''
    session = boto3.Session(region_name="eu-west-1")
    ec2 = session.resource('ec2')

    instances = ec2.instances.filter()

    for instance in instances:
        print(instance.id, instance.instance_type, instance.launch_time, instance.ebs_optimized, instance.state['Name'], instance.tags[0]['Value'])

    return
    '''
    parallel_chunk_size = 2
    ec2_list=[1,2,3,4]
    for i in range(0, len(ec2_list), parallel_chunk_size):
        chunk = ec2_list[i:i+parallel_chunk_size]
        for ec2 in chunk:
            print(chunk)
            print("working on " + str(ec2))
            #collect_ec2_utilization(ec2, ec2_metric_list, region)
    return 
    db_service = DbService()

    cloudwatch = boto3.client('cloudwatch', region_name = "eu-west-1")
    metric_name = 'NetworkOut'
    response = cloudwatch.get_metric_statistics(
    Namespace="AWS/EC2",
    Dimensions=[
        {
            'Name': 'InstanceId',
            'Value': 'i-0d4dc0ddfe07c9259'
        }
    ],
    MetricName=metric_name,
    StartTime='2020-12-06T00:00:00',
    EndTime='2020-12-07T00:00:00',
    Period=3600,
    Statistics=[
        'Average'
    ]#,
    #Unit='Bytes'
    )
    
    datapoints = response["Datapoints"]
    df = pandas.DataFrame(columns=[metric_name, "start_time"])

    #df = pandas.DataFrame({metric_name: pandas.Series([], dtype='float64'), "start_time": pandas.Series([], dtype='object')})
    
    for datapoint in datapoints:
        new_row = {metric_name :datapoint["Average"], "start_time":datapoint["Timestamp"]}        
        df = df.append(new_row, ignore_index=True)
    if metric_name == 'CPUUtilization':
        df['is_cpu_utilization_idle'] = np.where(df[metric_name] < Thresholds.cpu_utilization_threshold, 1, 0)
    elif metric_name == 'NetworkIn':
        df['is_network_in_idle'] = np.where(df[metric_name] < Thresholds.network_in_threshold, 1, 0)
    elif metric_name == 'NetworkOut':
            df['is_network_out_idle'] = np.where(df[metric_name] < Thresholds.network_out_threshold, 1, 0)
    elif metric_name == 'NetworkPacketsIn':
        df['is_network_packets_in_idle'] = np.where(df[metric_name] < Thresholds.network_packets_in_threshold, 1, 0)
    elif metric_name == 'NetworkPacketsOut':
        df['is_network_packets_out_idle'] = np.where(df[metric_name] < Thresholds.network_packets_out_threshold, 1, 0)
    elif metric_name == 'DiskWriteOps':
        df['is_disk_write_ops_idle'] = np.where(df[metric_name] < Thresholds.disk_write_ops_threshold, 1, 0)
    elif metric_name == 'DiskReadOps':
        df['is_disk_read_ops_idle'] = np.where(df[metric_name] < Thresholds.disk_read_ops_threshold, 1, 0)
    elif metric_name == 'DiskWriteBytes':
        df['is_disk_write_bytes_idle'] = np.where(df[metric_name] < Thresholds.disk_write_bytes_threshold, 1, 0)
    elif metric_name == 'DiskReadBytes':
        df['is_disk_read_bytes_idle'] = np.where(df[metric_name] < Thresholds.disk_read_bytes_threshold, 1, 0)

    df2 = df
    print(df2)
    #print(df.info())

    metric_name = "CPUUtilization"

    response = cloudwatch.get_metric_statistics(
    Namespace="AWS/EC2",
    Dimensions=[
        {
            'Name': 'InstanceId',
            'Value': 'i-0d4dc0ddfe07c9259'
        }
    ],
    MetricName=metric_name,
    StartTime='2020-12-06T00:00:00',
    EndTime='2020-12-07T00:00:00',
    Period=3600,
    Statistics=[
        'Average'
    ]
    )
    

    datapoints = response["Datapoints"]
    df = pandas.DataFrame(columns=[metric_name, "start_time"])
    for datapoint in datapoints:
        new_row = {metric_name :datapoint["Average"], "start_time":datapoint["Timestamp"]}        
        df = df.append(new_row, ignore_index=True)
    if metric_name == 'CPUUtilization':
        df['is_cpu_utilization_idle'] = np.where(df[metric_name] < Thresholds.cpu_utilization_threshold, 1, 0)
    elif metric_name == 'NetworkIn':
        df['is_network_in_idle'] = np.where(df[metric_name] < Thresholds.network_in_threshold, 1, 0)
    elif metric_name == 'NetworkOut':
            df['is_network_out_idle'] = np.where(df[metric_name] < Thresholds.network_out_threshold, 1, 0)
    elif metric_name == 'NetworkPacketsIn':
        df['is_network_packets_in_idle'] = np.where(df[metric_name] < Thresholds.network_packets_in_threshold, 1, 0)
    elif metric_name == 'NetworkPacketsOut':
        df['is_network_packets_out_idle'] = np.where(df[metric_name] < Thresholds.network_packets_out_threshold, 1, 0)
    elif metric_name == 'DiskWriteOps':
        df['is_disk_write_ops_idle'] = np.where(df[metric_name] < Thresholds.disk_write_ops_threshold, 1, 0)
    elif metric_name == 'DiskReadOps':
        df['is_disk_read_ops_idle'] = np.where(df[metric_name] < Thresholds.disk_read_ops_threshold, 1, 0)
    elif metric_name == 'DiskWriteBytes':
        df['is_disk_write_bytes_idle'] = np.where(df[metric_name] < Thresholds.disk_write_bytes_threshold, 1, 0)
    elif metric_name == 'DiskReadBytes':
        df['is_disk_read_bytes_idle'] = np.where(df[metric_name] < Thresholds.disk_read_bytes_threshold, 1, 0)
    print(df)

    frames = []
    frames.append(df)
    frames.append(df2)

    

    df3 = db_service.merge_metrics_on_start_time (frames)
    print(df3)
         



    
    #df2 = db_service.convert_csv_to_dataframe('metric_files/NetworkOut_i-0d4dc0ddfe07c9259.csv')
    #print(df2.info())
    #print.pprint(response)

    #print(response)
    #print(type(response))

    #df = pandas.DataFrame.from_dict(response)
    #print(df)
    return

    client = boto3.client('ec2')

    response = client.describe_instances()

    response = response['Reservations']

    #response = response['Instances'][0]

    #print(response)

    #InstanceType,LaunchTime,State.Name,EbsOptimized,Tags[0].Value]

    for i in response:
        for s in i['Instances']:
            print(s['InstanceId'])
            print(s['InstanceType'])
            print(s['LaunchTime'])
            print(s['EbsOptimized'])
            print(s['State']['Name'])
            print(s['Tags'][0]['Value'])
        #for s in response['Instances']:
        #    print(s['InstanceId'])

    #json_object = json.dumps(response)   
    #print(json_object)  

    #print(response.get('Reservations.Instances'))
    #print(response['Reservations']['Instances']['InstanceId'][0])

    #for key,value in response.items():
    #    print(key)

    
    
# Prints the nicely formatted dictionary
    #pprint.pprint(response)

    
    #print (response)
    #print(type(response))



def main():

    

    cloudwatch = boto3.client('cloudwatch')


        
    response = cloudwatch.get_metric_statistics(
    Namespace='AWS/EC2',
    Dimensions=[
        {
            'Name': 'InstanceId',
            'Value': 'i-0c5d8b5f06a419b0c'
        }
    ],
    MetricName="NetworkIn",
    StartTime='2021-01-05',
    EndTime='2021-01-10',
    Period=3600,
    Statistics=['Average']
    )      

    pprint.pprint(response)
    return

    session = boto3.Session()
    ec2 = session.resource('ec2')
    instances = ec2.instances.filter()

    client = boto3.client('lambda',region_name='eu-west-1')
    response = client.list_functions()  

    pprint.pprint(response)
    return

    id = boto3.client('sts').get_caller_identity().get('Account')

    name =   boto3.client('organizations').describe_account(AccountId=id).get('Account').get('Name')

    print(name)

    return

    client = boto3.client('ce')
    '''
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': '2021-01-02',
            'End': '2021-01-02'
        },
        Granularity='DAILY',
        Metrics=['AMORTIZED_COST'],
        GroupBy=[
        {
            'Type': 'DIMENSION',
            'Key': 'SERVICE'
        }]
    )

    pprint.pprint(response)
    return
    '''

    response = client.get_cost_forecast(
        TimePeriod={
            'Start': '2021-01-06',
            'End': '2021-02-01'
        },        
        Metric='AMORTIZED_COST',
        Granularity='MONTHLY',
        PredictionIntervalLevel=80,
       
    )

    print(response['ForecastResultsByTime'][0]['MeanValue'])
    print(response['ForecastResultsByTime'][0]['PredictionIntervalLowerBound'])
    print(response['ForecastResultsByTime'][0]['PredictionIntervalUpperBound'])

    return

    given_date = '2020-05-05'

    date_time_obj = datetime.datetime.strptime(given_date, '%Y-%m-%d')

    first_day_of_month = date_time_obj.replace(day=1)

    next_month = first_day_of_month + relativedelta(months=+1)

    first_day_of_month_str = first_day_of_month.strftime('%Y-%m-%d')

    print(first_day_of_month)
    print(first_day_of_month_str)
    print(next_month)
    return

    client = boto3.client('sts')

    

    response = client.get_caller_identity()

    print(response['Account'])

    return

    client = boto3.client('lambda')

    response = client.update_function_configuration(
    FunctionName='billingoptimizations-prod-calcBillingOptimizations',
	Environment={
        'Variables': {
            'TEST': '11111'
        }
    },
    )

    
    response = client.get_function_configuration(FunctionName='Cx-CircleCi-Pipeliene-Status-Shipper')

    print(response)
    return
    
   

    client = boto3.client('ce')
    '''
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': '2020-09-01',
            'End': '2020-12-01'
        },
        Granularity='MONTHLY',
        Metrics=['AMORTIZED_COST'],
        GroupBy=[
        {
            'Type': 'DIMENSION',
            'Key': 'SERVICE'
        }]
    )

    print(response)
    return
    '''

    response = client.get_cost_forecast(
        TimePeriod={
            'Start': '2021-01-05',
            'End': '2021-01-06'
        },        
        Metric='AMORTIZED_COST',
        Granularity='DAILY',
        PredictionIntervalLevel=80,
       
    )

    '''
        Filter = {      
            "Dimensions": {
            "Key": "SERVICE",
            "Values": ["AWS CloudTrail","EC2 - Other"]
            }
        }
    '''

    pprint.pprint(response)

    return

    targetES = Elasticsearch("https://elastic:kJ12iC0bfTVXo3qhpJqRLs87@c11f5bc9787c4c268d3b960ad866adc2.eu-central-1.aws.cloud.es.io:9243")

    now = datetime.datetime.now()
    target_index_name = "account-billing-" + now.strftime("%m-%Y")

    request_body = {
        "settings" : {
            "number_of_shards": 5,
            "number_of_replicas": 1
        },
        'mappings': {            
            'properties': {
                'account': {'type': 'text'},
                'keys': {'type': 'text'},
                'amount': {'type': 'float'},
                'start_time': {'format': 'dateOptionalTime', 'type': 'date'},
                'end_time': {'format': 'dateOptionalTime', 'type': 'date'},                        
                'metrics': {'type': 'text'},
            }}
        }
        
    targetES.indices.create(index = target_index_name, body = request_body)

    return


    

    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': '2020-09-01',
            'End': '2020-12-01'
        },
        Granularity='MONTHLY',
        Metrics=['AMORTIZED_COST'],
        GroupBy=[
        {
            'Type': 'DIMENSION',
            'Key': 'SERVICE'
        }]
    )
    
    pprint.pprint(response)
    
    for row in response['ResultsByTime']:
        pprint.pprint(row['TimePeriod']['Start'])
        pprint.pprint(row['TimePeriod']['End'])
        for group in row['Groups']:
            pprint.pprint(group['Keys'][0])
            pprint.pprint(group['Metrics']['AmortizedCost']['Amount'])
            key_list = list(group['Metrics'].keys())
            pprint.pprint(key_list[0])

        print("************************************")
        
    return


     
    result = response['ResultsByTime'][0]['Groups']
    pprint.pprint(result)

    

    return

    for row in result:
        print(row['Keys'][0])
        print(row['Metrics']['AmortizedCost']['Amount'])
        print("********************")
    #print(result)
    
    #pprint.pprint(result)

    #pprint.pprint(response)
       
    return 

   

    #print(response['Environment']['Variables'])

    results = response['Environment']['Variables']

    for key in results.keys():
        print(results[key])

    

    run_parallel()
    
               
if __name__ == "__main__":
    main()
    