import os
import boto3
from ec2 import EC2
from thresholds import Thresholds
import pandas
import numpy as np
from account import Account
from decimal import Decimal
from datetime import datetime

class AwsService: 

    def get_supported_metrics(self, ec2):
        
        metric_list = []
        
        client = boto3.client('cloudwatch')

        response = client.list_metrics(Namespace='AWS/EC2',
            Dimensions=[
            {
                'Name': 'InstanceId',
                'Value': ec2.instance_id
            },
        ])

        for metric in response['Metrics']:
            metric_list.append(metric['MetricName'])

        return metric_list

    def get_aws_cost_forecast(self, account_number, start, end, granularity, metrics, groupby):
        
        response = ""

        client = boto3.client('ce')
        
        try:
            response = response = client.get_cost_forecast(
                TimePeriod={
                'Start': start,
                'End': end
                },
                Granularity=granularity,
                Metric=metrics,            
                Filter={      
                    "Dimensions": {
                    "Key": "SERVICE",
                    "Values": [
                        groupby
                    ]
                    }
                },
                PredictionIntervalLevel=80,
            )
        except Exception as e:
            if type(Exception) == "botocore.errorfactory.DataUnavailableException":
                pass
                return

        return response


    def get_aws_cost_and_usage(self, account_number, start, end, granularity, metrics, groupby):
        
        client = boto3.client('ce')

        response = client.get_cost_and_usage(
            TimePeriod={
                'Start': start,
                'End': end
            },
            Granularity=granularity,
            Metrics=[metrics],
            GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': groupby
            }]
        )

        return response
       
    def get_aws_describe_instances(self):
        
        ec2_list = []
        session = boto3.Session()
        ec2 = session.resource('ec2')
        instances = ec2.instances.filter()

        
        for instance in instances:
            availability_zone = instance.placement["AvailabilityZone"]           
            state = instance.state['Name']            
            try:
                account_number = instance.network_interfaces_attribute[0]['OwnerId']
                account_name = Account.map_account_name_to_account_number(account_number)                
                pu = Account.map_pu_to_account(account_number)
            except Exception as e:
                pass

            if state == "running":
           
                ec2 = EC2(availability_zone, instance.id, instance.instance_type, instance.launch_time, state,  instance.ebs_optimized, account_number, pu, account_name)
                ec2_list.append(ec2)

        return ec2_list    
    
    def get_aws_cost_and_usage_with_resources(self, ec2, start_time, end_time, granularity, metrics):

        client = boto3.client('ce')

        response = client.get_cost_and_usage_with_resources(
        TimePeriod={
            'Start': start_time,
            'End': end_time
        },
        Metrics=[metrics],
        Granularity=granularity,
        Filter={
            "And": 
            [
                {
                    "Dimensions": { 
                        "Key": "SERVICE",
                        "MatchOptions": [ "EQUALS" ],
                        "Values": [ "Amazon Elastic Compute Cloud - Compute" ]
                    }
                },
                {
                    "Dimensions": { 
                        "Key": "RESOURCE_ID",
                        "MatchOptions": [ "EQUALS" ],
                        "Values": [ ec2.instance_id ]
                    }
                }
            ]
        }
        )

        df = pandas.DataFrame(columns=["cost", "start_time"])

        datapoints = response["ResultsByTime"]

        for datapoint in datapoints:

            cost = round(Decimal(datapoint['Total']['AmortizedCost']['Amount']),4)
            start_time = datapoint['TimePeriod']['Start']
            start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
            #start_time = start_time.astimezone()
            new_row = {"cost" : cost, "start_time": start_time}
            df = df.append(new_row, ignore_index=True)

        return df

        

    def get_aws_metric_statistics(self, ec2, metric_name, period, start_time, end_time, namespace, statistics):
        
        cloudwatch = boto3.client('cloudwatch')

        start_time = start_time + 'T00:00:00Z'
        end_time = end_time + 'T00:00:00Z'

        start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
        end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')        
        
        response = cloudwatch.get_metric_statistics(
        Namespace=namespace,
        Dimensions=[
            {
                'Name': 'InstanceId',
                'Value': ec2.instance_id
            }
        ],
        MetricName=metric_name,
        StartTime=start_time,
        EndTime=end_time,
        Period=period,
        Statistics=[statistics]
        )        

        datapoints = response["Datapoints"]

        df = pandas.DataFrame(columns=[metric_name, "start_time"])
        
        for datapoint in datapoints:
            start_time = datapoint["Timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            new_row = {metric_name :datapoint["Average"], "start_time": start_time}
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
        
        return df    
       