import pandas
import traceback 
from functools import reduce
from performance_counters import PerformanceCounters
from thresholds import Thresholds
from account import Account
import numpy as np
from elasticsearch import Elasticsearch
import elasticsearch.helpers as helpers
import datetime 
import os

pandas.set_option('display.max_rows', 500)
pandas.set_option('display.max_columns', 500)
pandas.set_option('display.width', 1000)


class DbService:
      
    def merge_ec2_metrics_on_start_time (self, frames):

        df_merged = reduce(lambda  left,right: pandas.merge(left,right,on=['start_time'], how='outer'), frames)  
        return df_merged  

    
    def account_bulk_insert_elastic(self, account):
        pass

    def ec2_bulk_insert_elastic(self, ec2):

        ElasticConnectionString = os.getenv("ELASTIC_CONNECTIONSTRING")        
        targetES = Elasticsearch(ElasticConnectionString)

        now = datetime.datetime.now()
        target_index_name = "ec2-billing-" + now.strftime("%m-%Y")

        request_body = {
        "settings" : {
            "number_of_shards": 5,
            "number_of_replicas": 1
        },
        'mappings': {            
            'properties': {                
                'start_time': {'format': 'dateOptionalTime', 'type': 'date'},
                'cpu_utilization': {'type': 'float'},
                'network_in': {'type': 'float'},
                'network_out': {'type': 'float'},
                'network_packets_in': {'type': 'float'},
                'network_packets_out': {'type': 'float'},
                'disk_write_ops': {'type': 'float'},
                'disk_read_ops': {'type': 'float'},
                'disk_write_bytes': {'type': 'float'},
                'disk_read_bytes': {'type': 'float'},
                'is_idle': {'type': 'short'},
                'availability_zone': {'type': 'keyword'},
                'instance_id': {'type': 'keyword'},
                'instance_type': {'type': 'keyword'},                
                'launch_time': {'format': 'dateOptionalTime', 'type': 'date'},                        
                'state': {'type': 'keyword'},
                'ebs_optimized': {'type': 'keyword'},
                'tags': {'type': 'keyword'},
                'instance_owner_id': {'type': 'keyword'},                
            }}
        }
        
        #targetES.indices.delete(index=target_index_name, ignore=[400, 404])
        targetES.indices.create(index = target_index_name, body = request_body, ignore=[400, 404])

        df = pandas.DataFrame(columns=["_id","start_time","cpu_utilization","network_in","network_out", "network_packets_in","network_packets_out", \
                "disk_write_ops","disk_read_ops","disk_write_bytes","disk_read_bytes", "is_idle","availability_zone","instance_id","instance_type", \
                     "launch_time", "state", "ebs_optimized", "tags", "instance_owner_id" ])      

        for performance_counters in ec2.performance_counters_list:
                       
            new_row = {"_id": ec2.instance_id + "-" + performance_counters.start_time.strftime("%Y%m%d%H%M%S") ,"start_time": performance_counters.start_time, "cpu_utilization":performance_counters.cpu_utilization, \
                "network_in":performance_counters.network_in, \
                "network_out": performance_counters.network_out, "network_packets_in":performance_counters.network_packets_in, \
                "network_packets_out":performance_counters.network_packets_out, "disk_write_ops": performance_counters.disk_write_ops, \
                    "disk_read_ops": performance_counters.disk_read_ops, "disk_write_bytes":performance_counters.disk_write_bytes, \
                        "disk_write_bytes": performance_counters.disk_write_bytes, "disk_read_bytes":performance_counters.disk_read_bytes, \
                           "is_idle": performance_counters.is_idle, "availability_zone": ec2.availability_zone, "instance_id":ec2.instance_id, \
                               "instance_type":ec2.instance_type, "launch_time":ec2.launch_time, \
                                   "state": ec2.state, "ebs_optimized":ec2.ebs_optimized, "tags":ec2.tags , "instance_owner_id": ec2.instance_owner_id}
            
            df = df.append(new_row, ignore_index=True)
           
        documents = df.to_dict(orient='records')

        try:
            helpers.bulk(targetES, documents, index=target_index_name,doc_type='_doc', raise_on_error=True)
        except Exception as e:
            print(e)
            raise

    def account_bulk_insert_elastic(self, account_list):

        ElasticConnectionString = os.getenv("ELASTIC_CONNECTIONSTRING")
        
        targetES = Elasticsearch(ElasticConnectionString)

        now = datetime.datetime.now()
        target_index_name = "account-billing-" + now.strftime("%m-%Y")

        #targetES.indices.delete(index=target_index_name, ignore=[400, 404])

        request_body = {
        "settings" : {
            "number_of_shards": 5,
            "number_of_replicas": 1
        },
        'mappings': {            
            'properties': {                
                'account': {'type': 'keyword'},
                'keys': {'type': 'text'},
                'amount': {'type': 'float'},
                'start_time': {'format': 'dateOptionalTime', 'type': 'date'},
                'end_time': {'format': 'dateOptionalTime', 'type': 'date'},                        
                'metrics': {'type': 'keyword'},
            }}
        }
        
        targetES.indices.create(index = target_index_name, body = request_body, ignore=[400, 404])

        df = pandas.DataFrame(columns=["_id","account","keys","amount","start_time","end_time","metrics"])

        for account in account_list:

            new_row = {"_id": account.account_number + "-" + account.keys + "-" + datetime.datetime.strptime(account.start, '%Y-%m-%d').strftime("%Y%m%d%H%M%S"), "account":account.account_number,"keys":account.keys,\
                "amount":account.amount,"start_time":account.start,"end_time":account.end,\
                    "metrics":account.metrics}
            
            df = df.append(new_row, ignore_index=True)
        
        documents = df.to_dict(orient='records')

        try:
            helpers.bulk(targetES, documents, index=target_index_name,doc_type='_doc', raise_on_error=True)
        except Exception as e:
            print(e)
            raise
       
    
    def create_account(self, account_number, response):

        account_list = []

        for row in response['ResultsByTime']:
            start = row['TimePeriod']['Start']
            end = row['TimePeriod']['End']
            for group in row['Groups']:
                keys = group['Keys'][0]
                amount = group['Metrics']['AmortizedCost']['Amount']
                key_list = list(group['Metrics'].keys())
                metrics = key_list[0]

                account = Account(account_number = account_number, keys = keys, amount = amount, start = start, end = end, metrics = metrics)

                account_list.append(account)

        return account_list
           

    def create_performance_counters_list(self, df_merged, metric_list):

        performance_counters_list = []

        for index, row in df_merged.iterrows():
            
            start_time = row['start_time'] 

            cpu_utilization = row['CPUUtilization'] if 'CPUUtilization' in metric_list else ''
            network_in = row['NetworkIn'] if 'NetworkIn' in metric_list else ''
            network_out = row['NetworkOut'] if 'NetworkOut' in metric_list else ''      
            network_packets_in = row['NetworkPacketsIn']  if 'NetworkPacketsIn' in metric_list else ''       
            network_packets_out = row['NetworkPacketsOut'] if 'NetworkPacketsOut' in metric_list else ''
            disk_write_ops = row['DiskWriteOps'] if 'DiskWriteOps' in metric_list else ''
            disk_read_ops = row['DiskReadOps'] if 'DiskReadOps' in metric_list else ''
            disk_write_bytes = row['DiskWriteBytes'] if 'DiskWriteBytes' in metric_list else ''
            disk_read_bytes = row['DiskReadBytes'] if 'DiskReadBytes' in metric_list else ''
            is_idle = row['is_cpu_utilization_idle'] if 'CPUUtilization' in metric_list else 1 * \
                row['is_network_in_idle'] if 'NetworkIn' in metric_list else 1 * \
                row['is_network_out_idle'] if 'NetworkOut' in metric_list else 1 * \
                row['is_network_packets_in_idle'] if 'NetworkPacketsIn' in metric_list else 1 * \
                row['is_network_packets_out_idle'] if 'NetworkPacketsOut' in metric_list else 1 * \
                row['is_disk_write_ops_idle'] if 'DiskWriteOps' in metric_list else 1 * \
                row['is_disk_read_ops_idle'] if 'DiskReadOps' in metric_list else 1 * \
                row['is_disk_write_bytes_idle'] if 'DiskWriteBytes' in metric_list else 1 * \
                row['is_disk_read_bytes_idle'] if 'DiskReadBytes' in metric_list else 1 

            performance_counters = PerformanceCounters(start_time = start_time,cpu_utilization = cpu_utilization, network_in = network_in, network_out = network_out, network_packets_in = network_packets_in, network_packets_out = network_packets_out, disk_write_ops = disk_write_ops, disk_read_ops = disk_read_ops, disk_write_bytes = disk_write_bytes, disk_read_bytes = disk_read_bytes, is_idle = is_idle)
            performance_counters_list.append(performance_counters)  

        return performance_counters_list
                
            
           
        
        

