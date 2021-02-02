import pandas
import traceback 
from performance_counters import PerformanceCounters
from thresholds import Thresholds
from account import Account
import numpy as np
from elasticsearch import Elasticsearch
import elasticsearch.helpers as helpers
import datetime 
import os
from decimal import Decimal


pandas.set_option('display.max_rows', 10000)
pandas.set_option('display.max_columns', 10000)
pandas.set_option('display.width', 10000)


class DbService:
      
    def ec2_bulk_insert_elastic(self, ec2):

        ElasticConnectionString = os.getenv("ELASTIC_CONNECTIONSTRING")        
        targetES = Elasticsearch(ElasticConnectionString)

        now = datetime.datetime.now()
        target_index_name = "ec2-cost-" + now.strftime("%m-%Y")
        index_template_name = "ec2-cost-template"


        request_body = {
        "index_patterns": ["ec2-cost-*"],
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        'mappings': {            
            'properties': {                
                'start_time': {'format': 'dateOptionalTime', 'type': 'date'},
                'cpu_utilization': {'type': 'float'},
                'network_in': {'type': 'float'},
                'network_out': {'type': 'float'},                
                'disk_write_ops': {'type': 'float'},
                'disk_read_ops': {'type': 'float'},
                'disk_write_bytes': {'type': 'float'},
                'disk_read_bytes': {'type': 'float'},
                'ebs_write_bytes': {'type': 'float'},
                'ebs_read_bytes': {'type': 'float'},
                'is_idle': {'type': 'short'},
                'availability_zone': {'type': 'keyword'},
                'instance_id': {'type': 'keyword'},
                'instance_type': {'type': 'keyword'},                
                'launch_time': {'format': 'dateOptionalTime', 'type': 'date'},                        
                'state': {'type': 'keyword'},
                'ebs_optimized': {'type': 'keyword'},
                'account_number': {'type': 'keyword'},  
                'pu': {'type': 'keyword'}, 
                'account_name': {'type': 'keyword'},   
                'cost': {'type': 'float'},            
            }}
        }                        


        if not targetES.indices.exists_template(index_template_name):
            targetES.indices.put_template(index_template_name, request_body, create=True)

        #targetES.indices.delete(index=target_index_name, ignore=[400, 404])
        #targetES.indices.create(index = target_index_name, body = request_body, ignore=[400, 404])

        #alias is needed for data retention policy
        #targetES.indices.put_alias(index=target_index_name, name='ec2-cost', ignore=[400, 404])


        df = pandas.DataFrame(columns=["_id","start_time","cpu_utilization","network_in","network_out", "ebs_write_bytes", "ebs_read_bytes", \
                "disk_write_ops","disk_read_ops","disk_write_bytes","disk_read_bytes", "is_idle","availability_zone","instance_id","instance_type", \
                     "launch_time", "state", "ebs_optimized", "account_number", "pu", "account_name", "cost"])      

        for performance_counters in ec2.performance_counters_list:
                       
            new_row = {"_id": ec2.instance_id + "-" + performance_counters.start_time.strftime("%Y%m%d%H%M%S") ,"start_time": performance_counters.start_time, "cpu_utilization":performance_counters.cpu_utilization, \
                "network_in":performance_counters.network_in, \
                "network_out": performance_counters.network_out, "ebs_write_bytes":performance_counters.ebs_write_bytes, \
                "ebs_read_bytes":performance_counters.ebs_read_bytes, "disk_write_ops": performance_counters.disk_write_ops, \
                    "disk_read_ops": performance_counters.disk_read_ops, "disk_write_bytes":performance_counters.disk_write_bytes, \
                         "disk_read_bytes":performance_counters.disk_read_bytes, \
                           "is_idle": performance_counters.is_idle, "availability_zone": ec2.availability_zone, "instance_id":ec2.instance_id, \
                               "instance_type":ec2.instance_type, "launch_time":ec2.launch_time, \
                                   "state": ec2.state, "ebs_optimized":ec2.ebs_optimized,  "account_number": ec2.account_number, "pu": ec2.pu, \
                                        "account_name": ec2.account_name, "cost": performance_counters.cost}
            
            df = df.append(new_row, ignore_index=True)
           
        documents = df.to_dict(orient='records')

        try:
            helpers.bulk(targetES, documents, index=target_index_name,doc_type='_doc', raise_on_error=True)
        except Exception as e:
            print(e)
            print(documents)
            raise

    def account_bulk_insert_elastic(self, account_list):

        ElasticConnectionString = os.getenv("ELASTIC_CONNECTIONSTRING")
        
        targetES = Elasticsearch(ElasticConnectionString)

        now = datetime.datetime.now()
        target_index_name = "account-cost-" + now.strftime("%m-%Y")
        index_template_name = "account-cost-template"

        #targetES.indices.delete(index=target_index_name, ignore=[400, 404])
        request_body = {
        "index_patterns": ["account-cost-*"],
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        'mappings': {            
            'properties': { 
                'pu': {'type': 'keyword'},   
                'account_name': {'type': 'keyword'},          
                'account_number': {'type': 'keyword'},
                'keys': {'type': 'keyword'},
                'amount': {'type': 'float'},
                'start_time': {'format': 'dateOptionalTime', 'type': 'date'},
                'end_time': {'format': 'dateOptionalTime', 'type': 'date'},                        
                'metrics': {'type': 'keyword'},
                'forecast_mean_value': {'type': 'float'},
                'forecast_prediction_interval_lowerbound': {'type': 'float'},
                'forecast_prediction_interval_upperbound': {'type': 'float'},
            }}
        }

        if not targetES.indices.exists_template(index_template_name):
            targetES.indices.put_template(index_template_name, request_body, create=True)
        
        #targetES.indices.create(index = target_index_name, body = request_body, ignore=[400, 404])

        #alias is needed for data retention policy
        #targetES.indices.put_alias(index=target_index_name, name='account-cost', ignore=[400, 404])

        df = pandas.DataFrame(columns=["_id","pu", "account_name", "account_number","keys","amount","start_time","end_time","metrics","forecast_mean_value","forecast_prediction_interval_lowerbound","forecast_prediction_interval_upperbound"])

        for account in account_list:

            new_row = {"_id": account.account_number + "-" + account.keys + "-" + datetime.datetime.strptime(account.start, '%Y-%m-%d').strftime("%Y%m%d%H%M%S"), \
                "pu": account.pu, "account_name":account.account_name, "account_number":account.account_number,"keys":account.keys,\
                "amount":account.amount,"start_time":account.start,"end_time":account.end,\
                    "metrics":account.metrics, "forecast_mean_value": account.forecast_mean_value, \
                        "forecast_prediction_interval_lowerbound": account.forecast_prediction_interval_lowerbound, \
                            "forecast_prediction_interval_upperbound": account.forecast_prediction_interval_upperbound}
            
            df = df.append(new_row, ignore_index=True)
        
        documents = df.to_dict(orient='records')

        try:
            helpers.bulk(targetES, documents, index=target_index_name,doc_type='_doc', raise_on_error=True)
        except Exception as e:
            print(e)
            raise

    def print_account_list(self, account_list):

        for account in account_list:
            print(f"pu = {account.pu}, account_name = {account.account_name}, account_number = {account.account_number}, start = {account.start}, end = {account.end}, metrics = {account.metrics}, keys = {account.keys}, amount = {account.amount}, forecast = {account.forecast_mean_value}, interval_lowerbound = {account.forecast_prediction_interval_lowerbound}, interval_upperbound = {account.forecast_prediction_interval_upperbound}")
       
    
    def create_account(self, account_number, response):

        account_list = []

        for row in response['ResultsByTime']:
            start = row['TimePeriod']['Start']
            end = row['TimePeriod']['End']
            for group in row['Groups']:
                #keys = service
                keys = group['Keys'][0]
                amount = round(Decimal(group['Metrics']['AmortizedCost']['Amount']),4)
                key_list = list(group['Metrics'].keys())
                #metrics = 'AmortizedCost'
                metrics = key_list[0]

                pu = Account.map_pu_to_account(account_number)
                account_name = Account.map_account_name_to_account_number(account_number)                

                account = Account(pu = pu, account_name = account_name, account_number = account_number, keys = keys, amount = amount, start = start, end = end, metrics = metrics)

                account_list.append(account)

        return account_list
           

    def create_performance_counters_list(self, df_merged, metric_list):

        performance_counters_list = []

        for index, row in df_merged.iterrows():
            
            start_time = row['start_time'] 
            cost = row['cost']
            cpu_utilization = row['CPUUtilization'] if 'CPUUtilization' in metric_list and 'CPUUtilization' in df_merged.columns else 0
            network_in = row['NetworkIn'] if 'NetworkIn' in metric_list and 'NetworkIn' in df_merged.columns else 0
            network_out = row['NetworkOut'] if 'NetworkOut' in metric_list and 'NetworkOut' in df_merged.columns else 0                  
            disk_write_ops = row['DiskWriteOps'] if 'DiskWriteOps' in metric_list and 'DiskWriteOps' in df_merged.columns else 0
            disk_read_ops = row['DiskReadOps'] if 'DiskReadOps' in metric_list and 'DiskReadOps' in df_merged.columns else 0
            disk_write_bytes = row['DiskWriteBytes'] if 'DiskWriteBytes' in metric_list and 'DiskWriteBytes' in df_merged.columns else 0
            disk_read_bytes = row['DiskReadBytes'] if 'DiskReadBytes' in metric_list and 'DiskReadBytes' in df_merged.columns else 0
            ebs_read_bytes = row['EBSReadBytes'] if 'EBSReadBytes' in metric_list and 'EBSReadBytes' in df_merged.columns else 0
            ebs_write_bytes = row['EBSWriteBytes'] if 'EBSWriteBytes' in metric_list and 'EBSWriteBytes' in df_merged.columns else 0


            ebs_read_bytes

            is_idle = row['is_cpu_utilization_idle'] if 'CPUUtilization' in metric_list and 'is_cpu_utilization_idle' in df_merged.columns else 1 * \
                row['is_network_in_idle'] if 'NetworkIn' in metric_list and 'is_network_in_idle' in df_merged.is_network_in_idle else 1 * \
                row['is_network_out_idle'] if 'NetworkOut' in metric_list and 'is_network_out_idle' in df_merged.columns else 1 
                

            performance_counters = PerformanceCounters(start_time = start_time,cpu_utilization = cpu_utilization, network_in = network_in, network_out = network_out, disk_write_ops = disk_write_ops, disk_read_ops = disk_read_ops, ebs_write_bytes = ebs_write_bytes, ebs_read_bytes = ebs_read_bytes,  disk_write_bytes = disk_write_bytes, disk_read_bytes = disk_read_bytes, is_idle = is_idle, cost = cost)
            performance_counters_list.append(performance_counters)  

        return performance_counters_list
                
            
           
        
        

