import  CalculateRollingMTTR   # The code to test
import logging
from logging import Logger
import unittest   # The test framework
import pandas as pd
import numpy as np
import sys
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta,date, time
from dotenv import load_dotenv
import os


logger = logging.getLogger(__name__)
c_handler = logging.StreamHandler(stream=sys.stdout)
c_handler.setLevel(logging.ERROR)
c_format = logging.Formatter('%(asctime)s - %(message)s')
c_handler.setFormatter(c_format)
logger.addHandler(c_handler)

class Test_TestIncrementDecrement(unittest.TestCase):
    def test_mttr_calvulation(self):
        
        #date1 = datetime.now()-timedelta(minutes=300)
        #date2 = datetime.now()-timedelta(minutes=200)
        #date3 =datetime.now()-timedelta(minutes=100)

        

        df =[]

        df = pd.DataFrame([["myJob","SUCCESS" ,"2020-06-16T01:00:00.000Z"]],columns=list(["job_name", "result", "timestamp"]))
        df1 = pd.DataFrame([["myJob","FAILURE","2020-06-17T23:00:00.000Z"]],columns=list(["job_name", "result", "timestamp"]))
        df2 = pd.DataFrame([["myJob","FAILURE","2020-06-18T01:00:00.000Z"]],columns=list(["job_name", "result", "timestamp"]))
        df3 = pd.DataFrame([["myJob","SUCCESS","2020-06-18T02:00:00.000Z"]],columns=list(["job_name", "result", "timestamp"]))
        df4 = pd.DataFrame([["myJob","FAILURE","2020-06-18T03:00:00.000Z"]],columns=list(["job_name", "result", "timestamp"]))
        df5 = pd.DataFrame([["myJob","SUCCESS","2020-06-18T04:00:00.000Z"]],columns=list(["job_name", "result", "timestamp"]))

        df = df.append(df1)
        df = df.append(df2)        
        df = df.append(df3)
        df = df.append(df4)
        df = df.append(df5)
     
        raw_df = CalculateRollingMTTR.create_raw_dataframe(df, logger)

        start = "2020-06-18"

        grouped_df = CalculateRollingMTTR.create_grouped_dataframe(raw_df,start) 

        recovery = float(grouped_df["recovery_time"])
        incidents = int(grouped_df["incidents"])
        recovery_mean_time = recovery/incidents

        
        self.assertEqual(recovery, 240.0)
        print("\n")
        print (f"recovery  = {recovery}")
        self.assertEqual(incidents,2)
        print (f"incidents = {incidents}")
        self.assertEqual(recovery_mean_time,120.0)
        print(f"recovery_mean_time = {recovery_mean_time}")

    def test_mttr_calvulation(self):
        
        load_dotenv()
        targetESConnectionString = os.getenv("ELASTIC_CONNECTIONSTRING")
        targetES = Elasticsearch(targetESConnectionString)
        
        sourceES = targetES
        '''
        end = (date.today() - timedelta(days=1))
        start = end -timedelta(days=30)
        '''
        start = date(2020, 7, 16)
        end   = date(2020, 7, 16)
        #res = CalculateRollingMTTR.get_all_jobs(sourceES,start, end, logger)
        job_name = "LumoGitHub/Lumo-Service-SASTProxy/master"
        source_index_name = "jenkins-sca"
        query = CalculateRollingMTTR.JENKINS_JOBS_POPULATION

        population = []
        population = CalculateRollingMTTR.get_population(sourceES, source_index_name, start, job_name, query, logger) 

        print(population)
       
        df_mttr = CalculateRollingMTTR.calculate_mttr_job(sourceES, targetES, job_name, source_index_name, query, start, end, logger)
        
        print(df_mttr)
    

if __name__ == '__main__':
    
    unittest.main()
