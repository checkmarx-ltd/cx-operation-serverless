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
import sqlite3
from sqlalchemy import create_engine


logger = logging.getLogger(__name__)
c_handler = logging.StreamHandler(stream=sys.stdout)
c_handler.setLevel(logging.ERROR)
c_format = logging.Formatter('%(asctime)s - %(message)s')
c_handler.setFormatter(c_format)
logger.addHandler(c_handler)

class Test_TestMTTR(unittest.TestCase):

    def test_mttr_fixed_data(self):
               
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
        self.assertEqual(incidents,2)
        self.assertEqual(recovery_mean_time,120.0)
        
    def test_mttr_real_data(self):

        logger = logging.getLogger(__name__)
        c_handler = logging.StreamHandler(stream=sys.stdout)
        c_handler.setLevel(logging.ERROR)
        c_format = logging.Formatter('%(asctime)s - %(message)s')
        c_handler.setFormatter(c_format)
        logger.addHandler(c_handler)

        load_dotenv()
        targetESConnectionString = os.getenv("ELASTIC_CONNECTIONSTRING")
        targetES = Elasticsearch(targetESConnectionString)        
        sourceES = targetES

        today = date.today()
        end = (today-timedelta(days=1))
        start = (today - timedelta(days=CalculateRollingMTTR.DAYS_BACK_TO_CALCULATE))

        query = CalculateRollingMTTR.JENKINS_JOBS_POPULATION
        
        job_name = "LumoGitHub/Lumo-Service-ScanRunner/master"        
        source_index_name = "jenkins-sca"
        df_mttr = CalculateRollingMTTR.calculate_mttr_job(sourceES, targetES, job_name, source_index_name, query, start, end, logger)
        df_mttr  = df_mttr.tail(1)

        
        for index, row in df_mttr.iterrows():
            timestamp = row["timestamp"]
            recovery_time_pandas = int(row["recovery_time"])


        timestamp = datetime.strptime(str(timestamp), '%Y-%m-%dT%H:%M:%S')
        
        population = []
        population = CalculateRollingMTTR.get_population(sourceES, source_index_name, timestamp, job_name, query, logger) 

        population['timestamp'] = pd.to_datetime(population['timestamp']).dt.tz_localize(None)

        population['timestamp'] +=  pd.to_timedelta(3, unit='h')

        conn = sqlite3.connect('example.db')
        engine = create_engine('sqlite:///example.db')

        population.to_sql('population', con=engine, if_exists='replace')

        results = engine.execute(
            """
            with raw_data
            as
            (
            SELECT job_name,timestamp,result,lag(result,1,result) over(order by timestamp) as lag_result,
                case when result = lag(result,1,result) over(order by timestamp) then 0 else 1 end as rank
            FROM population 
            ),
            ranking
            as
            (
            select job_name,timestamp,result,sum(rank)over(partition by rank order by timestamp) as rank
            from raw_data
            ),
            grouping
            as
            (
            select min(job_name) as job_name,min(timestamp) as timestamp, min(result) as result 
            from ranking
            group by rank
            ),
            final
            as
            (
            select job_name, timestamp, result,lag(result,1,result) over(order by timestamp) as lag_result,
            lag(timestamp,1,timestamp) over(order by timestamp) lag_timestamp
            from grouping
            )
            select job_name,timestamp,lag_timestamp,'FAILURE',CAST((julianday(timestamp) - julianday(lag_timestamp))*24*60 AS int) AS recovery_time
            from final
            where result = 'SUCCESS' and lag_result = 'FAILURE'
            order by timestamp desc limit 1

                        
                        
            """).fetchall()

        for row in results:
            recovery_time_sqlite = int(row["recovery_time"])

        self.assertEqual(recovery_time_sqlite,recovery_time_pandas)
        

if __name__ == '__main__':
    
    unittest.main()
