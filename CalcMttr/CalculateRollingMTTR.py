import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
import elasticsearch.helpers as helpers
from datetime import datetime, timedelta,date, time
import sys
import argparse
import logging
from logging import Logger
from dotenv import load_dotenv
import os

DAYS_BACK_TO_CALCULATE = 30
INDEX_PREFIX = 'mttr-'
SCA_INDEX_NAME = "jenkins-sca"
CODEBASHING_INDEX_NAME = "jenkins-codebahsing"
LOGSTASH_INDEX_NAME = "logstash-jenkins"
CIRCLECI_INDEX_NAME = "circleci"
TFS_INDEX_NAME = "tfs"

TFS_JOBS = """
{
"_source": ["definition.name","startTime","status"],
"query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "match_all": {}
        },
        {
          "range": {
            "startTime": {
              "gte": "{0}",
              "lte": "{1}",
              "format": "strict_date_optional_time"
            }
          }
        }
      ],
      "should": [],
      "must_not": []
    }
  }
}
"""

TFS_JOBS_POPULATION = """
{
"_source": ["definition.name","startTime","status"],
"sort" : [
        { "startTime" : {"order" : "asc"}}],
"query": {
    "bool": {
    "must": [],
    "filter": [
        {
        "match_all": {}
        },
        {
        "match_phrase": {
            "definition.name": "{0}"
          }
        }
            ,
            {
            "range": {
                "startTime": {
                "gte": "{1}",
                "lte": "{2}",
                "format": "strict_date_optional_time"
                }
            }
            }
        ]
        }
    }
}
"""


CIRCLECI_JOBS = """
{
"_source": ["name","created_at","status"],
"query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "match_all": {}
        },
        {
          "range": {
            "created_at": {
              "gte": "{0}",
              "lte": "{1}",
              "format": "strict_date_optional_time"
            }
          }
        }
      ],
      "should": [],
      "must_not": []
    }
  }
}
"""

CIRCLECI_JOBS_POPULATION = """
{
"_source": ["name","created_at","status"],
"sort" : [
        { "created_at" : {"order" : "asc"}}],
"query": {
    "bool": {
    "must": [],
    "filter": [
        {
        "match_all": {}
        },
        {
        "match_phrase": {
            "name": "{0}"
          }
        }
            ,
            {
            "range": {
                "created_at": {
                "gte": "{1}",
                "lte": "{2}",
                "format": "strict_date_optional_time"
                }
            }
            }
        ]
        }
    }
}
"""

JENKINS_JOBS_POPULATION = """
{
"_source": ["data.buildVariables.JOB_NAME","@timestamp","data.result"],
"sort" : [
        { "@timestamp" : {"order" : "asc"}}],
"query": {
    "bool": {
    "must": [],
    "filter": [
        {
        "match_all": {}
        },
        {
        "match_phrase": {
            "data.buildVariables.JOB_NAME": "{0}"
          }
        }
            ,
            {
            "range": {
                "@timestamp": {
                "gte": "{1}",
                "lte": "{2}",
                "format": "strict_date_optional_time"
                }
            }
            }
        ]
        }
    }
}
"""

JENKINS_JOBS = """
{
"_source": ["data.buildVariables.JOB_NAME","@timestamp","data.result"],
"sort" : [
        { "@timestamp" : {"order" : "asc"}}],
"query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "match_all": {}
        },
        {
          "range": {
            "@timestamp": {
              "gte": "{0}",
              "lte": "{1}",
              "format": "strict_date_optional_time"
            }
          }
        }
      ],
      "should": [],
      "must_not": []
    }
  }
}
"""

GET_TARGET_INDEX_DATA = """
{
    "_source": ["job_name","result","timestamp","recovery_time","incidents"],
    "sort" : [
            { "timestamp" : {"order" : "asc"}}],
        "query": {
            "term": {
                "job_name": {
                    "value": "{0}"
                }
            }
        }
}
"""

def get_all_jobs(sourceES, start,end, logger): 

    all_jobs = None

    try:
        jenkins_jobs      = get_job_names(sourceES,LOGSTASH_INDEX_NAME, "JENKINS_JOBS",start, end, logger)
        codebashing_jobs  = get_job_names(sourceES,CODEBASHING_INDEX_NAME, "JENKINS_JOBS",start, end, logger)
        sca_jobs          = get_job_names(sourceES,SCA_INDEX_NAME, "JENKINS_JOBS",start, end, logger)
        circleci_jobs     = get_job_names(sourceES,CIRCLECI_INDEX_NAME, "CIRCLECI_JOBS",start, end, logger)
        #tfs_jobs          = get_job_names(sourceES,TFS_INDEX_NAME, "TFS_JOBS",start, end, logger)
        
        #frames = [jenkins_jobs, codebashing_jobs, sca_jobs, circleci_jobs, tfs_jobs]
        frames = [jenkins_jobs, codebashing_jobs, sca_jobs, circleci_jobs]
        
        all_jobs = pd.concat(frames)

        return all_jobs

    except Exception:
        logger.exception(f"Exception occurred in get_all_jobs {start},{end}")
        raise

def get_job_names(sourceES, index_name, query, start, end, logger):

    try:
        df = pd.DataFrame(columns=["job_name"])        
        jobs_query = globals()[query]
        jobs_query = jobs_query.replace('{0}',start.strftime("%Y-%m-%d")).replace('{1}',end.strftime("%Y-%m-%d"))
        res = sourceES.search(index=index_name, body=jobs_query, size = 1000)

        for hit in (res["hits"]["hits"]): 
            try:           
                if (index_name == CIRCLECI_INDEX_NAME):
                    new_row = {"job_name":  hit['_source']['name'],
                                "query_name": query + "_POPULATION",
                                "index_name": index_name
                        }
                elif (index_name == TFS_INDEX_NAME):
                    new_row = {"job_name":  hit['_source']['definition']['name'],
                                "query_name": query + "_POPULATION",
                                "index_name": index_name
                        }
                else:
                    new_row = {"job_name":  hit['_source']['data']['buildVariables']['JOB_NAME'],
                            "query_name": query + "_POPULATION",
                            "index_name": index_name
                        }
                df = df.append(new_row, ignore_index=True)
            except Exception:
                pass

        df = df.drop_duplicates(subset='job_name', keep="first")
        
        ########################## for debug ######################################
        #df = df.loc[df['job_name'] == 'LumoGitHub/Lumo-Service-SASTProxy/master']#
        #print(df)                                                                #
        ########################## end debug ######################################
        return df

    except Exception:
        logger.exception(f"Exception occurred in get_job_names {index_name}")
        raise


def get_population(sourceES, index_name, start, job_name, query, logger):

    try:
        # retun each job name result like SUCCESS or FAILURE on timestamp        
        # we calculate the time of recovery of incidents and attached the value to the day it ends.
        # but an incident can start in one day and finishes on another day so we need to takes several days before (7)

        end   = start
        start = end - timedelta(days=7)
        query = query.replace("{0}", job_name).replace("{1}", start.strftime("%Y-%m-%d")).replace("{2}", end.strftime("%Y-%m-%d"))
        df = pd.DataFrame(columns=["job_name","timestamp", "result"])
        res = sourceES.search(index=index_name, body=query, size = 1000)
    except Exception:
        logger.exception(f"Exception occurred in get_population {start},{end}, {query.format(start,end, job_name)}, {sourceESConn}")
        raise  


    try:
        for hit in (res["hits"]["hits"]):
            if (index_name == CIRCLECI_INDEX_NAME):
                status = hit['_source']['status']
                if status == "failed":
                    status = "FAILURE"
                else:
                    status = status.upper()
                new_row = {"job_name":  hit['_source']['name'],
                           "timestamp": hit['_source']['created_at'],
                           "result":    status }
            elif (index_name == TFS_INDEX_NAME):
                status = hit['_source']['status']
                if status == "failed":
                    status = "FAILURE"
                if status == "succeeded" or status == "completed":
                    status = "SUCCESS"
                else:
                    status = status.upper()
                new_row = {"job_name":  hit['_source']['definition']['name'],
                           "timestamp": hit['_source']['startTime'],
                           "result":    status }
            else:        
                new_row = {"job_name":  hit['_source']['data']['buildVariables']['JOB_NAME'],
                           "timestamp": hit['_source']['@timestamp'],
                           "result":    hit['_source']['data']['result'] }
            df = df.append(new_row, ignore_index=True)
    except Exception:
        pass
        
    return df

def get_target_index_data(targrtES, query, index_name, job_name, logger):

    query = query.replace("{0}", job_name)
    df = pd.DataFrame(columns=["job_name","result","timestamp","recovery_time","incidents"])
    res = targrtES.search(index=index_name, body=query, size = 1000)

    for hit in (res["hits"]["hits"]):
        new_row = {"job_name":  hit['_source']['job_name'],
                    "result": hit['_source']['result'],
                    "timestamp": hit['_source']['timestamp'],
                    "recovery_time": hit['_source']['recovery_time'],
                    "incidents": hit['_source']['incidents']
                }
        df = df.append(new_row, ignore_index=True)
    return df

def create_raw_dataframe(data, logger):

    # return of each job name its values and its previous values
    # like for job name Smoke-Dynamic-CI-Tests-9.3.0 return its current result like SUCCESS and its previous by time its result like FAILURE
    # add rank to rows meaning that every consequently rows with the same result valve will be given the same rank
    # e.g.    result  rank  timestamp           timestamp_lagged            begin_of_day
    #         SUCCESS 1     '2020-01-01 01:00'  '2019-12-12 02:00'          '2020-01-01'
    #         FAILURE 2     '2020-03-03 04:00'  '2020-01-01 01:00'          '2020-03-03'
    #         FAILURE 2     '2020-05-05 05:00'  '2020-03-03 04:00'          '2020-05-05'
    #         SUCCESS 3     '2020-07-07 02:00' ' 2020-05-05 05:00'          -2020-07-07'

    row_number        = 0
    incidents         = 0
    total_time        = 0
    step_time         = 0
    rank_list         = []
    begin_of_day_list = []

    # we calculate the time of recovery of incidents and attached the value to the day it ends.
    # but an incident can start in one day and finishes on another day so we need to takes several days before (7)
    #start = end - timedelta(days=7)

    try:
        df_a = pd.DataFrame(data)
        if df_a.empty:
            return df_a

        df_a['timestamp_lag'] = (df_a.sort_values(by=['timestamp'], ascending=True)
                            .groupby(['job_name'])['timestamp'].shift(1,fill_value='1970-01-01T00:00:00.000Z'))

        df_a['result_lag'] = (df_a.sort_values(by=['timestamp'], ascending=True)
                            .groupby(['job_name'])['result'].shift(1))

        df_a['status'] = df_a['result_lag'] == df_a['result']

        for index, row in df_a.iterrows():

            row_number = row_number +  np.where (row['result_lag'] == row['result'],0,1)
            d1 = pd.to_datetime(row['timestamp'])
            begin_of_day = datetime.combine(d1, time())
            rank_list.append(row_number)
            begin_of_day_list.append(begin_of_day)

        df_a['rank'] = rank_list
        df_a['timestamp_begin_of_day'] = begin_of_day_list

        return df_a

    except Exception:
        logger.exception(f"Exception occurred in create_raw_dataframe. raw dataframe = {df_a}")
        raise

def create_grouped_dataframe(raw_df,date):

    # Look at the example from create_raw_dataframe
    # the records will be grouped to (third row eliminatd)
    # e.g.    result  rank  timestamp           timestamp_lagged            begin_of_day
    #         SUCCESS 1     '2020-01-01 01:00'  '2019-12-12 02:00'          '2020-01-01'
    #         FAILURE 2     '2020-03-03 04:00'  '2020-01-01 01:00'          '2020-03-03'
    #         SUCCESS 3     '2020-07-07 02:00' ' 2020-05-05 05:00'          '2020-07-07'
 
    
    grouped = raw_df.groupby(['job_name','rank','result']).agg(
    start_timestamp = ('timestamp',np.min),
    timestamp_begin_of_day = ('timestamp_begin_of_day',np.min)
    )

    if grouped.empty:

        return grouped

    grouped = grouped.reset_index()
    grouped['timestamp_lag'] = (grouped.sort_values(by=['start_timestamp'], ascending=True)
        .groupby(['job_name'])['start_timestamp'].shift(1,fill_value='1970-01-01T00:00:00.000Z'))

    grouped['result_lag'] = (grouped.sort_values(by=['start_timestamp'], ascending=True)
        .groupby(['job_name'])['result'].shift(1,fill_value='SUCCESS'))

    grouped = grouped[np.logical_and(grouped.result == "SUCCESS",grouped.result_lag=='FAILURE')]
    grouped['total_time'] = ((pd.to_datetime(grouped['start_timestamp'])-pd.to_datetime(grouped['timestamp_lag'])).dt.total_seconds())/60
    grouped['incidents'] = 1

    # e.g.    result  rank  timestamp           timestamp_lagged            begin_of_day    result_lagged   incidents    total_time
    #         SUCCESS 1     '2020-01-01 01:00'  '2019-12-12 02:00'          '2020-01-01'    SUCCESS         1            '2020-01-01 01:00'-'2019-12-12 02:00'
    #         FAILURE 2     '2020-03-03 04:00'  '2020-01-01 01:00'          '2020-03-03'    SUCCESS         1            '2020-03-03 04:00'-'2020-01-01 01:00'
    #         SUCCESS 3     '2020-07-07 02:00'  '2020-05-05 05:00'          '2020-07-07'    FAILURE         1            '2020-07-07 02:00'-'2020-05-05 05:00'

    grouped = grouped.groupby(['job_name','result_lag','timestamp_begin_of_day']).agg(
        recovery_time = ('total_time',np.sum),
        incidents = ('incidents',np.sum)
    )

    grouped = grouped.reset_index()
    grouped.rename(columns = {"result_lag": "result","timestamp_begin_of_day": "timestamp"}, inplace=True)
    grouped = grouped[grouped.timestamp == str(date)]

    return grouped

    

def create_index(client,index_name):

    client.indices.create(
        index=index_name,
        body={
            "settings": {"number_of_shards": 1},
            "mappings": {
                "properties": {
                    "job_name": {"type": "keyword"},
                    "result": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "recovery_time": {"type": "double"},
                    "incidents": {"type": "integer"},
                    "recovery_mean_time": {"type": "double"},
                    "incidents_mean": {"type": "integer"},
                }
            },
        },
    )

    return False

def is_index_exists(client, index_name, logger):

    try:
        if client.indices.exists(index=index_name):
            return True
        return False
    except Exception:
        logger.exception(f"Exception occurred in is_index_exists. index name = {index_name}")
        raise

def drop_index(client, index_name, logger):
    try:
        client.indices.delete(index=index_name, ignore=[400, 404])
    except Exception:
        logger.error("failed to drop index")
        logger.exception(f"Exception occurred. index name = {index_name}")
        raise

def init_args():

    try:
        args={}
        ap = argparse.ArgumentParser()
        ap.add_argument("-se", required=True,
            help="name of the source Elasticsearch server name or ip")
        ap.add_argument("-sp", required=True,default = 9200,
            help="port of the source Elasticsearch")
        ap.add_argument("-su", required=True, default="None",
            help="user of the source Elasticsearch")
        ap.add_argument("-ss", required=True, default="None",
            help="password of the source Elasticsearch")
        ap.add_argument("-te", required=True,
            help="name of the target Elasticsearch server name or ip")
        ap.add_argument("-tp", required=True, default = 9200,
            help="port of the target Elasticsearch")
        ap.add_argument("-tu", required=True, default="None",
            help="user of the target Elasticsearch")
        ap.add_argument("-ts", required=True, default="None",
            help="password of the target Elasticsearch")
        args = vars(ap.parse_args())   

        return args

    except Exception:
        logging.exception("Exception occurred")
        raise

def init_connections():

    sourceElastic   = os.getenv("SOURCE_ELASTIC")
    sourceUser      = os.getenv("SOURCE_USER")
    sourcePort      = os.getenv("SOURCE_PORT")
    sourcePassword  = os.getenv("SOURCE_PASSWORD")
    targetElastic   = os.getenv("TARGET_ELASTIC")
    targetPort      = os.getenv("TARGET_PORT")
    targetUser      = os.getenv("TARGE_TUSER")
    targetPassword  = os.getenv("TARGET_PASSWORD")

    connDict = {
        "sourceElastic":   sourceElastic,
        "sourceUser":      sourceUser,
        "sourcePort":      sourcePort,
        "sourcePassword":  sourcePassword,
        "targetElastic":   targetElastic,
        "targetPort":      targetPort,
        "targetUser":      targetUser,
        "targetPassword":  targetPassword
    }

    return connDict

def init_logger():

    myLogger = logging.getLogger(__name__)
    c_handler = logging.StreamHandler(stream=sys.stdout)
    c_handler.setLevel(logging.INFO)
    c_format = logging.Formatter('%(asctime)s - %(message)s')
    c_handler.setFormatter(c_format)
    myLogger.addHandler(c_handler)
    # yuri
    return myLogger

def delete_history_indexes(targetES, start, end, logger):

    while start <= end:

        indexExists = False
        index_name = INDEX_PREFIX + str(start)
        if is_index_exists(targetES, index_name, logger):
            logger.info(f'dropping index if exists = {index_name}')
            drop_index(targetES, index_name, logger)
            logger.info('index dropped')
        start = start + timedelta(days=1)  # increase day one by one


def calculate_mttr_job(sourceES, targetES, job_name, source_index_name, query, start, end, logger):
    dtypes = np.dtype([

          ('job_name', object),
          ('result', object),
          ('timestamp', datetime),
          ('recovery_time', float),
          ('incidents', int)
          ])

    data = np.empty(0, dtype=dtypes)
    df_mttr = pd.DataFrame(data)
    
    while start <= end:

        target_index_name = INDEX_PREFIX + str(start)
        #logger.warning(f'Collecting data for job name {job_name}, start = {start}, end = {end}, target_index_name = {target_index_name}')

        indexExists = False
        if is_index_exists(targetES, target_index_name, logger):

            indexExists = True

        raw_df = pd.DataFrame
        grouped_df = pd.DataFrame
        if indexExists: 

            logger.warning(f'index {target_index_name} exists, fetch data from target')  
            grouped_df = get_target_index_data(targetES,GET_TARGET_INDEX_DATA, target_index_name, job_name, logger)

        else: #if index is not exists then calculate the mean daily data on the source (production) Elasticsearch  

            population = []
            population = get_population(sourceES, source_index_name, start, job_name, query, logger)    

            if not population.empty:                                                                         
                raw_df = create_raw_dataframe(population, logger)
                logger.warning(f"population for job_name {job_name} on {str(start)}")
                logger.warning(population)
            
            if not raw_df.empty:                   
                grouped_df = create_grouped_dataframe(raw_df,start)  
                logger.warning(f"raw_df for job_name {job_name} on {str(start)}")
                logger.warning(raw_df)           

        if not grouped_df.empty:
            df_mttr = df_mttr.append(grouped_df)  
            logger.warning(f"grouped_df for job_name {job_name} on {str(start)}")
            logger.warning(grouped_df)
                    
        start = start + timedelta(days=1)  # increase day one by one

    if not df_mttr.empty:
        df_mttr['incidents_mean'] = df_mttr['incidents'].rolling(window=30,min_periods=1).sum()
        df_mttr['recovery_mean_time'] = df_mttr['recovery_time'].rolling(window=DAYS_BACK_TO_CALCULATE,min_periods=1).sum()/df_mttr['incidents_mean']
        #df_mttr_rolling = df_mttr_rolling.append(df_mttr)
        df_mttr["index_name"] = source_index_name
        logger.warning(f"df_mttr for job_name {job_name} on all dates")
        logger.warning(df_mttr)
    
    return df_mttr


def main():

    load_dotenv()
    logger = init_logger()    
    targetESConnectionString = os.getenv("ELASTIC_CONNECTIONSTRING")
    targetES = Elasticsearch(targetESConnectionString)

    sourceES = targetES

    # force re-calculate the last day 
    create_force = True
    
    # force re-calculate the last 30 days 
    create_force_history = False

    logger.warning(f'create_force = {create_force}, create_force_history = {create_force_history}')

    today = date.today()
    
    # do not calculate the current date cause its not over yet and new results may arrived
    end = (today-timedelta(days=1))

    target_index_name = INDEX_PREFIX + str(end)

    if create_force:

        logger.warning(f'droping index if exists = {target_index_name}')
        drop_index(targetES, target_index_name, logger)

    if is_index_exists(targetES, target_index_name, logger):

        logger.warning(f'last day already calculated for index = {target_index_name}.....exit ')
        return

    start = (today - timedelta(days=DAYS_BACK_TO_CALCULATE))


    if create_force_history:
        delete_history_indexes(targetES, start, end, logger)

    jobs_name = get_all_jobs(sourceES, start, end, logger)
    logger.warning(jobs_name)

    dtypes_rolling = np.dtype([

          ('job_name', object),
          ('result', object),
          ('timestamp', datetime),
          ('recovery_time', float),
          ('incidents', int),
          ('recovery_mean_time',float),
          ('incidents_mean',int),
          ('index_name',object)
          ])

    data = np.empty(0, dtype=dtypes_rolling)
    df_mttr_rolling = pd.DataFrame(data)

    for index, row in jobs_name.iterrows():

        job_name = row['job_name']
        query_name =row['query_name']
        source_index_name = row['index_name']
        logger.info(query_name)
        query = []
        query = globals()[query_name]

        start = (today - timedelta(days=DAYS_BACK_TO_CALCULATE))

        # do not calculate the current date cause its not over yet and new results may arrived
        end = (today-timedelta(days=1))

        df_mttr = calculate_mttr_job(sourceES, targetES, job_name, source_index_name, query, start, end, logger)

        if not df_mttr.empty:            
            df_mttr_rolling = df_mttr_rolling.append(df_mttr)
            
    if not df_mttr_rolling.empty: 
        logger.warning(f"df_mttr_rolling for all jobs on all dates")
        logger.warning(df_mttr_rolling)

    #iterate over all dates to calculate rolling average(average over last n days)

    start = (today - timedelta(days = DAYS_BACK_TO_CALCULATE))

    while start <= end:

        df = pd.DataFrame
        documents = []
        # target_index_name mttr-<date> contains all jobs data 
        target_index_name = INDEX_PREFIX + str(start)

        if is_index_exists(targetES, target_index_name, logger):
            logger.warning(f'index {target_index_name} already exists, no need to create it......')
        else:
            df = df_mttr_rolling[pd.to_datetime(df_mttr_rolling.timestamp) == str(start)]           
            documents = df.to_dict(orient='records')
            # create the index even if its stays empty so spend time repeating the same calculation the next day
            create_index(targetES, target_index_name) 

            if len(documents) > 0:

                logger.warning(f'Actual data about to get inserted to {target_index_name} for date {start}')
                logger.warning(documents)

                try:
                    helpers.bulk(targetES, documents, index=target_index_name,doc_type='_doc', raise_on_error=True)
                except Exception:
                    logger.exception(f("Exception occurred target_index_name = {target_index_name}"))
                    raise

        start = start + timedelta(days=1)  # increase day one by one  

if __name__ == "__main__":
    main()