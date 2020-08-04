from elasticsearch import Elasticsearch
from es.elastic.api import connect
from elasticsearch_dsl import Search, UpdateByQuery
from datetime import datetime, timedelta,date, time
import logging
import json
import requests
import os
from dotenv import load_dotenv

INDEX_NAME      = "circleci*"
SUCCESS_STATUS  = "success"
RUNNING_STATUS  = "running"
FAILED_STATUS   = "failed"


class PipelineManagement:
    """
    This class is the manager of the program holding all the members: Pipeline, Workflows[].
    Each Pipeline object has one or more Workflows as represented in CircleCi.
    """

    class Workflow:
        """
        This class holds CircleCi workflow information (son of pipeline)
        """
        def __init__(self, workflowName, workflowStatus, workflowId):            
            self.workflowId = workflowId #use this id to access circleci
            self.workflowName = workflowName
            self.workflowStatus = workflowStatus

    
    class Pipeline:
        """
        This class holds CircleCi pipeline information
        """
        def __init__(self, pipelineId, recordId, pipelineStatus):
            self.pipelineId = pipelineId 
            self.recordId  = recordId #record id inside Elasticsearch holding this pipeline 
            self.workflows = []
            self.pipelineStatus = pipelineStatus
            self.updateStatusDate = datetime.now()

    @staticmethod
    def initLogger():
       
        myLogger = logging.getLogger(__name__)

        '''
        #f_handler = logging.FileHandler('circleci_status.log', mode='w')
        #f_handler.setLevel(logging.WARNING)
        #f_format = logging.Formatter('%(asctime)s - %(message)s')
        '''

        s_handler=logging.StreamHandler()       
        s_handler.setLevel(logging.INFO)

        # Add handlers to the logger
        myLogger.addHandler(s_handler)
        #f_handler.setFormatter(f_format)

        return myLogger
      
    
    def __init__(self):
        
        load_dotenv()
        ElasticConnectionString = os.getenv("ELASTIC_CONNECTIONSTRING")
        ElasticHost = os.getenv("ELASTIC_HOST")
        ElasticPort = os.getenv("ELASTIC_PORT")
        ElasticUser= os.getenv("ELASTIC_USER")
        ElasticPassword= os.getenv("ELASTIC_PASSWORD")

        self.CIRCLECI_TOKEN= os.getenv("CIRCLECI_TOKEN")

        self.es = Elasticsearch(ElasticConnectionString)
        self.esConn = connect(host=ElasticHost,port=ElasticPort,user=ElasticUser,password=ElasticPassword)
        self.pipelines = []        
        self.logger = PipelineManagement.initLogger()
           
    def getPopulationFromElasticsearch(self, calculateForce, start, end):
        """
        The population contains circle builds from the last days with only status == running 
        """
        try:                       

            self.logger.warning(f'Get population between {start} and {end} for running pipelines.')

            if calculateForce:
                s = Search(using=self.es, index=INDEX_NAME).filter("range",created_at={'gte': start, 'lte': end}).query()
            else:
                s = Search(using=self.es, index=INDEX_NAME).filter("range",created_at={'gte': start, 'lte': end}).query("match", status = RUNNING_STATUS)
            
            '''
            # another way to get response
            body = { "query": {
                            "bool": {
                            "must": [
                                        {
                                            "term": {
                                                "status": "running"
                                            }
                                        },
                                        {
                                            "range": {
                                                "created_at": {
                                                "from": start,
                                                "to": end
                                                }
                                            }
                                        }
                                    ]
                            }
                        }
            }
            '''
            #response = self.es.search(index=INDEX_NAME, body=body, size=9999)

            
            response = s[0:9999].execute()
            self.logger.warning(f"number of hits found = {response.hits.total.value}")
            return response
        except Exception as e:
            self.logger.exception("Exception occurred")

    def parseCollectePiplinesDataFromElastic(self, response):
        """
        The raw data in elasticsearch holds only the first snapshot taken from CircleCi, meaning the status of the workflows are running.
        This class holds the data seen on elasticsearch before the program finishes
        """

        try:
            for hit in response:
                if hasattr(hit,"workflow"):
                
                    pipelineId = hit["pipeline_id"]
                    recordId = hit["id"]
                    status = hit["status"]

                    self.logger.warning(f"pipelineId = {pipelineId}, status = {status}")

                    pipeline = PipelineManagement.Pipeline(pipelineId = pipelineId,recordId = recordId, pipelineStatus = status)

                    for row in hit.workflow.items:
                        workflow = PipelineManagement.Workflow(workflowId = row["id"], workflowName = row["name"],workflowStatus = row["status"])
                        pipeline.workflows.append(workflow)
                    
                    self.pipelines.append(pipeline)         
            
            return self.pipelines

        except Exception as e:
            self.logger.exception(f"Exception occurred with pipelineId = {pipelineId}, recordId = {recordId}, status = {status}, pipelines object = {pipelines}")


    def collectPipelineDataFromCircleCi(self, workflowId):

        r = requests.get(f'https://circleci.com/api/v2/workflow/{workflowId}', auth=(self.CIRCLECI_TOKEN, ''))
        _json = json.loads(r.text)
        status = _json["status"]
        return status


    def updatePiplinesDataFromCircleCi(self, pipelines):
        """
        After collecting the data from elasticsearch, we need to know the exact status of the workflow cause elasticsearch holds only the initialize status which is running.
        E.g WorkflowId 1234 status is running on elasticsearch so we need to get the exact status of WorklowId 1234 from CircleCi.
        This class also holds the token to access CircleCi
        """
        try:
            if pipelines == None:
                return
            for pipeline in pipelines:
                for workflow in pipeline.workflows:
                    workflowId = workflow.workflowId
                    workflow.workflowStatus = self.collectPipelineDataFromCircleCi(workflowId)                    
                    workflowId = None                
            return pipelines

        except Exception as e:
            self.logger.exception(f"Exception occurred with workflowId = {workflowId}")
            raise

    
    def  determinePipelineStatus(self, pipeline):
        '''
        determine ONE pipline status
        '''
        pipeline.pipelineStatus = SUCCESS_STATUS
        for workflow in pipeline.workflows:
            if workflow.workflowStatus == FAILED_STATUS:
                pipeline.pipelineStatus = FAILED_STATUS
            elif pipeline.pipelineStatus != FAILED_STATUS and workflow.workflowStatus != SUCCESS_STATUS:
                pipeline.pipelineStatus = workflow.workflowStatus 

        return pipeline.pipelineStatus

    def determinePipelinesStatus(self, pipelines):
        """
        A Pipeline consider failed if one of its worklows fails.
        If none of the workflows failed, it consider running unless every one of the workflow ended successfuly and then also the piple considered ended successfuly.
        """
        try:
            if pipelines == None:
                return
            
            for pipeline in pipelines:
                self.determinePipelineStatus(pipeline)                   

            return pipelines

        except Exception as e:
            self.logger.exception(f"Exception occurred")

    def printPipelines(self, pipelines):
        """
        Print the pipelines and their worklows
        """
        try:
            if pipelines == None:
                return        
            for pipeline in pipelines:
                recordId = pipeline.recordId
                for row in pipeline.workflows:
                    print(f'recordId = {recordId},pipelineStatus = {pipeline.pipelineStatus} pipeline_id = {pipeline.pipelineId}, workflow =  {row.workflowName}, status = {row.workflowStatus}, workflowId = {row.workflowId} ')   
        except Exception as e:
            self.logger.exception(f"Exception occurred")

    def updateElasticWithCircleCiStatus(self, pipelines):
        """
        Update elasticsearch workflows status with the final status found on CircleCi
        """
        
        if pipelines == None:
            return 
        for pipeline in pipelines:
            recordId = pipeline.recordId
            status = pipeline.pipelineStatus
            updateTime = pipeline.updateStatusDate
            pipelineId = pipeline.pipelineId

            '''
            # without params - exceed thrteshold and throws error
            ubq = UpdateByQuery(using=self.es, index=INDEX_NAME) \
                .query("match", id = recordId)   \
                .script(source=f"ctx._source.status = '{status}'; ctx._source.updated_status_at = '{updateTime}'", lang="painless")
            '''

            ubq = UpdateByQuery(using=self.es, index=INDEX_NAME) \
                .query("match", id = recordId)   \
                .script(source=f"ctx._source.status = params.status; ctx._source.updated_status_at = params.updateTime", 
                        params={
                            "status": status,
                            "updateTime": updateTime
                        }                    
                )

            self.logger.warning(f"ctx._source.id = {recordId}, ctx._source.status = '{status}'; ctx._source.updated_status_at = '{updateTime}'")
            try:
                ubq.execute()
            except Exception as e:
                self.logger.warning(f"Exception occurred {e} on: ctx._source.pipeline_id = {pipelineId}, ctx._source.id = {recordId}, ctx._source.status = '{status}'; ctx._source.updated_status_at = '{updateTime}'")            
                pass
              
    @staticmethod
    def main():

        pManage   = PipelineManagement()

        start = date.today() - timedelta(days = 2)
        end = date.today() - timedelta(hours=2)

        
        #for debug
        #start = date(2020, 7, 21)
        #end = date(2020, 7, 21)
        
        '''
        retun raw data, hits,  from elasticsearch as seen in the circleci index 
        '''
        rawDataFromElastic  = pManage.getPopulationFromElasticsearch(False, start, end)

        '''
        map the results from previous function to Python Classes
        every Pipeline class has array of Workflow classes
        return Python array of Pipeline objects
        '''
        pipelines = pManage.parseCollectePiplinesDataFromElastic(rawDataFromElastic)  

        pipelines = pManage.updatePiplinesDataFromCircleCi(pipelines) 

        pManage.printPipelines(pipelines) 
        pipelines = pManage.determinePipelinesStatus(pipelines)
        pManage.updateElasticWithCircleCiStatus(pipelines)

if __name__ == "__main__":
    PipelineManagement.main()
 
















#curl -u '8895d65398888c8e859e4e8851e73a1fde420de6:' -X GET https://circleci.com/api/v2/workflow/96834933-95b0-48de-a628-61cf6b975a89 | jq

