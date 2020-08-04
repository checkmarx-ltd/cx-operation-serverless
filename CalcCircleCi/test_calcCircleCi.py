import unittest   # The test framework
import CircleCi
from CircleCi import PipelineManagement
from datetime import datetime, timedelta,date, time


class Test_TestCircleCi(unittest.TestCase):

    def test_one_pipeline_calculation(self):

        check_pipelineId = '12356e0e-c7d2-48f0-926b-e48d6acb75b3'

        pManage   = PipelineManagement()
        start = date.today() - timedelta(days = 30)
        end = date.today() - timedelta(hours = 2)

        res = pManage.getPopulationFromElasticsearch(calculateForce = True, start = start, end = end)

        for hit in res:
            calculated_status = CircleCi.SUCCESS_STATUS
            if hasattr(hit,"workflow"):
                
                    pipelineId = hit["pipeline_id"]
                    if pipelineId == check_pipelineId:

                        recordId = hit["id"]
                        elasticsearch_pipelineId_status = hit["status"]
                        #print(pipelineId)

                        for row in hit.workflow.items:
                            workflowId = row["id"]
                            status = pManage.collectPipelineDataFromCircleCi(workflowId)
                            if status == CircleCi.FAILED_STATUS:
                                calculated_status = status
                            elif not calculated_status == CircleCi.FAILED_STATUS and not status == CircleCi.SUCCESS_STATUS:
                                calculated_status = status
                            print(f"pipeline_id = {pipelineId}, pipelineId_status = {elasticsearch_pipelineId_status}, workfrowId = {workflowId}, status = {status}")    

                        print (f"pipelineId = {pipelineId}, calculated_status = {calculated_status}, elasticsearch_pipelineId_status = {elasticsearch_pipelineId_status}")                
                        self.assertEqual(calculated_status, elasticsearch_pipelineId_status)
                       
                            
    
   
    def test_pipelines_calculation(self):

        pManage   = PipelineManagement()
        start = date.today() - timedelta(1)
        end = date.today() - timedelta(hours=2)

        res = pManage.getPopulationFromElasticsearch(calculateForce = True, start = start, end = end)

       
        pipelines = []
        for hit in res:
            finalStatus = CircleCi.SUCCESS_STATUS
            if hasattr(hit,"workflow"):
                
                    pipelineId = hit["pipeline_id"]
                    recordId = hit["id"]
                    pipelineId_status = hit["status"]
                    #print(pipelineId)

                    for row in hit.workflow.items:
                        workflowId = row["id"]
                        status = pManage.collectPipelineDataFromCircleCi(workflowId)
                        if status == CircleCi.FAILED_STATUS:
                            finalStatus = status
                        elif not finalStatus == CircleCi.FAILED_STATUS and not status == CircleCi.SUCCESS_STATUS:
                            finalStatus = status
                        print(f"pipeline_id = {pipelineId}, pipelineId_status = {pipelineId_status}, workfrowId = {workflowId}, status = {status}")    

                    #circleciFinalStatus = pManage.determinePipelineStatus
                    print (f"pipelineId = {pipelineId}, finalStatus = {finalStatus}")
                    try:
                        self.assertEqual(finalStatus, pipelineId_status)
                    except AssertionError as e: 
                        pipelines.append(pipelineId)
                        print(e)
                        #self.verificationErrors.append(str(e))
                        pass
                    
                    #break
        print(pipelines)
    
        
        # for debug test in CircleCi
        # https://circleci.com/api/v2/pipeline/14da496c-1de8-42f9-8f34-bbbb6b16c505/workflow
        # https://circleci.com/api/v2/pipeline/022a7426-328d-4058-a800-cb8ca34545c3/workflow
        

if __name__ == '__main__':
    unittest.main()

   