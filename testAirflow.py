import os
import logging
import unittest
from datetime import datetime
from airflow import AirflowMonitor

class TestAirflow(unittest.TestCase):
    def setUp(self):
        os.environ['AWS_REGION'] = "region"
        os.environ['AWS_ACCESS_KEY_ID'] = "KEY_ID"
        os.environ['AWS_SECRET_ACCESS_KEY' ]= "ACCESS_KEY"
        os.environ['AWS_AIRFLOW_NAME'] = "airflow-name"
        os.environ['AIRFLOW_URL'] = 'URL'
        os.environ['AIRFLOW_USERNAME'] = 'AIRFLOW_USERNAME'
        os.environ['AIRFLOW_PASSWORD'] = 'password'

        self.className = 'AirflowTest'
        l = logging.getLogger(self.className)
        l.setLevel(logging.ERROR)
        self.airflow = AirflowMonitor(logger = l)

    def testLogger01(self):
        airflow = AirflowMonitor()
        logger = airflow.logger
        self.assertEqual(logger.name, 'AirflowMonitor')
    
    def testLogger02(self):
        logger = self.airflow.logger
        self.assertEqual(logger.name, self.className)

    def testSetDefaults(self):
        headers = self.airflow.headers
        cookies = self.airflow.cookies
        self.assertTrue('Content-Type' in headers)
        self.assertTrue('Authorization' in headers)
        self.assertIsNone(cookies)

    def testGetEnvironmentVariables(self):
        os.environ['AIRFLOW_URL'] = 'NULL'
        error = f'variáveis de configuração setadas de forma errada, revisar o Dockerfile.'
        with self.assertRaises(ValueError) as ctx:
            self.airflow.getEnvironmentVariables()
        self.assertEqual(error, str(ctx.exception))

    def testSetCookiesExpiration(self):
        cookies_expiration = self.airflow.cookies_expiration
        self.assertGreater(cookies_expiration, datetime.now())

    def testExtractIdsFromResponse(self):
        dict = {}
        dict['dags'] = [{'dag_id':1}, {'dag_id':2}]
        ret = self.airflow.extractIdsFromResponse(response=dict)
        self.assertEqual([1,2], ret)

    def testFilterByPrefix(self):
        dag_ids = []
        dag_ids.append('DL_test_prd')
        dag_ids.append('dl_test_PRD')
        dag_ids.append('bi_test_prd')
        ret = self.airflow.filterByPrefix(dag_ids=dag_ids, prefix='dl')
        self.assertEqual(len(ret), 2)

    def testFilterBySuffix(self):
        dag_ids = []
        dag_ids.append('DL_test_prd')
        dag_ids.append('dl_test_PRD')
        dag_ids.append('bi_test_pRd')
        dag_ids.append('bi_test_qas')
        ret = self.airflow.filterBySuffix(dag_ids=dag_ids, suffix='prd')
        self.assertEqual(len(ret), 3)

    def testFilterByPrefixAndSuffix(self):
        dag_ids = []
        dag_ids.append('DL_test_prd')
        dag_ids.append('dl_test_PRD')
        dag_ids.append('bi_test_pRd')
        dag_ids.append('bi_test_qas')
        ret = self.airflow.filterByPrefixAndSuffix(dag_ids=dag_ids, prefix='DL', suffix='prd')
        self.assertEqual(len(ret), 2)

    def testFilterDagsByPrefixSuffix(self):
        dag_ids = []
        dag_ids.append('DL_test_prd')
        dag_ids.append('dl_test_PRD')
        dag_ids.append('bi_test_pRd')
        dag_ids.append('bi_test_qas')
        ret = self.airflow.filterDagsByPrefixSuffix(dag_ids=dag_ids, prefix='Dl')
        self.assertEqual(len(ret), 2)
        ret = self.airflow.filterDagsByPrefixSuffix(dag_ids=dag_ids, suffix='QAS')
        self.assertEqual(len(ret), 1)
        ret = self.airflow.filterDagsByPrefixSuffix(dag_ids=dag_ids, prefix='BI', suffix='prd')
        self.assertEqual(len(ret), 1)
        ret = self.airflow.filterDagsByPrefixSuffix(dag_ids=dag_ids)
        self.assertEqual(len(ret), 4)

    def testTimeFormat(self):
        date = datetime.strptime('2024-08-15_12:30:30', "%Y-%m-%d_%H:%M:%S")
        ret = self.airflow.timeFormat(date)
        self.assertEqual(ret, '2024-08-15T12:30:30Z')

    def testAnalyseDagRuns(self):
        dag_id = 'test'
        lst = []
        lst.append({'state':'success'})
        lst.append({'state':'success'})
        lst.append({'state':'failed'})
        lst.append({'state':'success'})
        lst.append({'state':'success'})
        lst.append({'state':'success'})
        lst.append({'state':'failed'})
        lst.append({'state':'success'})
        lst.append({'state':'failed'})
        lst.append({'state':'success'})
        ret = self.airflow.analyseDagRuns(dag_id=dag_id, run_list=lst)
        self.assertEqual(ret['dag_id'], dag_id)
        self.assertEqual(ret['run_count'], 10)
        self.assertEqual(ret['fail_count'], 3)

    def testConsolidateResults(self):
        lst = []
        lst.append({'dag_id': '1', 'run_count': 10, 'fail_count':3})
        lst.append({'dag_id': '2', 'run_count': 10, 'fail_count':2})
        lst.append({'dag_id': '3', 'run_count': 10, 'fail_count':1})
        ret = self.airflow.consolidateResults(result_list=lst)
        self.assertAlmostEqual(ret, 0.2)

if __name__ == '__main__':
    unittest.main()  # pragma: no cover