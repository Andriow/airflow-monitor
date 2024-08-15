import os
import logging
import unittest
from datetime import datetime
from airflow import AirflowReq

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
        self.airflow = AirflowReq(logger = l)

    def testLogger01(self):
        airflow = AirflowReq()
        logger = airflow.logger
        self.assertEqual(logger.name, 'AirflowReq')
    
    def testLogger02(self):
        logger = self.airflow.logger
        self.assertEqual(logger.name, self.className)

    def testSetDefaults01(self):
        headers = self.airflow.headers
        cookies = self.airflow.cookies
        cookies_expiration = self.airflow.cookies_expiration
        self.assertTrue('Content-Type' in headers)
        self.assertTrue('Authorization' in headers)
        self.assertIsNone(cookies)
        self.assertGreater(cookies_expiration, datetime.now())

    def testSetDefaults02(self):
        del os.environ['AIRFLOW_URL']
        error = f'variáveis de configuração setadas de forma errada, revisar o Dockerfile.'
        with self.assertRaises(ValueError) as ctx:
            self.airflow.setDefaults()
        self.assertEqual(error, str(ctx.exception))

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


if __name__ == '__main__':
    unittest.main()  # pragma: no cover