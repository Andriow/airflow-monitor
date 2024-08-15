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

    

if __name__ == '__main__':
    unittest.main()  # pragma: no cover