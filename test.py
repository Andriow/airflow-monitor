import logging
import unittest
from airflow import AirflowReq

class Test(unittest.TestCase):
    def testLoggerNew(self):
        airflow = AirflowReq()
        logger = airflow.logger
        self.assertEqual(logger.name, 'AirflowReq')
    
    def testLoggerNew(self):
        l = logging.getLogger("test")
        airflow = AirflowReq(logger = l)
        logger = airflow.logger
        self.assertEqual(logger.name, 'test')

if __name__ == '__main__':
    unittest.main()  # pragma: no cover