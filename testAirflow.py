import os
import shlex
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

    def testCleanArgs(self):
        arg_list = ['a.py', 'b', 'c']
        to_test = ['b', 'c']
        lst = self.airflow.cleanArgs(arg_list=arg_list)
        self.assertEqual(lst, to_test)
        lst = self.airflow.cleanArgs(arg_list=lst)
        self.assertEqual(lst, to_test)

    def testParseArgs01(self):
        command = ''
        args = self.airflow.parseArgs(shlex.split(command))
        self.assertEqual(args.dataFim, datetime.today().strftime('%Y-%m-%d'))
        self.assertEqual(args.qtdDias, 90)
        self.assertIsNone(args.prefix)
        self.assertIsNone(args.suffix)
        self.assertFalse(args.verbose)

    def testParseArgs02(self):
        command = '-d 2024-03-15 -q 10'
        args = self.airflow.parseArgs(shlex.split(command))
        self.assertEqual(args.dataFim, '2024-03-15')
        self.assertEqual(args.qtdDias, 10)
        self.assertIsNone(args.prefix)
        self.assertIsNone(args.suffix)
        self.assertFalse(args.verbose)
    
    def testParseArgs03(self):
        command = '-p DL'
        args = self.airflow.parseArgs(shlex.split(command))
        self.assertEqual(args.dataFim, datetime.today().strftime('%Y-%m-%d'))
        self.assertEqual(args.qtdDias, 90)
        self.assertEqual(args.prefix, 'DL')
        self.assertIsNone(args.suffix)
        self.assertFalse(args.verbose)

    def testParseArgs04(self):
        command = '-s prd'
        args = self.airflow.parseArgs(shlex.split(command))
        self.assertEqual(args.dataFim, datetime.today().strftime('%Y-%m-%d'))
        self.assertEqual(args.qtdDias, 90)
        self.assertIsNone(args.prefix)
        self.assertEqual(args.suffix, 'prd')
        self.assertFalse(args.verbose)

    def testParseArgs05(self):
        command = '-d 2024-03-15 -q 10 -q 10 -p DL -s prd'
        args = self.airflow.parseArgs(shlex.split(command))
        self.assertEqual(args.dataFim, '2024-03-15')
        self.assertEqual(args.qtdDias, 10)
        self.assertEqual(args.prefix, 'DL')
        self.assertEqual(args.suffix, 'prd')
        self.assertFalse(args.verbose)

    def testParseArgs06(self):
        command = '-v'
        args = self.airflow.parseArgs(shlex.split(command))
        self.assertEqual(args.dataFim, datetime.today().strftime('%Y-%m-%d'))
        self.assertEqual(args.qtdDias, 90)
        self.assertIsNone(args.prefix)
        self.assertIsNone(args.suffix)
        self.assertTrue(args.verbose)

    def testMainInvalidDate(self):
        command = '-d 2024-08 -q 10 -q 10 -p DL -s prd'
        error = f'data em formato inválido: 2024-08, formato esperado: YYYY-MM-DD'
        with self.assertRaises(ValueError) as ctx:
            self.airflow.main(shlex.split(command))
        self.assertEqual(error, str(ctx.exception))

    def testMainInvalidDateAndVerbose(self):
        command = '-d 2024-08 -q 10 -q 10 -p DL -s prd -v'
        error = f'data em formato inválido: 2024-08, formato esperado: YYYY-MM-DD'
        with self.assertRaises(ValueError) as ctx:
            self.airflow.main(shlex.split(command))
        self.assertEqual(logging.DEBUG, self.airflow.logger.level)
        self.assertEqual(error, str(ctx.exception))

    def testLoggerEmptyLogger(self):
        airflow = AirflowMonitor()
        logger = airflow.logger
        self.assertEqual(logger.name, 'AirflowMonitor')
    
    def testLoggerExistingLogger(self):
        logger = self.airflow.logger
        self.assertEqual(logger.name, self.className)

    def testLoggerExistingLoggerAndChangeLevel(self):
        logger = self.airflow.logger
        self.airflow.initializeLogger(logger=logger, level=logging.DEBUG)
        self.assertEqual(logging.DEBUG, self.airflow.logger.level)

    def testSetDefaults(self):
        headers = self.airflow.headers
        cookies = self.airflow.cookies
        self.assertTrue('Content-Type' in headers)
        self.assertTrue('Authorization' in headers)
        self.assertIsNone(cookies)

    def testExecuteRequest(self):
        url = f'http://www.google.com/nothere'
        
        error = 'Erro ao chamar a URL'
        with self.assertRaises(SystemExit) as ctx:
            self.airflow.executeRequest('GET', url)
        self.assertTrue(error in str(ctx.exception))

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