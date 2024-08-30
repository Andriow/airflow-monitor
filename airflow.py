import os
import sys
import json
import logging
import argparse
import requests
from base64 import b64encode
from datetime import datetime, timedelta

class AirflowMonitor(object):

    def __init__(self, logger: object = None) -> None:
        super().__init__()
        self.className = 'AirflowMonitor'
        self.initializeLogger(logger=logger)
        self.setDefaults()

    def initializeLogger(self, logger: object = None, level: int = logging.INFO) -> logging.Logger:
        if logger == None:
            self.logger = logging.getLogger(self.className)
            self.logger.setLevel(level)
            ch = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s")
            ch.setFormatter(formatter)
            ch.addFilter(logging.Filter(self.className))
            self.logger.addHandler(ch)
        else:
            self.logger = logger

    def setCookiesExpiration(self):
        # O token da sessão expira após 12 horas, criando controle para 9 horas por segurança.
        self.cookies_expiration = datetime.now() + timedelta(hours=9)

    def getEnvironmentVariables(self):
        self.baseURL = os.environ.get('AIRFLOW_URL')
        self.airflow_username = os.environ.get('AIRFLOW_USERNAME')
        self.airflow_password = os.environ.get('AIRFLOW_PASSWORD')
        if 'NULL' in (self.baseURL, self.airflow_username, self.airflow_password):
            error = f'variáveis de configuração setadas de forma errada, revisar o Dockerfile.'
            raise ValueError(error)

    def setDefaults(self) -> None:
        self.logger.info('Inicializando variaveis')
        self.getEnvironmentVariables()
        base64_bytes = b64encode((f"{self.airflow_username}:{self.airflow_password}").encode("ascii")).decode("ascii")
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization' : f'Basic {base64_bytes}'
        }
        self.cookies = None
        self.setCookiesExpiration()

    def executeRequest(self, method:str, url:str, payload:json=None):
        if (datetime.now() >= self.cookies_expiration):
            self.setDefaults() # pragma: no cover

        try:
            response = requests.request(method=method, 
                                        url=url, 
                                        data=payload,
                                        headers=self.headers,
                                        cookies=self.cookies)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            error = f'Erro ao chamar a URL: {url} \n {e}'
            raise SystemExit(error)

        return response

    def extractIdsFromResponse(self, response:dict) -> list:
        ids=[x['dag_id'] for x in response['dags']]
        return ids

    def listAllActiveDags(self) -> list:
        self.logger.info('Listando todas as dags ativas')
        limit_per_itr = 100
        url = f'{self.baseURL}/api/v1/dags?only_active=true&limit={limit_per_itr}'
        DAG_response = self.executeRequest(method='GET', url=url)
        total_entries = DAG_response.json()['total_entries']
        self.logger.debug(f'total_entries: {total_entries}')
        dag_ids = self.extractIdsFromResponse(DAG_response.json())
        number_of_itr = (total_entries // limit_per_itr)
        for i in range(number_of_itr):
            num_of_record = len(dag_ids)
            new_airflow_url = f"{url}&offset={num_of_record}"
            response = self.executeRequest('GET',new_airflow_url)
            ids = self.extractIdsFromResponse(response.json())
            dag_ids = dag_ids + ids
        self.logger.debug(f'dag_ids: {dag_ids}')
        return dag_ids
    
    def filterByPrefix(self, dag_ids:list, prefix:str) -> list:
        retList =[]
        prefix = prefix.lower()
        for x in dag_ids:
            if x.lower().startswith(prefix):
                retList.append(x)
        return retList
    
    def filterBySuffix(self, dag_ids:list, suffix:str) -> list:
        retList =[]
        suffix = suffix.lower()
        for x in dag_ids:
            if x.lower().endswith(suffix):
                retList.append(x)
        return retList
    
    def filterByPrefixAndSuffix(self, dag_ids:list, prefix:str, suffix:str) -> list:
        retList =[]
        prefix = prefix.lower()
        suffix = suffix.lower()
        for x in dag_ids:
            if (x.lower().startswith(prefix) and x.lower().endswith(suffix)):
                retList.append(x)
        return retList
    
    def filterDagsByPrefixSuffix(self, dag_ids:list, prefix:str=None, suffix:str=None) -> list:
        self.logger.info(f'Filtering dags with prefix {prefix} and sufix: {suffix}')
        if prefix is not None and suffix is not None:
            return self.filterByPrefixAndSuffix(dag_ids, prefix, suffix)
        elif prefix is None and suffix is not None:
            return self.filterBySuffix(dag_ids=dag_ids, suffix=suffix)
        elif prefix is not None and suffix is None:
            return self.filterByPrefix(dag_ids=dag_ids, prefix=prefix)
        else:
            return dag_ids

    def timeFormat(self, time:datetime) -> str:
        return time.strftime('%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
    
    def getAllExecutionsByDagId(self, dag_id:str, start_date:datetime, end_date:datetime) -> list:
        self.logger.info(f'Consultando a dag: {dag_id}')
        url = f'{self.baseURL}/api/v1/dags/~/dagRuns/list'
        self.logger.debug(f'Consultando de {start_date} ate {end_date}')
        payload = json.dumps({
            'dag_ids': [dag_id],
            'start_date_gte': self.timeFormat(start_date),
            'end_date_lte': self.timeFormat(end_date),
        })
        DAG_response = self.executeRequest(method='POST', url=url, payload=payload)
        self.logger.debug(DAG_response.json()['dag_runs'])
        return DAG_response.json()['dag_runs']
    
    def analyseDagRuns(self, dag_id:str, run_list:list) -> dict:
        self.logger.info(f'Analizando retorno das execucoes da dag: {dag_id}')
        run_count = 0
        fail_count = 0
        for run in run_list:
            run_count = run_count + 1 
            if run['state'] == 'failed':
                fail_count = fail_count + 1
        ret = {'dag_id': dag_id,
               'run_count': run_count, 
               'fail_count': fail_count}
        self.logger.debug(f'Analise concluida: {ret}')
        return ret
    
    def consolidateResults(self, result_list:list) -> float:
        self.logger.info('Consolidando valores de resultado')
        total_runs = 0
        total_fails = 0
        for i in result_list:
            total_runs = total_runs + i['run_count']
            total_fails = total_fails + i['fail_count']
            self.logger.info(f'{i["dag_id"]} - runs: {i["run_count"]} fails: {i["fail_count"]}')
        consolidate = total_fails / total_runs
        self.logger.info(f'total runs: {total_runs} total fails: {total_fails}')
        return consolidate

    def run(self, end_date:datetime, qtdDias:int, prefix:str=None, suffix:str=None): # pragma: no cover
        result_list = []
        active_dags = self.listAllActiveDags()
        active_dags = self.filterDagsByPrefixSuffix(active_dags, prefix, suffix)
        start_date = (end_date - timedelta(qtdDias))
        self.logger.info(f'Consultando de {start_date} ate {end_date}')
        for dag in active_dags:
            list = self.getAllExecutionsByDagId(dag_id=dag, 
                                                start_date=start_date, 
                                                end_date=end_date)
            analyse = self.analyseDagRuns(dag_id=dag, run_list=list)
            result_list.append(analyse)
        consolidate = self.consolidateResults(result_list=result_list)
        self.logger.info(f'resultado final: {consolidate}')

    def parseArgs(self, arg_list: list[str] | None):
        parser = argparse.ArgumentParser(description='Monitoramento de dags com erros no airflow.')
        parser.add_argument('-d', '--dataFim', type=str, default=datetime.today().strftime('%Y-%m-%d'), 
                            help='Data da última execução a ser verificada. Formato: YYYY-MM-DDD. Default = hoje.')
        parser.add_argument('-q', '--qtdDias', type=int, default=90, 
                            help='Quantidade de dias antes da data de fim a ser considerado para a análise. Default = 90')
        parser.add_argument('-p', '--prefix', type=str, default=None, 
                            help='Prefixo que a DAG deverá ter no nome para entrar na análise.')
        parser.add_argument('-s', '--suffix', type=str, default=None, 
                            help='Sufixo que a DAG deverá ter no nome para entrar na análise.')
        parser.add_argument('-v', '--verbose', action='store_true',
                            help='O nível de verbose por padrão é logging.INFO, quando passado este argumento altera para logging.DEBUG')
        args = parser.parse_args(arg_list)
        return args

    def main(self, arg_list: list[str] | None):
        args = self.parseArgs(arg_list)
        if args.verbose:
            self.initializeLogger(level=logging.DEBUG)
        
        try:
            date_format = '%Y-%m-%d'
            dataFim = datetime.strptime(args.dataFim, date_format)
        except:
            error = f'data em formato inválido: {args.dataFim}, formato esperado: YYYY-MM-DD'
            raise ValueError(error)

        self.run(end_date=dataFim,
                qtdDias=args.qtdDias,
                prefix=args.prefix,
                suffix=args.suffix) # pragma: no cover

if __name__ == "__main__":
    airflow = AirflowMonitor() # pragma: no cover
    #remover o primeiro argumento que sempre será o nome do arquivo executado.
    sys.argv.pop(0)
    airflow.main(sys.argv) # pragma: no cover
