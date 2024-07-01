import json
import logging
import requests
from base64 import b64encode
from datetime import datetime, timedelta

class AirflowReq(object):

    def __init__(self, logger: object = None) -> None:
        super().__init__()
        self.className = 'AirflowReq'
        self.initializeLogger(logger=logger)
        self.setDefaults()

    def initializeLogger(self, logger: object = None) -> logging.Logger:
        if logger == None:
            self.logger = logging.getLogger(self.className)
            self.logger.setLevel(logging.INFO)
            ch = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            ch.setFormatter(formatter)
            ch.addFilter(logging.Filter(self.className))
            self.logger.addHandler(ch)
        else:
            self.logger = logger

    def setDefaults(self) -> None:
        self.logger.info('Inicializando variaveis')
        self.baseURL = "http://YOUR_AIRFLOW_URL"
        airflow_username= "YOUR AIRFLOW USER"
        airflow_password= "YOUR AIRFLOW PASSWORD"
        base64_bytes = b64encode((f"{airflow_username}:{airflow_password}").encode("ascii")).decode("ascii")
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization' : f'Basic {base64_bytes}'
        }

    def extractIdsFromResponse(self, response:dict) -> list:
        ids=[x['dag_id'] for x in response['dags']]
        return ids

    def listAllActiveDags(self) -> list:
        self.logger.info('Listando todas as dags ativas')
        limit_per_itr = 100
        url = f'{self.baseURL}/api/v1/dags?only_active=true&limit={limit_per_itr}'
        DAG_response = requests.request("GET", url=url, headers=self.headers)
        total_entries = DAG_response.json()['total_entries']
        self.logger.debug(f'total_entries: {total_entries}')
        dag_ids = self.extractIdsFromResponse(DAG_response.json())
        number_of_itr = (total_entries // limit_per_itr)
        for next_itr in range(number_of_itr):
            num_of_record = len(dag_ids)
            new_airflow_url = f"{url}&offset={num_of_record}"
            response = requests.get(new_airflow_url, headers=self.headers)
            ids = self.extractIdsFromResponse(response.json())
            dag_ids = dag_ids + ids
        self.logger.debug(f'dag_ids: {dag_ids}')
        return dag_ids
    
    def filterDagsByPrefixSufix(self, dag_ids:list, prefix:str=None, sufix:str=None) -> list:
        self.logger.info(f'Filtering dags with prefix {prefix} and sufix: {sufix}')
        prefix = prefix.lower()
        sufix = sufix.lower()
        retList =[]
        for x in dag_ids:
            if (prefix == None) and (x.lower().endswith(sufix)): # filtrar apenas pelo sufixo
                retList.append(x)
            elif (sufix == None) and (x.lower().startswith(prefix)): # filtrar apenas pelo prefixo
                retList.append(x)
            else:
                if (x.lower().startswith(prefix) and x.lower().endswith(sufix)): # filtrar por ambos
                    retList.append(x)
        self.logger.debug(f'filtered dag_ids: {retList}')
        return retList

    def timeFormat(self, time:datetime) -> str:
        return time.strftime('%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
    
    def getAllExecutionsByDagId(self, dag_id:str) -> list:
        self.logger.info(f'Consultando a dag: {dag_id}')
        url = f'{self.baseURL}/api/v1/dags/~/dagRuns/list'
        end_date = datetime.today()
        start_date = (datetime.now() - timedelta(90))
        self.logger.debug(f'Consultando de {start_date} ate {end_date}')
        payload = json.dumps({
            'dag_ids': [dag_id],
            'start_date_gte': self.timeFormat(start_date),
            'end_date_lte': self.timeFormat(end_date),
        })
        DAG_response = requests.request("POST", url, headers=self.headers, data=payload)
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

    def run(self):
        result_list = []
        active_dags = self.listAllActiveDags()
        active_dags = self.filterDagsByPrefixSufix(active_dags, 'DL', 'prd')
        for dag in active_dags:
            list = self.getAllExecutionsByDagId(dag_id=dag)
            analyse = self.analyseDagRuns(dag_id=dag, run_list=list)
            result_list.append(analyse)
        consolidate = self.consolidateResults(result_list=result_list)
        self.logger.info(f'resultado final: {consolidate}')

if __name__ == "__main__":
    airflow = AirflowReq()
    airflow.run()
