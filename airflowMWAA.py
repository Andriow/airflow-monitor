import os
import sys
import boto3
import argparse
from datetime import datetime, timedelta
from airflow import AirflowReq

class AirflowMWAA(AirflowReq):
    def __init__(self, logger: object = None) -> None:
        super().__init__()
        self.className = 'AirflowMWAA'
        self.initializeLogger(logger=logger)
        self.setDefaults()

    # reference: https://docs.aws.amazon.com/pt_br/mwaa/latest/userguide/access-mwaa-apache-airflow-rest-api.html
    def setDefaults(self) -> None:
        self.logger.info('Inicializando variaveis')
        
        region = os.environ.get('AWS_REGION')
        env_name = os.environ.get('AWS_AIRFLOW_NAME')
        if None in (region, env_name):
            error = f'variáveis de configuração setadas de forma errada, revisar o Dockerfile.'
            raise SystemExit(error)

        mwaa = boto3.client('mwaa', region_name=region)
        response = mwaa.create_web_login_token(Name=env_name)

        web_server_host_name = response["WebServerHostname"]
        web_token = response["WebToken"]

        self.baseURL = f'https://{web_server_host_name}'

        login_url = f"{self.baseURL}/aws_mwaa/login"
        #Este token expira após 60 segundos.
        login_payload = {"token": web_token}

        # O token da sessão expira após 12 horas, criando controle para 9 horas por segurança.
        self.cookies_expiration = datetime.now() + timedelta(hours=9)
        response = self.executeRequest(
            method='POST',
            url=login_url,
            data=login_payload,
            timeout=50)

        self.headers = None
        self.cookies = response.cookies["session"]
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Monitoramento de dags com erros no airflow gerenciado pela AWS.')
    parser.add_argument('-d', '--dataFim', type=str, default=datetime.today().strftime('%Y-%m-%d'), 
                        help='Data da última execução a ser verificada. Formato: YYYY-MM-DDD. Default = hoje.')
    parser.add_argument('-q', '--qtdDias', type=int, default=90, 
                        help='Quantidade de dias antes da data de fim a ser considerado para a análise. Default = 90')
    parser.add_argument('-p', '--prefix', type=str, default=None, 
                        help='Prefixo que a DAG deverá ter no nome para entrar na análise.')
    parser.add_argument('-s', '--suffix', type=str, default=None, 
                        help='Sufixo que a DAG deverá ter no nome para entrar na análise.')
    args = parser.parse_args()
    try:
        date_format = '%Y-%m-%d'
        dataFim = datetime.strptime(args.dataFim, date_format)
    except:
        print(f'data em formato inválido: {args.dataFim}, formato esperado: YYYY-MM-DD')
        sys.exit(1)
    airflow = AirflowMWAA()
    airflow.run(end_date=dataFim,
                qtdDias=args.qtdDias,
                prefix=args.prefix,
                suffix=args.suffix)
