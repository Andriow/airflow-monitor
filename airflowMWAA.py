import os
import sys
import boto3
import argparse
from datetime import datetime
from airflow import AirflowMonitor

# reference: https://docs.aws.amazon.com/pt_br/mwaa/latest/userguide/access-mwaa-apache-airflow-rest-api.html
class AirflowMWAA(AirflowMonitor):
    def __init__(self, logger: object = None) -> None:
        super().__init__()
        self.className = 'AirflowMWAA'
        self.initializeLogger(logger=logger)
        self.setDefaults()

    def getEnvironmentVariables(self):
        self.region = os.environ.get('AWS_REGION')
        self.env_name = os.environ.get('AWS_AIRFLOW_NAME')
        if 'NULL' in (self.region, self.env_name):
            error = f'variáveis de configuração setadas de forma errada, revisar o Dockerfile.'
            raise ValueError(error)

    def createFirstAuth(self, region_name:str, env_name:str) -> str:
        mwaa = boto3.client('mwaa', region_name=region_name)
        response = mwaa.create_web_login_token(Name=env_name)
        self.baseURL = f'https://{response["WebServerHostname"]}'
        return response["WebToken"]

    def setDefaults(self) -> None:
        self.logger.info('Inicializando variaveis')

        self.getEnvironmentVariables()
        web_token = self.createFirstAuth(region_name=self.region, env_name=self.env_name)
        
        self.setCookiesExpiration()
        login_url = f"{self.baseURL}/aws_mwaa/login"
        login_payload = {"token": web_token} #Este token expira após 60 segundos.
        response = self.executeRequest(
            method='POST',
            url=login_url,
            data=login_payload,
            timeout=50)

        self.headers = None
        self.cookies = response.cookies["session"]
        

if __name__ == "__main__":
    airflow = AirflowMWAA()
    airflow.main(sys.argv)