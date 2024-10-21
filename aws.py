import os
import boto3

from pathlib import Path
from configparser import ConfigParser

class AWS(object):
    
    def __init__(self, logger):
        self.logger = logger
        self.credentials = None
        # Obrigatory parameters
        self.region = os.environ.get('AWS_REGION', None)
        self.access_key_id = os.environ.get('AWS_ACCESS_KEY_ID', None)
        self.secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
        # optionals parameters
        self.role_arn = os.environ.get('AWS_ROLE_ARN', None)
        self.role_session_name = os.environ('AWS_ROLE_SESSION_NAME', None)
        
        self.aws_conf = Path(f'{os.environ.get("HOME")}/.aws/credentials')
        
        if self.aws_conf.is_file():
            self._parseConfig()
        else:
            self.logger.info(f"File '{self.aws_conf}' not found. Running on environment variables.")
        
        self._validateCredentials()
    
    def _validateCredentials(self):
        self.logger.info('Validating AWS credentials.')
        self.logger.debug(f'region: {self.region}')
        self.logger.debug(f'access_key_id: {self.access_key_id}')
        self.logger.debug(f'secret_access_key: {self.secret_access_key}')
        if None in [self.region, self.access_key_id, self.secret_access_key]:
            raise ValueError('None found in AWS credentials, review your credentials.')
    
    def _parseConfig(self, profile:str = 'default') -> None:
        self.logger.info(f'parsing file: {self.aws_conf} whith profile: {profile}.')
        parser = ConfigParser()
        parser.read_file(self.aws_conf.open(mode = 'r', encoding = 'utf8'))
        if profile not in parser.sections():
            raise KeyError(f"Key '{profile}' not found on file '{self.aws_conf}'.")
        
        self.access_key_id = parser[profile].get('aws_access_key_id', None)
        self.secret_access_key = parser[profile].get('aws_secret_access_key', None)
        self.region = parser[profile].get('region', None)
        self.role_arn = parser[profile].get('role_arn', None)
        self.role_session_name = parser[profile].get('role_session_name', None)

    def _createSession(self) -> boto3.session.Session:
        self.logger.info(f'creating boto3 session.')
        return boto3.session.Session(
            aws_access_key_id = self.access_key_id,
            aws_secret_access_key = self.secret_access_key,
            region_name = self.region,
            aws_session_token = None
        )

    def _assumeRole(self) -> dict:
        sts_session = self._createSession()
        sts_client = sts_session.client('sts')
        if self.role_arn is not None and self.role_session_name is not None:
            self.logger.info(f'assuming role: {self.role_arn}.')
            sts_response = sts_client.assume_role(
                RoleArn = self.role_arn,
                RoleSessionName = self.role_session_name
            )
        credentials = sts_response['Credentials']
        return credentials

    def createAWSSession(self) -> dict:
        cred = self._assumeRole()
        self.logger.info('creating aws credentials.')
        aws_session = {
            'access_key': cred['AccessKeyId'],
            'secret_key': cred['SecretAccessKey'],
            'session_token': cred['SessionToken'],
            'region': self.region
        }
        return aws_session
    
    def createClient(self, service_name: str) -> boto3.session.Session.client:
        if self.credentials is None:
            self.credentials = self.createAWSSession()
        self.logger.info('creating boto3 client.')
        cliente = boto3.client(
            service_name,
            region_name = self.region,
            aws_access_key_id = self.credentials['access_key'],
            aws_secret_access_key = self.credentials['secret_key'],
            aws_session_token = self.credentials['session_token']
        )
        
        return cliente
    
    def createResource(self, service_name: str) -> boto3.session.Session.resource:
        if self.credentials is None:
            self.credentials = self.createAWSSession()
        self.logger.info('creating boto3 resource.')
        recurso = boto3.resource(
            service_name,
            aws_access_key_id = self.credentials['access_key'],
            aws_secret_access_key = self.credentials['secret_key'],
            aws_session_token = self.credentials['session_token'],
        )
        return recurso
