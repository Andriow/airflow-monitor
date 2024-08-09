# airflow-monitor
Projeto para monitoramento do airflow

# Como compilar
Para compilar o projeto é preciso alterar o Dockerfile com as credenciais necessárias.

Caso você queira executar no ambiente MWAA não é necessário preencher as informações de  URL, username e password, pois o MWAA realiza o login utilizando o boto3.

Caso você queira executar em um ambiente local de airflow, será necessário indicar a URL o username e a senha para que a API possa autenticar, e não será necessário indicar as credenciais de AWS.

Para dar o build no container deverá executar o comando:

```sh
docker build -t airflow-monitor:latest -f Dockerfile .
```

# Como executar
O primeiro passo será de entrar no Container compilado:

```sh
docker run --rm -it --entrypoint bash airflow-monitor:latest
```

Uma vez no container poderá ser executado o script com seus parâmetros:

```sh
python3 airflow.py --help
usage: airflow.py [-h] [-d DATAFIM] [-q QTDDIAS] [-p PREFIX] [-s SUFFIX]

Monitoramento de dags com erros no airflow.

options:
  -h, --help            show this help message and exit
  -d DATAFIM, --dataFim DATAFIM
                        Data da última execução a ser verificada. Formato: YYYY-MM-DDD. Default = hoje.
  -q QTDDIAS, --qtdDias QTDDIAS
                        Quantidade de dias antes da data de fim a ser considerado para a análise. Default = 90
  -p PREFIX, --prefix PREFIX
                        Prefixo que a DAG deverá ter no nome para entrar na análise.
  -s SUFFIX, --suffix SUFFIX
                        Sufixo que a DAG deverá ter no nome para entrar na análise.
```

Todos os parâmetros são opcionais, caso não seja indicado nenhum prefixo nem nenhum sufixo o script irá considerar todas as DAGs ativas no airflow.

Para uma execução de teste, que irá retornar o log de execução dos últimos 10 dias, rodar da seguinte forma:
```sh
python3 airflow.py -q 10
```
