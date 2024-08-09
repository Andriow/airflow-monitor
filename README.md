# airflow-monitor
Projeto para monitoramento de execuções de DAGs com falhas no airflow

# Configuração do docker para uso em ambiente próprio
No Dockerfile possui as variáveis de ambiente do container:

```Dockerfile
ENV AWS_REGION="YOUR-REGION" \
    AWS_ACCESS_KEY_ID="YOUR-KEY-ID" \
    AWS_SECRET_ACCESS_KEY="YOUR-SECRET-ACCESS-KEY" \
    AIRFLOW_URL="http://YOUR_AIRFLOW_URL" \
    AIRFLOW_USERNAME="YOUR AIRFLOW USER" \
    AIRFLOW_PASSWORD="YOUR AIRFLOW PASSWORD"
```

Para execução em ambiente próprio, ou em alguma ferramenta gerenciada que permita login por usuário e senha, será necessário alterar as seguintes informações antes de compilar o container:
> AIRFLOW_URL="http://YOUR_AIRFLOW_URL"

> AIRFLOW_USERNAME="YOUR AIRFLOW USER"

> AIRFLOW_PASSWORD="YOUR AIRFLOW PASSWORD"

Nesse caso pode deixar as variáveis relacionadas a AWS com o valor padrão.

# Configuração do docker para uso no ambiente AWS utilizando o MWAA
No Dockerfile possui as variáveis de ambiente do container:

```Dockerfile
ENV AWS_REGION="YOUR-REGION" \
    AWS_ACCESS_KEY_ID="YOUR-KEY-ID" \
    AWS_SECRET_ACCESS_KEY="YOUR-SECRET-ACCESS-KEY" \
    AIRFLOW_URL="http://YOUR_AIRFLOW_URL" \
    AIRFLOW_USERNAME="YOUR AIRFLOW USER" \
    AIRFLOW_PASSWORD="YOUR AIRFLOW PASSWORD"
```

Para execução no MWAA da AWS será necessário alterar as seguintes informações antes de compilar o container:
> AWS_REGION="YOUR-REGION"

> AWS_ACCESS_KEY_ID="YOUR-KEY-ID"

> AWS_SECRET_ACCESS_KEY="YOUR-SECRET-ACCESS-KEY"

# Compilação do container
Para dar o build no container deverá executar o comando:

> docker build -t airflow-monitor:latest -f Dockerfile .

# Como executar
Para a execução existem dois arquivos distintos:
> airflow.py

> airflowMWAA.py

Os dois scripts possuem exatamente a mesma forma de ser executado, alterando apenas a forma de realizar a autenticação, então pode-se executar os dois scripts da mesma fora.

O primeiro passo será de entrar no Container compilado:

> docker run --rm -it --entrypoint bash airflow-monitor:latest

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

Para uma execução de teste, que irá retornar o log de execução dos últimos 10 dias, rodar da seguinte forma:
```sh
python3 airflow.py -q 10
```
