# airflow-monitor
Projeto para monitoramento do airflow

# Como compilar
Para compilar o projeto é preciso alterar o Dockerfile com as credenciais necessárias.

Caso você queira executar no ambiente MWAA não é necessário preencher as informações de  URL, username e password, pois o MWAA realiza o login utilizando o boto3.

Caso você queira executar em um ambiente local de airflow, será necessário indicar a URL o username e a senha para que a API possa autenticar, e não será necessário indicar as credenciais de AWS.

Para dar o build no container deverá executar o comando:

> docker build -t airflow-monitor:latest -f Dockerfile .

# Como executar
> docker run --rm -it --entrypoint bash airflow-monitor:latest
