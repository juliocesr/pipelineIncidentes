from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.processos import coletar_dados, transformar_dados, carregar_para_s3

default_args = {
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id="etl_certbr_dag",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=["cert.br", "etl"]
) as dag:

    tarefa1 = PythonOperator(
        task_id="coletar_dados_certbr",
        python_callable=coletar_dados
    )

    tarefa2 = PythonOperator(
        task_id="transformar_dados_certbr",
        python_callable=transformar_dados
    )

    tarefa3 = PythonOperator(
        task_id="carregar_dados_s3",
        python_callable=carregar_para_s3
    )

    tarefa1 >> tarefa2 >> tarefa3
