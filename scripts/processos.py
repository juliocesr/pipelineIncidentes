import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import boto3
from io import StringIO

def coletar_dados():
    url = "https://stats.cert.br/historico/incidentes/2020-jan-dec/total.html"
    response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.text, "html.parser")
    tables = pd.read_html(StringIO(str(soup)))
    df = tables[0]
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/incidentes_certbr_2020.csv", index=False)
    print("✅ CSV salvo com sucesso!")

def transformar_dados():
    df = pd.read_csv("data/incidentes_certbr_2020.csv")
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    df.to_parquet("data/incidentes_certbr_2020.parquet", index=False)
    print("✅ Dados transformados e salvos em Parquet!")

def carregar_para_s3():
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id='AKIAQZMOKHQJWL4I76FM',
        aws_secret_access_key='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'
    )
    with open("data/incidentes_certbr_2020.parquet", "rb") as f:
        s3.upload_fileobj(f, "projincidentes", "trusted/incidentes_cert_2020.parquet")
    print("✅ Arquivo enviado ao S3!")


def carregar_para_s3():
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id='AKIAQZMOKHQJWL4I76FM',
        aws_secret_access_key='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'
    )

    with open("data/incidentes_certbr_2020.parquet", "rb") as f:
        s3.meta.client.upload_fileobj(
            Fileobj=f,
            Bucket="projincidentes",
            Key="trusted/incidentes_cert_2020.parquet"
        )

    print("✅ Arquivo enviado ao S3!")

