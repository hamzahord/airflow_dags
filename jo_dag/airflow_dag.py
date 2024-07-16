from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import pandas as pd
from supabase_py import create_client, Client
import os
import json

# Configurer les arguments par défaut pour le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Chemin vers le fichier data.txt
file_path = '/home/hamza/jo.csv'

# Fonction pour traiter les données
def process_data(**kwargs):
    # Lire le fichier CSV
    data = pd.read_csv(file_path, delimiter=';')

    # Split the rows based on the sports column
    split_rows = []

    for index, row in data.iterrows():
        sports = row['Sports'].split(',')
        for sport in sports:
            new_row = row.copy()
            new_row['Sports'] = sport.strip()
            split_rows.append(new_row)


    # Créer une DataFrame à partir des lignes éclatées
    processed_data = pd.DataFrame(split_rows)
    processed_data['Code_Site'] = processed_data['Code_Site'].astype(str)
    processed_data['Nom_Site'] = processed_data['Nom_Site'].astype(str)
    processed_data['category_id'] = processed_data['category_id'].astype(str)
    processed_data['Sports'] = processed_data['Sports'].astype(str)
    processed_data['start_date'] = processed_data['start_date'].astype(str)
    processed_data['end_date'] = processed_data['end_date'].astype(str)
    processed_data['latitude'] = processed_data['latitude'].astype(str)
    processed_data['longitude'] = processed_data['longitude'].astype(str)
    processed_data['point_geo'] = processed_data['point_geo'].astype(str)
    processed_data['adress'] = processed_data['adress'].astype(str)

    df_json = processed_data.to_json(orient='records', date_format='iso')
    # Pousser les données prétraitées dans XCom
    kwargs['ti'].xcom_push(key='processed_data', value=df_json)
    print("success")

def insert_data_to_supabase(**kwargs):
    try:
        # Récupérer les données traitées depuis XCom
        new_df_json = kwargs['ti'].xcom_pull(key='processed_data')
        new_df = pd.read_json(new_df_json, orient='records')
        print(new_df.dtypes)
        new_df = new_df.astype(str)
        # Récupérer la connexion Supabase depuis Airflow
        conn = BaseHook.get_connection('supabase_jo')  # Remplacez 'supabase_conn'
        supabase_url = conn.host
        supabase_key = conn.password
        supabase: Client = create_client(supabase_url, supabase_key)

        for index, row in new_df.iterrows():
            data_dict = row.to_dict()
            response = supabase.table('jo').insert(data_dict).execute()
            print("insertion")
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier ou de l'insertion des données : {str(e)}")


with DAG('preprocess_and_insert_to_supabase',
         default_args=default_args,
         schedule_interval='*/30 * * * *',  # Exécution toutes les 30 minutes
         catchup=False) as dag:

    # Tâche pour traiter les données
    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data,
        provide_context=True,
    )

    # Tâche pour insérer les données traitées dans Supabase
    insert_data_task = PythonOperator(
        task_id='insert_data_task',
        python_callable=insert_data_to_supabase,
        provide_context=True,
    )

    # Définir l'ordre des tâches
    process_data_task >> insert_data_task