from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'mubarok',
    'retries': '5',
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id = 'dag_with_postgres',
    default_args = default_args,
    start_date = datetime(2022,11,21),
    schedule_interval = '0 0 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgress_localhost',
        sql = """
            create table if not exists table_data(
                title character varying,
                author character varying
            )
        """
    )

    task2= PostgresOperator(
        task_id = 'insert_data_to_postgres',
        postgres_conn_id = 'postgress_localhost',
        sql = """
                insert into table_data(title, author)
                values ('DEVELOPMENT OF VACCINE TRACKING APPLICATION USING AGILE METHODOLOGY','Tegar Arifin Prasetyo'), 
                ('ALGORITMA SEQUENTIAL SEARCH DAN BINARY SEARCH PADA SISTEM PENCARIAN E-ARSIP BERBASIS WEB','ismail ismail ismail'), 
                ('IMPLEMENTASI METODE LOAD BALANCING UNTUK PENINGKATAN NILAI TROUGHPUT PADA SERVER','Rini Nuraini')
        """ 
      
   )

    task3= PostgresOperator(
            task_id = 'delete_data_to_postgres',
            postgres_conn_id = 'postgress_localhost',
            sql = """
                    delete from table_data where author = 'Rini Nuraini'
            """ 
    )
    task1 >> task2 >> task3