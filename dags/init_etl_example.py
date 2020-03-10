from __future__ import print_function
import os 
import airflow
import logging
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow import models
from airflow.settings import Session
from configparser import ConfigParser


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True
}


def initialize_etl_example():
    logging.info('Reading config')
    session = Session()

    def create_new_conn(session, attributes):
        new_conn = models.Connection()
        new_conn.conn_id = attributes.get("conn_id")
        new_conn.conn_type = attributes.get('conn_type')
        new_conn.host = attributes.get('host')
        new_conn.port = attributes.get('port')
        new_conn.schema = attributes.get('schema')
        new_conn.login = attributes.get('login')
        new_conn.set_password(attributes.get('password'))

        session.add(new_conn)
        session.commit()

    def config(filename='../.postgresql.ini'):
        # create a parser
        parser = ConfigParser(strict=False)
        # read config file
        parser.read(filename)
        # get section, default to postgresql
        db_config = {}
        logging.info('Creating connections, pool and sql path')

        for section in parser.sections():
            conn_id = section
            conn_type = section.split('_')
            for param in parser.items(section):
                db_config[param[0]] = param[1]

            create_new_conn(session,
                    {"conn_id": conn_id,
                     "conn_type": conn_type[0],
                     "host": db_config['host'],
                     "port": db_config['port'],
                     "schema": db_config['dbname'],
                     "login": db_config['user'],
                     "password": db_config['password']})

    config()

    new_var = models.Variable()
    new_var.key = "sql_path"
    new_var.set_val("/Users/jackychu/airflow/sql")
    session.add(new_var)
    session.commit()

    session.close()

dag = airflow.DAG(
    'init_example',
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1)

t1 = PythonOperator(task_id='initialize_etl_example',
                    python_callable=initialize_etl_example,
                    provide_context=False,
                    dag=dag)

sleep = BashOperator(
        task_id='sleep20',
        bash_command="sleep 20",
        dag=dag
)

t1 >> sleep