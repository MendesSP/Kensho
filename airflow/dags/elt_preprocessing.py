from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
'owner': 'test_user',
'start_date': days_ago(1),
'depends_on_past': False,
'email': ['info@example.com'],
'email_on_failure': True,
'email_on_retry': False,
'retries': 0,
'retry_delay': timedelta(minutes=30),
}

with DAG('etl_preprocessing',default_args=default_args,schedule_interval=timedelta(days=1)) as dag:

	t1 = DockerOperator(
		task_id='raw-to-db_web',
		image='raw_to_db_web:latest',
		api_version='auto',
		auto_remove=True,
		docker_url="unix://var/run/docker.sock",
		network_mode="bridge",
		host_tmp_dir='/tmp'
	)

	t2 = DockerOperator(
		task_id='split-data_web',
		image='split_data_web:latest',
		api_version='auto',
		auto_remove=True,
		docker_url="unix://var/run/docker.sock",
		network_mode="bridge",
		host_tmp_dir='/tmp'
	)

	t3 = DockerOperator(
		task_id='preprocess_train',
		image='feature_selection_web:latest',
		api_version='auto',
		auto_remove=True,
		docker_url="unix://var/run/docker.sock",
		network_mode="bridge",
		host_tmp_dir='/tmp',
		environment={'OPT': "train"}
	)

	t4 = DockerOperator(
		task_id='preprocess_test',
		image='feature_selection_web:latest',
		api_version='auto',
		auto_remove=True,
		docker_url="unix://var/run/docker.sock",
		network_mode="bridge",
		host_tmp_dir='/tmp',
		environment={'OPT': "test"}
	)

	t5 = DockerOperator(
		task_id='train',
		image='train_web:latest',
		api_version='auto',
		auto_remove=True,
		docker_url="unix://var/run/docker.sock",
		network_mode="bridge",
		host_tmp_dir='/tmp'
	)




	ready_task 	= DummyOperator(task_id='start')
	ready_task >> t1 >> t2 >> t3 >> t4 >> t5
