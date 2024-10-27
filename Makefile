run_airflow:
	AIRFLOW__CORE__DAGS_FOLDER=./src/dags PYTHONPATH=. airflow standalone
