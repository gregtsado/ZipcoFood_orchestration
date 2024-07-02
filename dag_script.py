from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from extraction import run_extraction 
from transformation import run_transformation
from loading import run_loading

default_args ={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,7,8),
    'email':'greg@alliance4ai.org',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retries_delay':timedelta(minutes=1)
}

dag = DAG(
    'zipco_food_pipeline',
    default_args =default_args,
    description ='This represents Zipco food data management pipeline'
)

extraction = PythonOperator(
    task_id = 'exctraction_layer',
    python_callable=run_extraction,
    dag=dag
)

transformation =PythonOperator(
    task_id = 'transformation_layer',
    python_callable=run_transformation,
    dag=dag
)

loading =PythonOperator(
    task_id = 'loading_layer',
    python_callable=run_loading,
    dag=dag
)

extraction >> transformation >> loading