from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'airflow',
    'email': ['ccortinhas@ubiwhere.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    }
dag = DAG('hello_world', description='Simple tutorial DAG',
          schedule_interval=None,
          start_date=datetime(2018, 3, 18), catchup=False, default_args=default_args)
run_this = BashOperator(
    task_id='run_after_loop', bash_command='echo 1', dag=dag)


# def error_throw():
#     raise ValueError()


def print_hello():
    for i in range(3):
        i = str(i)
        task = BashOperator(
            task_id='runme_' + i,
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
            dag=dag)
        task.set_downstream(run_this)
    return 'Hello world!'


# email_operator = EmailOperator(task_id='send_test_mail', retries=3, dag=dag, to='ccortinhas@ubiwhere.com', subject='Teste', html_content='Hello')

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

# error_operator = PythonOperator(task_id='error_task', python_callable=error_throw, dag=dag)

dummy_operator >> hello_operator
