from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the function to decide the branch
def choose_task(**kwargs):
    # Check if it's the first day of the month
    if datetime.now().day == 1:
        return 'first_of_month_ro_details'
    else:
        return 'regular_ro_details'

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 25),
    'retries': 1
}

with DAG('monthly_task_selector_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Branching logic to check the day of the month
    task_chooser = BranchPythonOperator(
        task_id='task_chooser',
        python_callable=choose_task,
        provide_context=True
    )

    # Define your first day of the month task
    first_of_month_ro_details = BashOperator(
        task_id='first_of_month_ro_details',
        bash_command='echo "This is the task for the first of the month"'
    )

    # Define your regular task for other days
    regular_ro_details = BashOperator(
        task_id='regular_ro_details',
        bash_command='echo "This is the regular task"'
    )

    # Dummy operator to end the DAG
    end = DummyOperator(
        task_id='end'
    )

    # Set up the branching
    task_chooser >> [first_of_month_ro_details, regular_ro_details] >> end
