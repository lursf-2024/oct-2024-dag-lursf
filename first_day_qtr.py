from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Function to check if it's the first day of the quarter
def choose_task(**kwargs):
    today = datetime.now()
    # Check if today is the first day of a quarter (Jan 1, Apr 1, Jul 1, Oct 1)
    if today.month in [1, 4, 7, 10] and today.day == 1:
        return 'first_day_of_quarter_ro_details'
    else:
        return 'regular_ro_details'

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 25),
    'retries': 1
}

with DAG('quarterly_task_selector_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Branching logic to check if it's the first day of the quarter
    task_chooser = BranchPythonOperator(
        task_id='task_chooser',
        python_callable=choose_task,
        provide_context=True
    )

    # Define your first day of the quarter task
    first_day_of_quarter_ro_details = BashOperator(
        task_id='first_day_of_quarter_ro_details',
        bash_command='echo "This is the task for the first day of the quarter"'
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
    task_chooser >> [first_day_of_quarter_ro_details, regular_ro_details] >> end
