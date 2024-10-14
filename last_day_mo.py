from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Function to check if it's the last day of the month
def choose_task(**kwargs):
    today = datetime.now()
    # Add one day to today's date and check if the day becomes 1, meaning today is the last day of the month
    next_day = today + timedelta(days=1)
    if next_day.day == 1:
        return 'last_day_of_month_ro_details'
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

    # Branching logic to check if it's the last day of the month
    task_chooser = BranchPythonOperator(
        task_id='task_chooser',
        python_callable=choose_task,
        provide_context=True
    )

    # Define your last day of the month task
    last_day_of_month_ro_details = BashOperator(
        task_id='last_day_of_month_ro_details',
        bash_command='echo "This is the task for the last day of the month"'
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
    task_chooser >> [last_day_of_month_ro_details, regular_ro_details] >> end
