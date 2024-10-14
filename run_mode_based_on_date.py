from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Function to check if a date is the first day of the month
def first_day_of_mo(date_to_check=None):
    if date_to_check is None:
        date_to_check = datetime.now()
    return date_to_check.day == 1

# Function to check if a date is the last day of the month
def last_day_of_mo(date_to_check=None):
    if date_to_check is None:
        date_to_check = datetime.now()
    
    # Calculate the first day of the next month
    next_month = date_to_check.replace(day=28) + timedelta(days=4)
    last_day_of_month = next_month - timedelta(days=next_month.day)
    
    return date_to_check.date() == last_day_of_month.date()

# Your DAG definition
with DAG(
    'example_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Example of setting the run mode based on the first or last day of the month
    run_mode = 'full' if first_day_of_mo() else 'end_of_month' if last_day_of_mo() else None

    def task_function():
        # Example of accessing the overridden run_mode variable within a task
        print(f"Run mode is set to: {run_mode}")

    # Define a PythonOperator to use the task function
    check_run_mode_task = PythonOperator(
        task_id='check_run_mode_task',
        python_callable=task_function
    )

    check_run_mode_task
