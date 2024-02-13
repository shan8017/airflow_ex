from datetime import datetime
import json
from airflow import DAG
from pandas import json_normalize

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from preprocess.naver_preprocess import preprocessing
from preprocess.chk_api_data import chk_api_data
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import random

from airflow.decorators import task, dag
from airflow.utils.edgemodifier import Label

@dag(
        start_date=datetime(2024,2,13),
        catchup=False,
        schedule="0 * * * *"
     )
def branch_python_operator_decorator_example():
    run_this_first = EmptyOperator(task_id="run_this_first")
    options = ["branch_a", "branch_b", "branch_c", "branch_d"]

    @task.branch(task_id="branching")
    def random_choice(choices):
        return random.choice(choices)

    random_choice_instance = random_choice(choices=options)

    run_this_first >> random_choice_instance

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success"
    )

    for option in options:

        t = EmptyOperator(
            task_id=option
        )

        empty_follow = EmptyOperator(
            task_id="follow_" + option
        )

        # Label is optional here, but it can help identify more complex branches
        random_choice_instance >> Label(option) >> t >> empty_follow >> join


branch_python_operator_decorator_example()

"""
def print_today():
    print(datetime.now())

def get_run_task():
    return random.choice([task_print_today.task_id, task_empty.task_id])


def _complete():
    print("네이버 검색 DAG 완료")


default_args = {
        "start_date": datetime(2024,2,8)
        }

NAVER_CLI_ID = "WgJKHPluzWyPEwTR7oSP"
NAVER_CLI_SECRET = "VIC5k3vXdD"


with DAG(
        dag_id = "naver-search-pipeline",
        schedule_interval="0 * * * *", #"@daily",
        default_args=default_args,
        tags=["naver","search","local","api","pipeline"],
        catchup=False) as dag:

    task_branch = BranchPythonOperator(
        task_id = 'task_branch',
        python_callable=get_run_task
        )
    
    task_print_today = PythonOperator(
        task_id='task_print_today',
        python_callable=print_today,
        )
    task_empty = EmptyOperator(
        task_id='task_empty',
        )

    creating_table = SqliteOperator(
        task_id="creating_table",
        sqlite_conn_id="db_sqlite",
        sql='''
            CREATE TABLE IF NOT EXISTS naver_search_result(
            title TEXT,
            address TEXT,
            category TEXT,
            description TEXT,
            link TEXT
            )
        '''
        )

    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="naver_search_api",
        endpoint="v1/search/local.json",
        headers = {
            "X-Naver-Client-Id" : f"{NAVER_CLI_ID}",
            "X-Naver-Client-Secret" : f"{NAVER_CLI_SECRET}",
            },
        request_params = {
            "query":"김치찌개",
            "display":5
            },
        response_check=lambda response: response.json(),
        dag=dag,
        )

    crawl_naver = SimpleHttpOperator(
        task_id = "crawl_naver",
        http_conn_id = "naver_search_api",
        endpoint = "v1/search/local.json", # url 설정
        headers = {
            "X-Naver-Client-Id" : f"{NAVER_CLI_ID}",
            "X-Naver-Client-Secret" : f"{NAVER_CLI_SECRET}",
            }, # 요청 헤더
        data = {
            "query": "김치찌개",
            "display": 5
            }, # 요청 변수
        method = "GET", # 통신 방식 GET, POST 등등 맞는 것으로
        response_filter = lambda res : json.loads(res.text),
        log_response = True
        )

    chk_api_data = PythonOperator(
            task_id = 'chk_api_data',
            python_callable = chk_api_data,
            )

    preprocess_result = PythonOperator(
        task_id="preprocess_result",
        python_callable=preprocessing
        )

    store_result = BashOperator(
        task_id="store_naver",
        bash_command='echo -e ".separator ","\n.import /home/shan/airflow/dags/data/naver_processed_result.csv naver_search_result" | sqlite3 /home/shan/airflow/airflow.db'
        )

    print_complete = PythonOperator(
        task_id="print_complete",
        python_callable=_complete
        )

    creating_table >> task_branch >> [[task_print_today >> is_api_available >> crawl_naver >> [chk_api_data, preprocess_result] >> store_result], task_empty] >> print_complete
"""
