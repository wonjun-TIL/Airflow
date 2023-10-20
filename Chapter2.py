import json
import pathlib
import airflow
import requests
import requests.exceptions as exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag=DAG( # 객체의 인스턴스 생성(구체화) - 모든 워크플로의 시작점. 워크플로 내의 모든 태스크는 DAG 개체를 참조하므로 Airflow는 어떤 태스크가 어떤 DAG에 속하는지 확인 할 수 있음.
    dag_id="download_rocket_launches", # DAG의 이름
    start_date=airflow.utils.dates.days_ago(14), # DAG 처음 실행 시작 날짜/시간 # 현재로부터 14일 전을 실행시작일로 지정하겠다는 의미. -> 과거데이터를 실행
    schedule_interval=None, # DAG의 실행 간격 # None은 Dag가 자동으로 실행되지 않음을 의미함.
)
