import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag=DAG( # 객체의 인스턴스 생성(구체화) - 모든 워크플로의 시작점. 워크플로 내의 모든 태스크는 DAG 개체를 참조하므로 Airflow는 어떤 태스크가 어떤 DAG에 속하는지 확인 할 수 있음.
    dag_id="download_rocket_launches", # DAG의 이름
    start_date=airflow.utils.dates.days_ago(14), # DAG 처음 실행 시작 날짜/시간 # 현재로부터 14일 전을 실행시작일로 지정하겠다는 의미. -> 과거데이터를 실행
    schedule_interval=None, # DAG의 실행 간격 # None은 Dag가 자동으로 실행되지 않음을 의미함.
)

# 배시 커맨드를 실행하기 위해 BashOperator 객체 인스턴스 생성
# 각 오퍼레이터는 하나의 태스크를 수행하고 여러 개의 오퍼레이터가 워크플로 또는 Dag를 구성. 
download_launches = BashOperator(
    task_id="download_launches", # 태스크의 이름
    bash_command="curl -o /tmp/launches.json 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'", # 실행할 배시 커맨드
    dag = dag # DAG 변수에 대한 참조.
)

# 오퍼레이터는 서로 독립적으로 실행할 수 있지만, 순서를 정의해 실행할 수도 있음.
# 이를 의존성(dependency)라고 함.
# 사진을 저장하는 디렉터리 위치에 대한 정보가 없는 상태에서 사진을 다운로드 하면, 워크플로는 정상 동작하지 않을 것임.


# 테스크와 오퍼레이터의 차이?

# Operator
# Airflow에서 오퍼레이터는 단일 작업 수행 역할을 한다.
# BashOperator는 단일 배시 커맨드를 실행.
# PythonOperator는 단일 파이썬 함수를 실행.
# EmailOperator는 단일 이메일을 보냄.
# SimpleHttpOperator는 단일 HTTP 엔드포인트 호출

# DAG는 오퍼레이터 집합에 대한실행을 조율하는 역할
# 오퍼레이션의 시작, 정지, 다음 태스크의 시작, 오퍼레이션간의 의존성보장 등을 담당.

# 태스크는 오퍼레이터의 인스턴스
# 사용자는 오퍼레이터로 수행할 작업에 집중하는 반면
# Airflow는 태스크를 통해 작업을 올바르게 실행함


# PythonOperator를 사용한 파이썬 함수 실행
def _get_pictures(): # 호출할 파이썬 함수
    # 디렉터리 확인
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True) # 디렉터리가 존재하지 않으면 생성
    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f: # 이전 단계의 테스크 결과 확인
        launches=json.load(f) # 데이터를 섞을 수 있도록 딕셔너리로 읽기.
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response=requests.get(image_url) # 각각의 이미지 다운로드
                image_filename=image_url.split("/")[-1]
                target_file=f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content) # 각각의 이미지 저장
                print(f"Downloaded {image_url} to {target_file}") # Airflow 로그에 저장하기 위해 stdout으로 출력
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

get_pictures=PythonOperator( # 파이썬 함수 호출을 위해 PythonOperator 구체화
    task_id="get_pictures",
    python_callable=_get_pictures, # 실행할 파이썬 함수를 지정
    dag=dag,
    )

# PythonOperator을 사용 시 다음 두가지 사항을 항상 적용해야함.
# 1. 오퍼레이터 자신(get_pictures)를 정의해야함
# 2. python_callable은 인수에 호출이 가능한 일반함수(_get_pictures) 가리킴
