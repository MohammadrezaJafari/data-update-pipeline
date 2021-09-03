from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'traffic',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
generate_sauron = BashOperator(
    task_id='generate_sauron',
    bash_command='date',
    dag=dag,
)

prepare_sharedsreets_data = BashOperator(
    task_id='prepare_sharedsreets_data',
    bash_command='date',
    dag=dag,
)

prepare_sharedsreets_tiles = BashOperator(
    task_id='prepare_sharedsreets_tiles',
    bash_command='date',
    dag=dag,
)

prepare_sharedsreets_tiles_mmc = BashOperator(
    task_id='prepare_sharedsreets_tiles_mmc',
    bash_command='date',
    dag=dag,
)

prepare_valhalla_data = BashOperator(
    task_id='prepare_valhalla_data',
    bash_command='date',
    dag=dag,
)

deploy_valhalla_teh1 = BashOperator(
    task_id='deploy_valhalla_teh1',
    bash_command='date',
    dag=dag,
)

deploy_valhalla_teh2 = BashOperator(
    task_id='deploy_valhalla_teh2',
    bash_command='date',
    dag=dag,
)


generate_traffic_data = BashOperator(
    task_id='generate_traffic_data',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=3,
    dag=dag,
)




dag.doc_md = __doc__

generate_sauron.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""
templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

update_central_data_repository = BashOperator(
    task_id='update_central_data_repository',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

generate_cluster_label = BashOperator(
    task_id='generate_cluster_label',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

update_central_data_repository = BashOperator(
    task_id='update_central_data_repository',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

generate_sauron >> prepare_sharedsreets_data >> [prepare_sharedsreets_tiles, prepare_sharedsreets_tiles_mmc]

prepare_sharedsreets_tiles >> prepare_valhalla_data >> [deploy_valhalla_teh1, deploy_valhalla_teh2]

# prepare_sharedsreets_data >> generate_traffic_data >> update_central_data_repository

# prepare_sharedsreets_data >> generate_cluster_label >> generate_cluster_label