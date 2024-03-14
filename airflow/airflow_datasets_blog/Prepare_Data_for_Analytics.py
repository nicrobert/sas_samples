from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.sensors.filesystem import FileSensor
from sas_airflow_provider.operators.sas_studio import SASStudioOperator
from sas_airflow_provider.operators.sas_jobexecution import SASJobExecutionOperator
from sas_airflow_provider.operators.sas_create_session import SASComputeCreateSession
from sas_airflow_provider.operators.sas_delete_session import SASComputeDeleteSession

dag = DAG(dag_id="Prepare_Data_for_Analytics",
   schedule=[Dataset("sas://sales/orders")],
   start_date=datetime(2024,3,14),
   catchup=False)

task1 = SASStudioOperator(task_id="1_AggregateOrders.flw",
   exec_type="flow",
   path_type="content",
   path="/Public/Aggregate Orders.flw",
   compute_context="SAS Studio compute context",
   connection_name="sas_default",
   exec_log=True,
   codegen_init_code=False,
   codegen_wrap_code=False,
   trigger_rule='all_success',
   dag=dag)

task2 = SASStudioOperator(task_id="2_PrepDataforMarketing.flw",
   exec_type="flow",
   path_type="content",
   path="/Public/Prep Data for Marketing.flw",
   compute_context="SAS Studio compute context",
   connection_name="sas_default",
   exec_log=True,
   codegen_init_code=False,
   codegen_wrap_code=False,
   trigger_rule='all_success',
   dag=dag)

task3 = SASStudioOperator(task_id="3_PrepDataforFinance.flw",
   exec_type="flow",
   path_type="content",
   path="/Public/Prep Data for Finance.flw",
   compute_context="SAS Studio compute context",
   connection_name="sas_default",
   exec_log=True,
   codegen_init_code=False,
   codegen_wrap_code=False,
   trigger_rule='all_success',
   dag=dag)

task1 >> task2
task1 >> task3