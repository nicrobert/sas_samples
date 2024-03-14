from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.sensors.filesystem import FileSensor
from sas_airflow_provider.operators.sas_studio import SASStudioOperator
from sas_airflow_provider.operators.sas_jobexecution import SASJobExecutionOperator
from sas_airflow_provider.operators.sas_create_session import SASComputeCreateSession
from sas_airflow_provider.operators.sas_delete_session import SASComputeDeleteSession

dag = DAG(dag_id="Load_Data_Model",
   schedule=None,
   start_date=datetime(2024,3,14),
   catchup=False)

task1 = SASStudioOperator(task_id="1_LoadOrders.flw",
   exec_type="flow",
   path_type="content",
   path="/Public/Load Orders.flw",
   compute_context="SAS Studio compute context",
   connection_name="sas_default",
   exec_log=True,
   codegen_init_code=False,
   codegen_wrap_code=False,
   outlets=[Dataset("sas://sales/orders")],
   trigger_rule='all_success',
   dag=dag)

task2 = SASStudioOperator(task_id="2_LoadDimensions.flw",
   exec_type="flow",
   path_type="content",
   path="/Public/Load Dimensions.flw",
   compute_context="SAS Studio compute context",
   connection_name="sas_default",
   exec_log=True,
   codegen_init_code=False,
   codegen_wrap_code=False,
   trigger_rule='all_success',
   dag=dag)

task3 = SASStudioOperator(task_id="3_CheckIntegrity.flw",
   exec_type="flow",
   path_type="content",
   path="/Public/Check Integrity.flw",
   compute_context="SAS Studio compute context",
   connection_name="sas_default",
   exec_log=True,
   codegen_init_code=False,
   codegen_wrap_code=False,
   trigger_rule='all_success',
   dag=dag)

task1 >> task3
task2 >> task3