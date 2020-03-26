from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
                               
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
    

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 3, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udend_capstone_Immig_Prj_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *',
          catchup = False,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_immig_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Immig',
    dag=dag,
    table="Immigration_Stg",
    s3_bucket="udend-capstone-immig",
    s3_key="immig/"
    
)
stage_airport_to_redshift = StageToRedshiftOperator(
    task_id='Stage_airport',
    dag=dag,
    table="Airport_Code_Stg",
    s3_bucket="udend-capstone-immig",
    s3_key="airport_data/"
    
)

stage_port_to_redshift = StageToRedshiftOperator(
    task_id='Stage_port',
    dag=dag,
    table="I94_port",
    s3_bucket="udend-capstone-immig",
    s3_key="i94_port/"
    
)

stage_state_to_redshift = StageToRedshiftOperator(
    task_id='Stage_state',
    dag=dag,
    table="State_Stg",
    s3_bucket="udend-capstone-immig",
    s3_key="i94_state/"
    
)

stage_visa_to_redshift = StageToRedshiftOperator(
    task_id='Stage_visa',
    dag=dag,
    table="Visa_Stg",
    s3_bucket="udend-capstone-immig",
    s3_key="i94_visa/"
    
)

stage_I94_Mode_to_redshift = StageToRedshiftOperator(
    task_id='Stage_I94mode',
    dag=dag,
    table="I94_Mode_Stg",
    s3_bucket="udend-capstone-immig",
    s3_key="i94_mode/"
    
)

stage_countryCodes_to_redshift = StageToRedshiftOperator(
    task_id='Stage_country',
    dag=dag,
    table="Country_Stg",
    s3_bucket="udend-capstone-immig",
    s3_key="countryCodes/"
    
)

stage_usdemo_to_redshift= StageToRedshiftOperator(
    task_id='Stage_us_demographics',
    dag=dag,
    table="us_demographics_stg",
    s3_bucket="udend-capstone-immig",
    s3_key="us_demographics/"
    
)

load_country_dimension_table = LoadDimensionOperator(
    task_id='Load_country_dim_table',
    dag=dag,
    table="dim_country",
    sql=SqlQueries.country_table_insert
    
)
load_state_dimension_table = LoadDimensionOperator(
    task_id='Load_state_dim_table',
    dag=dag,
    table="dim_state",
    sql=SqlQueries.state_table_insert
    
)

load_i94mode_dimension_table = LoadDimensionOperator(
    task_id='Load_i94mode_dim_table',
    dag=dag,
    table="dim_i94mode",
    sql=SqlQueries.i94mode_table_insert
    
)

load_i94visa_dimension_table = LoadDimensionOperator(
    task_id='Load_visa_dim_table',
    dag=dag,
    table="dim_visa",
    sql=SqlQueries.i94visa_table_insert
    
)

load_airport_dimension_table = LoadDimensionOperator(
    task_id='Load_airport_dim_table',
    dag=dag,
    table="dim_airport",
    sql=SqlQueries.airport_table_insert
    
)

load_date_dimension_table = LoadDimensionOperator(
    task_id='Load_date_dim_table',
    dag=dag,
    table="dim_date",
    sql=SqlQueries.Date_table_insert
    
)

load_i94port_dimension_table = LoadDimensionOperator(
    task_id='Load_i94port_dim_table',
    dag=dag,
    table="DIM_i94port",
    sql=SqlQueries.i94port_table_insert
    
)
load_immig_fact_table = LoadFactOperator(
    task_id='Load_immigration_fact_table',
    dag=dag,
    table="immigration_fact",
    sql=SqlQueries.immigration_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table=["IMMIGRATION_FACT", "DIM_i94port", "Dim_Date", "Dim_Country", "Dim_i94mode","Dim_Airport","Dim_State","Dim_Visa"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator  >> stage_immig_to_redshift 
start_operator >> stage_airport_to_redshift
start_operator >> stage_port_to_redshift
start_operator >> stage_state_to_redshift
start_operator >> stage_visa_to_redshift
start_operator >> stage_I94_Mode_to_redshift
start_operator >> stage_countryCodes_to_redshift
start_operator >> stage_usdemo_to_redshift

stage_immig_to_redshift >> load_country_dimension_table
stage_immig_to_redshift >> load_state_dimension_table
stage_immig_to_redshift >> load_i94mode_dimension_table
stage_immig_to_redshift >> load_i94visa_dimension_table
stage_immig_to_redshift >> load_airport_dimension_table
stage_immig_to_redshift >>load_date_dimension_table
stage_immig_to_redshift >> load_i94port_dimension_table

stage_airport_to_redshift >> load_country_dimension_table
stage_airport_to_redshift >> load_state_dimension_table
stage_airport_to_redshift >> load_i94mode_dimension_table
stage_airport_to_redshift >> load_i94visa_dimension_table
stage_airport_to_redshift >> load_airport_dimension_table
stage_airport_to_redshift >>load_date_dimension_table
stage_airport_to_redshift >> load_i94port_dimension_table

stage_port_to_redshift >> load_country_dimension_table
stage_port_to_redshift >> load_state_dimension_table
stage_port_to_redshift >> load_i94mode_dimension_table
stage_port_to_redshift >> load_i94visa_dimension_table
stage_port_to_redshift >> load_airport_dimension_table
stage_port_to_redshift >>load_date_dimension_table
stage_port_to_redshift >> load_i94port_dimension_table

stage_state_to_redshift >> load_country_dimension_table
stage_state_to_redshift >> load_state_dimension_table
stage_state_to_redshift >> load_i94mode_dimension_table
stage_state_to_redshift >> load_i94visa_dimension_table
stage_state_to_redshift >> load_airport_dimension_table
stage_state_to_redshift >>load_date_dimension_table
stage_state_to_redshift >> load_i94port_dimension_table

stage_visa_to_redshift >> load_country_dimension_table
stage_visa_to_redshift >> load_state_dimension_table
stage_visa_to_redshift >> load_i94mode_dimension_table
stage_visa_to_redshift >> load_i94visa_dimension_table
stage_visa_to_redshift >> load_airport_dimension_table
stage_visa_to_redshift >>load_date_dimension_table
stage_visa_to_redshift >> load_i94port_dimension_table

stage_I94_Mode_to_redshift >> load_country_dimension_table
stage_I94_Mode_to_redshift >> load_state_dimension_table
stage_I94_Mode_to_redshift >> load_i94mode_dimension_table
stage_I94_Mode_to_redshift >> load_i94visa_dimension_table
stage_I94_Mode_to_redshift >> load_airport_dimension_table
stage_I94_Mode_to_redshift >>load_date_dimension_table
stage_I94_Mode_to_redshift >> load_i94port_dimension_table

stage_countryCodes_to_redshift >> load_country_dimension_table
stage_countryCodes_to_redshift >> load_state_dimension_table
stage_countryCodes_to_redshift >> load_i94mode_dimension_table
stage_countryCodes_to_redshift >> load_i94visa_dimension_table
stage_countryCodes_to_redshift >> load_airport_dimension_table
stage_countryCodes_to_redshift >>load_date_dimension_table
stage_countryCodes_to_redshift >> load_i94port_dimension_table

stage_usdemo_to_redshift >> load_country_dimension_table
stage_usdemo_to_redshift >> load_state_dimension_table
stage_usdemo_to_redshift >> load_i94mode_dimension_table
stage_usdemo_to_redshift >> load_i94visa_dimension_table
stage_usdemo_to_redshift >> load_airport_dimension_table
stage_usdemo_to_redshift >> load_date_dimension_table
stage_usdemo_to_redshift >> load_i94port_dimension_table

load_country_dimension_table >> load_immig_fact_table
load_state_dimension_table  >> load_immig_fact_table
load_i94mode_dimension_table >> load_immig_fact_table
load_i94visa_dimension_table >> load_immig_fact_table
load_airport_dimension_table >> load_immig_fact_table
load_date_dimension_table >> load_immig_fact_table
load_i94port_dimension_table >> load_immig_fact_table

load_immig_fact_table >> run_quality_checks

run_quality_checks >> end_operator

