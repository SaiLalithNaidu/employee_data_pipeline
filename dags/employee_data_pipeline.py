from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


AWS_REGION = "ap-southeast-2"

GLUE_JOB_NAME = "employee-data-etl"

REDSHIFT_WORKGROUP = "employee-data-workgroup"
REDSHIFT_DATABASE = "dev"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
}



with DAG(
    dag_id="employee_data_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    end_date=datetime(2026, 10, 11, 23, 59),
    schedule="30 3 * * *",
    catchup=False,
    tags=["employee", "aws", "glue", "redshift"],
) as dag:


    run_glue = GlueJobOperator(
        task_id="run_glue_etl",
        job_name=GLUE_JOB_NAME,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        deferrable=False,
    )


    clear_redshift = RedshiftDataOperator(
        task_id="clear_redshift",
        aws_conn_id="aws_default",
        workgroup_name=REDSHIFT_WORKGROUP,
        database=REDSHIFT_DATABASE,
        sql="DELETE FROM public.employees;",
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    load_redshift = RedshiftDataOperator(
        task_id="load_redshift",
        aws_conn_id="aws_default",
        workgroup_name=REDSHIFT_WORKGROUP,
        database=REDSHIFT_DATABASE,
        sql="""
            COPY public.employees
            FROM 's3://employee-data-platform-sai-2026/processed-bucket/employees/'
            IAM_ROLE 'arn:aws:iam::682595228520:role/RedshiftEmployeeDataRole'
            FORMAT AS PARQUET;
        """,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    validate_redshift = RedshiftDataOperator(
        task_id="validate_redshift",
        aws_conn_id="aws_default",
        workgroup_name=REDSHIFT_WORKGROUP,
        database=REDSHIFT_DATABASE,
        sql="""
            SELECT COUNT(*) AS total_records
            FROM employees;
        """,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    run_glue >> clear_redshift >> load_redshift >> validate_redshift