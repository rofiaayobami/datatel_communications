from __future__ import annotations

import os  
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param  
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule  

# PATHS
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(DAG_DIR, "..", "sql")


def sql_path(*parts: str) -> str:
    """Return the absolute path to a SQL file."""
    return os.path.join(SQL_DIR, *parts)


# DEFAULT ARGS
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG DEFINITION
with DAG(
    dag_id="datatel_pipeline",
    default_args=default_args,
    description="DataTel end-to-end data pipeline",
    schedule_interval="0 3 * * *",       # 03:00 WAT daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["datatel", "pipeline"],
    template_searchpath=[SQL_DIR],
    params={
        "t_start": Param(default=None, type=["string", "null"], description="YYYY-MM-DD. Fallback is logical date."),
        "t_end":   Param(default=None, type=["string", "null"], description="YYYY-MM-DD. Fallback is next logical date."),
    },
) as dag:

    # STAGE 0 — Pipeline markers
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # STAGE 1 — DATA QUALITY CHECKS
    quality_billing = SQLExecuteQueryOperator(
        task_id="quality_check_billing",
        conn_id="postgres_datatel",  
        sql="data_quality_checks/check_and_quarantine_billing.sql",
    )

    quality_sessions = SQLExecuteQueryOperator(
        task_id="quality_check_sessions",
        conn_id="postgres_datatel",  
        sql="data_quality_checks/check_and_quarantine_sessions.sql",
    )

    quality_customers = SQLExecuteQueryOperator(
        task_id="quality_check_customers",
        conn_id="postgres_datatel",  
        sql="data_quality_checks/check_and_quarantine_customers.sql",
    )

    # STAGE 2 — STAGING LAYER
    stage_customers = SQLExecuteQueryOperator(
        task_id="stage_customers",
        conn_id="postgres_datatel",  
        sql="staging/stg_customers.sql",
    )

    stage_billing = SQLExecuteQueryOperator(
        task_id="stage_billing_incremental",
        conn_id="postgres_datatel",
        sql="incremental/incremental_billing.sql",
        parameters={
            "t_start": "{{ params.t_start if params.t_start else ds }}",
            "t_end":   "{{ params.t_end if params.t_end else next_ds }}",
        },
    )

    stage_sessions = SQLExecuteQueryOperator(
        task_id="stage_sessions",
        conn_id="postgres_datatel",
        sql="staging/stg_network_sessions.sql",
    )

    # STAGE 3 — TRANSFORMATIONS
    agg_user_revenue = SQLExecuteQueryOperator(
        task_id="agg_user_revenue",
        conn_id="postgres_datatel",
        sql="transformation/agg_user_revenue.sql",
    )

    agg_arpu = SQLExecuteQueryOperator(
        task_id="agg_arpu",
        conn_id="postgres_datatel",
        sql="transformation/agg_arpu.sql",
    )

    agg_user_usage = SQLExecuteQueryOperator(
        task_id="agg_user_usage",
        conn_id="postgres_datatel",
        sql="transformation/agg_user_usage.sql",
    )

    agg_monthly_revenue = SQLExecuteQueryOperator(
        task_id="agg_monthly_revenue",
        conn_id="postgres_datatel",
        sql="transformation/agg_monthly_revenue.sql",
    )

    agg_session_distribution = SQLExecuteQueryOperator(
        task_id="agg_session_distribution",
        conn_id="postgres_datatel",
        sql="transformation/agg_session_distribution.sql",
    )

    session_buckets = SQLExecuteQueryOperator(
        task_id="session_buckets",
        conn_id="postgres_datatel",
        sql="transformation/session_buckets.sql",
    )

    # STAGE - AUDIT LOGGING 
    audit_pipeline_run = SQLExecuteQueryOperator(
        task_id="audit_pipeline_run",
        conn_id="postgres_datatel",
        sql="""
        INSERT INTO pipeline_audit_log (
            run_date,
            billing_rows,
            session_rows,
            customer_rows,
            status,
            created_at
        )
        SELECT
            CURRENT_DATE,
            (SELECT COUNT(*) FROM stg_billing),
            (SELECT COUNT(*) FROM stg_network_sessions),
            (SELECT COUNT(*) FROM stg_customers),
            'SUCCESS',
            NOW();
        """,
    )

    # STAGE 4 — LOCAL DATA WAREHOUSE CONSOLIDATION
    load_warehouse = SQLExecuteQueryOperator(
        task_id="load_dw_user_analytics",
        conn_id="postgres_datatel",  
        sql="warehouse/dw_user_analytics.sql",
    )

    # DEPENDENCY GRAPH 
 # Start fans out to all three quality checks in parallel
    start >> [quality_billing, quality_sessions, quality_customers]

# Each quality check gates its own staging task
    quality_billing   >> stage_billing
    quality_sessions  >> stage_sessions
    quality_customers >> stage_customers

# Transformation tasks execute according to their source
#    data dependencies
    stage_billing   >> [agg_user_revenue, agg_monthly_revenue, agg_arpu]
    stage_sessions  >> [agg_user_usage, session_buckets]
    stage_customers >> [agg_arpu]
    
# session distribution analysis depends on the completion
#    of session bucketing because it aggregates bucket results
    session_buckets >> agg_session_distribution

#After all transformation outputs are successfully
#    generated, the pipeline:
#       - Writes execution metadata to the audit table
#       - Loads the analytical warehouse table
    all_transforms = [
        agg_user_revenue, 
        agg_monthly_revenue, 
        agg_arpu, 
        agg_user_usage, 
        agg_session_distribution
    ]
    
#The pipeline completes only after both the audit logging
#    and warehouse loading processes finish successfully.
    all_transforms >> audit_pipeline_run >> end
    all_transforms >> load_warehouse >> end
