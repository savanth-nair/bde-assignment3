import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


########################################################
#
#   DAG Settings
#
#########################################################



dag_default_args = {
    'owner': 'BDE_AT3',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='bde_dbt_at3',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################

AIRFLOW_DATA = "/home/airflow/gcs/data"
NSW_LGA = AIRFLOW_DATA+"/NSW_LGA/"
CENSUS_SUBURB = AIRFLOW_DATA+"/Census LGA/"
LISTINGS = AIRFLOW_DATA+"/listings/"

# importing lga_code
def import_date_func_lga_code(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    df_code = pd.read_csv(NSW_LGA+'NSW_LGA_CODE.csv')
    return df_code


# importing lga_suburb
def import_date_func_lga_suburb(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    df_code = pd.read_csv(NSW_LGA+'NSW_LGA_SUBURB.csv')
    return df_code


# importing Census g02
def import_date_func_census_two(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    df_code = pd.read_csv(CENSUS_SUBURB+'2016Census_G02_NSW_LGA.csv')
    return df_code


# importing Census g01
def import_date_func_census_one(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    df_code = pd.read_csv(CENSUS_SUBURB+'2016Census_G01_NSW_LGA.csv')
    return df_code


# importing date csv
def import_date_func_house(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
        # Listing of filenames
    months = [
        '01_2021', '02_2021', '03_2021', '04_2021',
        '05_2020', '06_2020', '07_2020', '08_2020',
        '09_2020', '10_2020', '11_2020', '12_2020'
    ]

    # Reading all files into one DataFrame
    df_code = pd.concat([pd.read_csv(LISTINGS + month + '.csv') for month in months], ignore_index=True)
    
    # Defining default values
    fill_values = {
        'HOST_NAME': '',
        'HOST_SINCE': '',
        'HOST_IS_SUPERHOST': False,
        'HOST_NEIGHBOURHOOD': '',
        'REVIEW_SCORES_RATING': 0,
        'REVIEW_SCORES_ACCURACY': 0,
        'REVIEW_SCORES_CLEANLINESS': 0,
        'REVIEW_SCORES_CHECKIN': 0,
        'REVIEW_SCORES_COMMUNICATION': 0,
        'REVIEW_SCORES_VALUE': 0
    }
    
    # Fill NA values
    df_code.fillna(fill_values, inplace=True)

    return df_code
# ------------------------------------------------------

def insert_data_func_lge_code(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    insert_df = ti.xcom_pull(task_ids=f'import_date_func_lga_code')
    if len(insert_df) > 0:
        col_names = ['LGA_CODE','LGA_NAME']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.nsw_lga_code (lga_code, lga_name)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None    
# ------------------------------------------------------

def insert_data_func_suburb_code(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    insert_df = ti.xcom_pull(task_ids=f'import_date_func_lga_suburb')
    if len(insert_df) > 0:
        col_names = ['LGA_NAME', 'SUBURB_NAME']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.nsw_lga_suburb (lga_name, suburb_name)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None 
# ------------------------------------------------------

def insert_data_func_census_two(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    insert_df = ti.xcom_pull(task_ids=f'import_date_func_census_two')
    if len(insert_df) > 0:
        col_names = ['LGA_CODE_2016', 'Median_age_persons', 'Median_mortgage_repay_monthly',
       'Median_tot_prsnl_inc_weekly', 'Median_rent_weekly',
       'Median_tot_fam_inc_weekly', 'Average_num_psns_per_bedroom',
       'Median_tot_hhd_inc_weekly', 'Average_household_size']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.census_two (LGA_CODE_2016,Median_age_persons,Median_mortgage_repay_monthly,Median_tot_prsnl_inc_weekly,Median_rent_weekly,Median_tot_fam_inc_weekly,Average_num_psns_per_bedroom,Median_tot_hhd_inc_weekly,Average_household_size)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None
# ------------------------------------------------------

def insert_data_func_census_one(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    insert_df = ti.xcom_pull(task_ids=f'import_date_func_census_one')
    if len(insert_df) > 0:
        col_names = ['LGA_CODE_2016','Tot_P_M','Tot_P_F','Tot_P_P','Age_0_4_yr_M','Age_0_4_yr_F',
                     'Age_0_4_yr_P','Age_5_14_yr_M','Age_5_14_yr_F','Age_5_14_yr_P','Age_15_19_yr_M',
                     'Age_15_19_yr_F','Age_15_19_yr_P','Age_20_24_yr_M','Age_20_24_yr_F','Age_20_24_yr_P',
                     'Age_25_34_yr_M','Age_25_34_yr_F','Age_25_34_yr_P','Age_35_44_yr_M','Age_35_44_yr_F',
                     'Age_35_44_yr_P','Age_45_54_yr_M','Age_45_54_yr_F','Age_45_54_yr_P','Age_55_64_yr_M',
                     'Age_55_64_yr_F','Age_55_64_yr_P','Age_65_74_yr_M','Age_65_74_yr_F','Age_65_74_yr_P',
                     'Age_75_84_yr_M','Age_75_84_yr_F','Age_75_84_yr_P','Age_85ov_M','Age_85ov_F','Age_85ov_P',
                     'Counted_Census_Night_home_M','Counted_Census_Night_home_F','Counted_Census_Night_home_P',
                     'Count_Census_Nt_Ewhere_Aust_M','Count_Census_Nt_Ewhere_Aust_F','Count_Census_Nt_Ewhere_Aust_P',
                     'Indigenous_psns_Aboriginal_M','Indigenous_psns_Aboriginal_F','Indigenous_psns_Aboriginal_P',
                     'Indig_psns_Torres_Strait_Is_M','Indig_psns_Torres_Strait_Is_F','Indig_psns_Torres_Strait_Is_P',
                     'Indig_Bth_Abor_Torres_St_Is_M','Indig_Bth_Abor_Torres_St_Is_F','Indig_Bth_Abor_Torres_St_Is_P',
                     'Indigenous_P_Tot_M','Indigenous_P_Tot_F','Indigenous_P_Tot_P','Birthplace_Australia_M',
                     'Birthplace_Australia_F','Birthplace_Australia_P','Birthplace_Elsewhere_M','Birthplace_Elsewhere_F',
                     'Birthplace_Elsewhere_P','Lang_spoken_home_Eng_only_M','Lang_spoken_home_Eng_only_F',
                     'Lang_spoken_home_Eng_only_P','Lang_spoken_home_Oth_Lang_M','Lang_spoken_home_Oth_Lang_F',
                     'Lang_spoken_home_Oth_Lang_P','Australian_citizen_M','Australian_citizen_F','Australian_citizen_P',
                     'Age_psns_att_educ_inst_0_4_M','Age_psns_att_educ_inst_0_4_F','Age_psns_att_educ_inst_0_4_P',
                     'Age_psns_att_educ_inst_5_14_M','Age_psns_att_educ_inst_5_14_F','Age_psns_att_educ_inst_5_14_P',
                     'Age_psns_att_edu_inst_15_19_M','Age_psns_att_edu_inst_15_19_F','Age_psns_att_edu_inst_15_19_P',
                     'Age_psns_att_edu_inst_20_24_M','Age_psns_att_edu_inst_20_24_F','Age_psns_att_edu_inst_20_24_P',
                     'Age_psns_att_edu_inst_25_ov_M','Age_psns_att_edu_inst_25_ov_F','Age_psns_att_edu_inst_25_ov_P',
                     'High_yr_schl_comp_Yr_12_eq_M','High_yr_schl_comp_Yr_12_eq_F','High_yr_schl_comp_Yr_12_eq_P',
                     'High_yr_schl_comp_Yr_11_eq_M','High_yr_schl_comp_Yr_11_eq_F','High_yr_schl_comp_Yr_11_eq_P',
                     'High_yr_schl_comp_Yr_10_eq_M','High_yr_schl_comp_Yr_10_eq_F','High_yr_schl_comp_Yr_10_eq_P',
                     'High_yr_schl_comp_Yr_9_eq_M','High_yr_schl_comp_Yr_9_eq_F','High_yr_schl_comp_Yr_9_eq_P',
                     'High_yr_schl_comp_Yr_8_belw_M','High_yr_schl_comp_Yr_8_belw_F','High_yr_schl_comp_Yr_8_belw_P',
                     'High_yr_schl_comp_D_n_g_sch_M','High_yr_schl_comp_D_n_g_sch_F','High_yr_schl_comp_D_n_g_sch_P',
                     'Count_psns_occ_priv_dwgs_M','Count_psns_occ_priv_dwgs_F','Count_psns_occ_priv_dwgs_P',
                     'Count_Persons_other_dwgs_M','Count_Persons_other_dwgs_F','Count_Persons_other_dwgs_P' ]


        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.census_one(LGA_CODE_2016,Tot_P_M,Tot_P_F,Tot_P_P,Age_0_4_yr_M,Age_0_4_yr_F,Age_0_4_yr_P,Age_5_14_yr_M,Age_5_14_yr_F,Age_5_14_yr_P,Age_15_19_yr_M,Age_15_19_yr_F,Age_15_19_yr_P,Age_20_24_yr_M,Age_20_24_yr_F,Age_20_24_yr_P,Age_25_34_yr_M,Age_25_34_yr_F,Age_25_34_yr_P,Age_35_44_yr_M,Age_35_44_yr_F,Age_35_44_yr_P,Age_45_54_yr_M,Age_45_54_yr_F,Age_45_54_yr_P,Age_55_64_yr_M,Age_55_64_yr_F,Age_55_64_yr_P,Age_65_74_yr_M,Age_65_74_yr_F,Age_65_74_yr_P,Age_75_84_yr_M,Age_75_84_yr_F,Age_75_84_yr_P,Age_85ov_M,Age_85ov_F,Age_85ov_P,Counted_Census_Night_home_M,Counted_Census_Night_home_F,Counted_Census_Night_home_P,Count_Census_Nt_Ewhere_Aust_M,Count_Census_Nt_Ewhere_Aust_F,Count_Census_Nt_Ewhere_Aust_P,Indigenous_psns_Aboriginal_M,Indigenous_psns_Aboriginal_F,Indigenous_psns_Aboriginal_P,Indig_psns_Torres_Strait_Is_M,Indig_psns_Torres_Strait_Is_F,Indig_psns_Torres_Strait_Is_P,Indig_Bth_Abor_Torres_St_Is_M,Indig_Bth_Abor_Torres_St_Is_F,Indig_Bth_Abor_Torres_St_Is_P,Indigenous_P_Tot_M,Indigenous_P_Tot_F,Indigenous_P_Tot_P,Birthplace_Australia_M,Birthplace_Australia_F,Birthplace_Australia_P,Birthplace_Elsewhere_M,Birthplace_Elsewhere_F,Birthplace_Elsewhere_P,Lang_spoken_home_Eng_only_M,Lang_spoken_home_Eng_only_F,Lang_spoken_home_Eng_only_P,Lang_spoken_home_Oth_Lang_M,Lang_spoken_home_Oth_Lang_F,Lang_spoken_home_Oth_Lang_P,Australian_citizen_M,Australian_citizen_F,Australian_citizen_P,Age_psns_att_educ_inst_0_4_M,Age_psns_att_educ_inst_0_4_F,Age_psns_att_educ_inst_0_4_P,Age_psns_att_educ_inst_5_14_M,Age_psns_att_educ_inst_5_14_F,Age_psns_att_educ_inst_5_14_P,Age_psns_att_edu_inst_15_19_M,Age_psns_att_edu_inst_15_19_F,Age_psns_att_edu_inst_15_19_P,Age_psns_att_edu_inst_20_24_M,Age_psns_att_edu_inst_20_24_F,Age_psns_att_edu_inst_20_24_P,Age_psns_att_edu_inst_25_ov_M,Age_psns_att_edu_inst_25_ov_F,Age_psns_att_edu_inst_25_ov_P,High_yr_schl_comp_Yr_12_eq_M,High_yr_schl_comp_Yr_12_eq_F,High_yr_schl_comp_Yr_12_eq_P,High_yr_schl_comp_Yr_11_eq_M,High_yr_schl_comp_Yr_11_eq_F,High_yr_schl_comp_Yr_11_eq_P,High_yr_schl_comp_Yr_10_eq_M,High_yr_schl_comp_Yr_10_eq_F,High_yr_schl_comp_Yr_10_eq_P,High_yr_schl_comp_Yr_9_eq_M,High_yr_schl_comp_Yr_9_eq_F,High_yr_schl_comp_Yr_9_eq_P,High_yr_schl_comp_Yr_8_belw_M,High_yr_schl_comp_Yr_8_belw_F,High_yr_schl_comp_Yr_8_belw_P,High_yr_schl_comp_D_n_g_sch_M,High_yr_schl_comp_D_n_g_sch_F,High_yr_schl_comp_D_n_g_sch_P,Count_psns_occ_priv_dwgs_M,Count_psns_occ_priv_dwgs_F,Count_psns_occ_priv_dwgs_P,Count_Persons_other_dwgs_M,Count_Persons_other_dwgs_F,Count_Persons_other_dwgs_P)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None
# ------------------------------------------------------

def insert_data_func_house(**kwargs):
    # Establishing connection to PostgreSQL
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Fetching the dataframe from the previous task
    ti = kwargs['ti']
    insert_df = ti.xcom_pull(task_ids=f'import_date_func_house')

    if len(insert_df) > 0:
        col_names = [
            'LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME',
            'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD',
            'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 'ROOM_TYPE', 'ACCOMMODATES',
            'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30', 'NUMBER_OF_REVIEWS',
            'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY',
            'REVIEW_SCORES_CLEANLINESS', 'REVIEW_SCORES_CHECKIN',
            'REVIEW_SCORES_COMMUNICATION', 'REVIEW_SCORES_VALUE'
        ]

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
            INSERT INTO raw.house (
                LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME,
                HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD,
                LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES,
                PRICE, HAS_AVAILABILITY, AVAILABILITY_30, NUMBER_OF_REVIEWS,
                REVIEW_SCORES_RATING, REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,
                REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION, REVIEW_SCORES_VALUE
            )
            VALUES %s
        """

        execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()

    return None


#########################################################
#
#   DAG Operator Setup
#
#########################################################

# Importing LGA_CODE csv
import_data_lga_code_task = PythonOperator(
    task_id='import_date_func_lga_code',
    python_callable=import_date_func_lga_code,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

create_psql_table_nsw_lga_code_task = PostgresOperator(
    task_id="create_psql_table_nsw_lga_code_task",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS raw.nsw_lga_code (
            id SERIAL PRIMARY KEY,
            lga_code        INT,
            lga_name        VARCHAR
            );
    """,
    dag=dag
)

insert_data_code_task = PythonOperator(
    task_id="insert_data_func_lge_code",
    python_callable=insert_data_func_lge_code,
    op_kwargs={},
    provide_context=True,
    dag=dag
)
# ------------------------------------------------------

# Importing LGA_SUBURB csv
import_data_lga_suburb_task = PythonOperator(
    task_id='import_date_func_lga_suburb',
    python_callable=import_date_func_lga_suburb,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

create_psql_table_nsw_lga_suburb_task = PostgresOperator(
    task_id="create_psql_table_nsw_lga_suburb_task",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS raw.nsw_lga_suburb (
            id SERIAL PRIMARY KEY,
            lga_name        VARCHAR,
            suburb_name     VARCHAR
            );
    """,
    dag=dag
)

insert_data_suburb_task = PythonOperator(
    task_id="insert_data_func_suburb_code",
    python_callable=insert_data_func_suburb_code,
    op_kwargs={},
    provide_context=True,
    dag=dag
)
# ------------------------------------------------------

# Importing 2016Census_G02_NSW_LGA

import_data_lga_census_two_task = PythonOperator(
    task_id='import_date_func_census_two',
    python_callable=import_date_func_census_two,
    op_kwargs={},
    provide_context=True,
    dag=dag
)


create_psql_table_census_two_task = PostgresOperator(
    task_id="create_psql_table_census_two_task",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS raw.census_two (
            id SERIAL PRIMARY KEY,
            LGA_CODE_2016                   VARCHAR,
            Median_age_persons              INTEGER,
            Median_mortgage_repay_monthly   INTEGER,
            Median_tot_prsnl_inc_weekly     INTEGER,
            Median_rent_weekly              INTEGER,
            Median_tot_fam_inc_weekly       INTEGER,
            Average_num_psns_per_bedroom    INTEGER,
            Median_tot_hhd_inc_weekly       INTEGER,
            Average_household_size          INTEGER
        );
    """,
    dag=dag
)

insert_data_census_two_task = PythonOperator(
    task_id="insert_data_func_census_two",
    python_callable=insert_data_func_census_two,
    op_kwargs={},
    provide_context=True,
    dag=dag
)
# ------------------------------------------------------

# Importing census_01
import_data_lga_census_one_task = PythonOperator(
    task_id='import_date_func_census_one',
    python_callable=import_date_func_census_one,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

create_psql_table_census_one_task = PostgresOperator(
    task_id="create_psql_table_census_one_task",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS raw.census_one (
            id SERIAL PRIMARY KEY,
            LGA_CODE_2016 VARCHAR,
            Tot_P_M INT,
            Tot_P_F INT,
            Tot_P_P INT,
            Age_0_4_yr_M INT,
            Age_0_4_yr_F INT,
            Age_0_4_yr_P INT,
            Age_5_14_yr_M INT,
            Age_5_14_yr_F INT,
            Age_5_14_yr_P INT,
            Age_15_19_yr_M INT,
            Age_15_19_yr_F INT,
            Age_15_19_yr_P INT,
            Age_20_24_yr_M INT,
            Age_20_24_yr_F INT,
            Age_20_24_yr_P INT,
            Age_25_34_yr_M INT,
            Age_25_34_yr_F INT,
            Age_25_34_yr_P INT,
            Age_35_44_yr_M INT,
            Age_35_44_yr_F INT,
            Age_35_44_yr_P INT,
            Age_45_54_yr_M INT,
            Age_45_54_yr_F INT,
            Age_45_54_yr_P INT,
            Age_55_64_yr_M INT,
            Age_55_64_yr_F INT,
            Age_55_64_yr_P INT,
            Age_65_74_yr_M INT,
            Age_65_74_yr_F INT,
            Age_65_74_yr_P INT,
            Age_75_84_yr_M INT,
            Age_75_84_yr_F INT,
            Age_75_84_yr_P INT,
            Age_85ov_M INT,
            Age_85ov_F INT,
            Age_85ov_P INT,
            Counted_Census_Night_home_M INT,
            Counted_Census_Night_home_F INT,
            Counted_Census_Night_home_P INT,
            Count_Census_Nt_Ewhere_Aust_M INT,
            Count_Census_Nt_Ewhere_Aust_F INT,
            Count_Census_Nt_Ewhere_Aust_P INT,
            Indigenous_psns_Aboriginal_M INT,
            Indigenous_psns_Aboriginal_F INT,
            Indigenous_psns_Aboriginal_P INT,
            Indig_psns_Torres_Strait_Is_M INT,
            Indig_psns_Torres_Strait_Is_F INT,
            Indig_psns_Torres_Strait_Is_P INT,
            Indig_Bth_Abor_Torres_St_Is_M INT,
            Indig_Bth_Abor_Torres_St_Is_F INT,
            Indig_Bth_Abor_Torres_St_Is_P INT,
            Indigenous_P_Tot_M INT,
            Indigenous_P_Tot_F INT,
            Indigenous_P_Tot_P INT,
            Birthplace_Australia_M INT,
            Birthplace_Australia_F INT,
            Birthplace_Australia_P INT,
            Birthplace_Elsewhere_M INT,
            Birthplace_Elsewhere_F INT,
            Birthplace_Elsewhere_P INT,
            Lang_spoken_home_Eng_only_M INT,
            Lang_spoken_home_Eng_only_F INT,
            Lang_spoken_home_Eng_only_P INT,
            Lang_spoken_home_Oth_Lang_M INT,
            Lang_spoken_home_Oth_Lang_F INT,
            Lang_spoken_home_Oth_Lang_P INT,
            Australian_citizen_M INT,
            Australian_citizen_F INT,
            Australian_citizen_P INT,
            Age_psns_att_educ_inst_0_4_M INT,
            Age_psns_att_educ_inst_0_4_F INT,
            Age_psns_att_educ_inst_0_4_P INT,
            Age_psns_att_educ_inst_5_14_M INT,
            Age_psns_att_educ_inst_5_14_F INT,
            Age_psns_att_educ_inst_5_14_P INT,
            Age_psns_att_edu_inst_15_19_M INT,
            Age_psns_att_edu_inst_15_19_F INT,
            Age_psns_att_edu_inst_15_19_P INT,
            Age_psns_att_edu_inst_20_24_M INT,
            Age_psns_att_edu_inst_20_24_F INT,
            Age_psns_att_edu_inst_20_24_P INT,
            Age_psns_att_edu_inst_25_ov_M INT,
            Age_psns_att_edu_inst_25_ov_F INT,
            Age_psns_att_edu_inst_25_ov_P INT,
            High_yr_schl_comp_Yr_12_eq_M INT,
            High_yr_schl_comp_Yr_12_eq_F INT,
            High_yr_schl_comp_Yr_12_eq_P INT,
            High_yr_schl_comp_Yr_11_eq_M INT,
            High_yr_schl_comp_Yr_11_eq_F INT,
            High_yr_schl_comp_Yr_11_eq_P INT,
            High_yr_schl_comp_Yr_10_eq_M INT,
            High_yr_schl_comp_Yr_10_eq_F INT,
            High_yr_schl_comp_Yr_10_eq_P INT,
            High_yr_schl_comp_Yr_9_eq_M INT,
            High_yr_schl_comp_Yr_9_eq_F INT,
            High_yr_schl_comp_Yr_9_eq_P INT,
            High_yr_schl_comp_Yr_8_belw_M INT,
            High_yr_schl_comp_Yr_8_belw_F INT,
            High_yr_schl_comp_Yr_8_belw_P INT,
            High_yr_schl_comp_D_n_g_sch_M INT,
            High_yr_schl_comp_D_n_g_sch_F INT,
            High_yr_schl_comp_D_n_g_sch_P INT,
            Count_psns_occ_priv_dwgs_M INT,
            Count_psns_occ_priv_dwgs_F INT,
            Count_psns_occ_priv_dwgs_P INT,
            Count_Persons_other_dwgs_M INT,
            Count_Persons_other_dwgs_F INT,
            Count_Persons_other_dwgs_P INT
        );
    """,
    dag=dag
)

insert_data_census_one_task = PythonOperator(
    task_id="insert_data_func_census_one",
    python_callable=insert_data_func_census_one,
    op_kwargs={},
    provide_context=True,
    dag=dag
)


# ------------------------------------------------------

# Task to import house data
import_data_house_task = PythonOperator(
    task_id='import_date_func_house',
    python_callable=import_date_func_house,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Task to create PostgreSQL table for houses
create_psql_table_house_task = PostgresOperator(
    task_id="create_psql_table_house_task",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS raw.house (
            id SERIAL PRIMARY KEY,
            LISTING_ID INT,
            SCRAPE_ID VARCHAR,
            SCRAPED_DATE VARCHAR,
            HOST_ID INT,
            HOST_NAME VARCHAR,
            HOST_SINCE VARCHAR,
            HOST_IS_SUPERHOST BOOLEAN,
            HOST_NEIGHBOURHOOD VARCHAR,
            LISTING_NEIGHBOURHOOD VARCHAR,
            PROPERTY_TYPE VARCHAR,
            ROOM_TYPE VARCHAR,
            ACCOMMODATES INT,
            PRICE INT,
            HAS_AVAILABILITY BOOLEAN,
            AVAILABILITY_30 INT,
            NUMBER_OF_REVIEWS INT,
            REVIEW_SCORES_RATING INT,
            REVIEW_SCORES_ACCURACY INT,
            REVIEW_SCORES_CLEANLINESS INT,
            REVIEW_SCORES_CHECKIN INT,
            REVIEW_SCORES_COMMUNICATION INT,
            REVIEW_SCORES_VALUE INT
        );
    """,
    dag=dag
)

# Task to insert house data into the table
insert_data_census_house_task = PythonOperator(
    task_id="insert_data_func_house",
    python_callable=insert_data_func_house,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Task dependencies
create_psql_table_nsw_lga_code_task >> import_data_lga_code_task >> insert_data_code_task
create_psql_table_nsw_lga_suburb_task >> import_data_lga_suburb_task >> insert_data_suburb_task
create_psql_table_census_two_task >> import_data_lga_census_two_task >> insert_data_census_two_task
create_psql_table_census_one_task >> import_data_lga_census_one_task >> insert_data_census_one_task
create_psql_table_house_task >> import_data_house_task >> insert_data_census_house_task

