# data_ingestion



import os, sys
os.popen("pip install google-cloud-secret-manager").read()
os.popen("pip install boto3").read()
from time import time
import logging
import configparser
from google.cloud import bigquery, storage
from datetime import date,timedelta,datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from pyspark.sql import SparkSession
import subprocess
from google.cloud import secretmanager
import google.auth
from google.auth import impersonated_credentials
import json
import pytz
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
import boto3
from pyspark.sql.functions import to_date
from pyspark.sql.functions import date_format
import random
import multiprocessing as mp
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.api_core.exceptions import Forbidden

logger = logging.getLogger("airflow.task")

# Read Arguments
table_id = sys.argv[1]
s3_source_bucket = sys.argv[2]
s3_source_path_pref = sys.argv[3]
refresh_type = sys.argv[4]
secret_value = sys.argv[5]
DAG_ID = sys.argv[6]
load_control_files_flag = sys.argv[7]

utc_now = datetime.utcnow()
est_tz = pytz.timezone('US/Eastern')
est_time = utc_now.replace(tzinfo=pytz.utc).astimezone(est_tz)
print('est_time', est_time)
# today = date.today()
today_formatted = est_time.strftime("%Y-%m-%d")
#today_formatted = "2024-02-06"

bucket_name = 'dao-chs-int-data-hcb-prod'

print("table_details variable is: " ,table_id)

project_name =  table_id.split('.')[0]
dataset_name = table_id.split('.')[1]
table_name = table_id.split('.')[2]
table_name_s3 = table_name.split("enc_")[1] if table_name.startswith("enc_") else table_name.split("encplus_")[1] if table_name.startswith("encplus_") else table_name

destination_prefix = f"s3_to_bq_adhoc/{dataset_name}/{today_formatted}/{table_name_s3}"

destination_gcs_uri = f"gs://{bucket_name}/s3_to_bq_adhoc/{dataset_name}/{today_formatted}/"
destination_gcs_archive_uri = f"gs://{bucket_name}/s3_to_bq_adhoc/{dataset_name}/archive/{today_formatted}/"


# # Py venv chunk
aws_key_id = json.loads(str(secret_value))['aws_key_id']
aws_secret_key = json.loads(str(secret_value))['aws_secret_key']

bq_client = bigquery.Client(project=project_name)
stts_msg = ""

## copy to locals
cwd = os.getcwd()
logger.info("current working dir: {}".format(cwd))
local_dir = cwd + f"/{DAG_ID}"
logger.info("local_dir: {}".format(local_dir))
Path(local_dir).mkdir(parents=True, exist_ok=True)

aws_uri_list = []



def copy_file_s3_to_gcs():
    try:
        t = time()
        
        if(refresh_type == "Monthly"):
            s3_client = boto3.client('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_secret_key)
            curr_mon = est_time.strftime("%Y%m")
            curr_mon = "202406"
            print("curr_mon",curr_mon)
            objects = s3_client.list_objects_v2(Bucket=s3_source_bucket, Prefix=s3_source_path_pref, Delimiter='/')
            print('object',objects)
            curre_mon_folder_name = ''
            for prefix in objects.get('CommonPrefixes', []):
                pref = prefix.get('Prefix')
                print('pref',pref)
                if curr_mon in pref:
                    curre_mon_folder_name = pref
                    break
            print("curr month folder name is ",curre_mon_folder_name)
            s3_source_path =  f"{curre_mon_folder_name}{table_name_s3}"
            print('s3_source_path',s3_source_path)
        else:
            s3_source_path = s3_source_path_pref
          
        # "s3_source_path":"komodo/inbound/monthly_commercial_claims/20231027/provider_summaries",
        aws_uri_list_new = [f"s3://{s3_source_bucket}/{s3_source_path}_dat/",f"s3://{s3_source_bucket}/{s3_source_path}_ctl/",f"s3://{s3_source_bucket}/{s3_source_path}_tst/"]
        aws_uri_list.extend(aws_uri_list_new)
        return
        for aws_uri in aws_uri_list:
            
            curr_folder_name = aws_uri.split("/")[-2]
            # S3 to local_dir
            os.popen("pip install awscli").read()        
            command_s3_to_local = f'AWS_ACCESS_KEY_ID={aws_key_id} AWS_SECRET_ACCESS_KEY={aws_secret_key} aws s3 cp {aws_uri} {local_dir}{curr_folder_name}/ --recursive --sse AES256 '
            os.popen(command_s3_to_local).read()
            
            #local to GCS
            command_local_to_gcs = f"gsutil -m cp -r {local_dir}{curr_folder_name}/* {destination_gcs_uri}{curr_folder_name}/ "
            os.popen(command_local_to_gcs).read()
            
            # Delete local files 
            command_delete_local = f"rm -r {local_dir}{curr_folder_name}"
            os.popen(command_delete_local).read()
            
            
        t = time()-t
        logger.info("File copy task completed. Time taken: {}".format(t))
    except Exception as e:
        job_monitoring('Failed',f"Unable to copy {curr_folder_name} files from {aws_uri} to {destination_gcs_uri}",'','',e) 
      

def gcs_to_bq():
    
    dat_file_foldername = aws_uri_list[0].split("/")[-2]
    ctl_file_foldername = aws_uri_list[1].split("/")[-2]
    tst_file_foldername = aws_uri_list[2].split("/")[-2]
    
    prt_flg=True
    #dat_file_foldername = "medical_service_lines_dat"
    #ctl_file_foldername = "medical_service_lines_ctl"
    #tst_file_foldername = "medical_service_lines_tst"
    
    partition_columns= {
        'anbc-hcb-prod.eds_srcapp_komodombrmdcd_hcb_prod.provider_summaries':'reactivation_date',
        'anbc-hcb-prod.eds_srcapp_komodombr_hcb_prod.provider_summaries':'reactivation_date',
        'anbc-hcb-prod.eds_srcapp_komodombr_hcb_prod.enc_provider_summaries':'reactivation_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.medical_headers':'claim_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.medical_service_lines':'DOS_Y_M',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.pharmacy':'date_of_service',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.enc_pharmacy':'date_of_service',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.prvdr_hcp':'reactivation_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.allowed_amounts_summaries':'s3_load_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.medical_headers_summaries':'s3_load_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.medical_service_lines_summaries':'s3_load_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.npi_hco_summaries':'s3_load_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.patient_summaries':'s3_load_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.payer_summaries':'s3_load_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.pharmacy_summaries':'s3_load_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.prvdr_hco_summaries':'s3_load_date',
        'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.prvdr_hcp_summaries':'s3_load_date'

    }
    
    
    
    

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=f"s3_to_bq_adhoc/{dataset_name}/{today_formatted}/{dat_file_foldername}/")
    truncate_table=truncate_table_ctl=archive_tst_onefile=True
    create_table_if_not_exist = create_table_if_not_exist_ctl = True
    archive_tst_onefile=False
    try:
        t = time()
        file_count = 0
        stts_msg =  ""
        for blob in blobs:
            print("blob name is",blob.name)
            gcs_data_file_with_path = blob.name
            file_name_without_extention = (gcs_data_file_with_path.split("/")[-1]).split(".")[0]
            dat_file_uri = f"{destination_gcs_uri}{dat_file_foldername}/{file_name_without_extention}.dat.gz"
            ctl_file_uri = f"{destination_gcs_uri}{ctl_file_foldername}/{file_name_without_extention}.ctl"
            tst_file_uri = f"{destination_gcs_uri}{tst_file_foldername}/{file_name_without_extention}.tst"
            
            tst_filename_onefile = (gcs_data_file_with_path.split("/")[-2]).replace("_dat","")
            tst_file_uri_onefile = f"{destination_gcs_uri}{tst_file_foldername}/{tst_filename_onefile}.tst"
            
            # Check for tst file if respective tst file each dat file exist or not 
            bucket = storage_client.get_bucket(bucket_name)
            blob_tst = bucket.blob('/'.join(tst_file_uri.split("/")[3:])) 
            tst_file_exists = blob_tst.exists()
            try:
                if tst_file_exists:
                    schema_lst = spark.read.text(tst_file_uri).rdd.map(lambda x: (x.value.replace('"','') + '_' )if '"' in x.value else x.value).collect()
                    print("schema list is",schema_lst)
                elif archive_tst_onefile:
                    schema_lst = spark.read.text(tst_file_uri_onefile).rdd.map(lambda x: (x.value.replace('"','') + '_' )if '"' in x.value else x.value).collect()
                    print("schema list is",schema_lst)
                else:
                    if prt_flg: print(f"scehma list passing -----{tst_file_exists}")
                    pass
            except Exception as e:
                print(f"A error occurred: {e}")
                
            if create_table_if_not_exist:
                column_sql = ",\n".join([f"{col_name} STRING" for col_name in schema_lst])
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_id} ({column_sql}) ;"
                qry_create_table_job = bq_client.query(create_table_sql)
                qry_create_table_job.result()
                bq_table_columns = get_bq_table_columns()
                create_table_if_not_exist = False
            
            # Validate number columns present in Source file Vs BQ table
            try:
                if((len(bq_table_columns)) == len(schema_lst)):
                    logger.info(f"Bq and tst file:{file_name_without_extention} schema size is matching ")
                else:
                    logger.info(f"Bq and tst file:{file_name_without_extention} schema size is not matching, please check with source team for schema changes in the source file ")
                    stts_msg = stts_msg + " | schema size is not matching for file:" + file_name_without_extention
            except Exception as e:
                print(f"An error occurred:: {e}")
                
            # Read data from dat file to Spark dataframe along with tst file's schema
            if prt_flg: print("Reading data from dat file to Spark dataframe along with tst file's schema")
            schema_fields = []
            for scma in schema_lst :
                schema_fields.append(StructField(scma, StringType(), True))
            schema = StructType(schema_fields)   
            
            df_gz = spark.read.csv(
                dat_file_uri,
                schema=schema,  
                sep="|"
            )
            
            # Validate count from data file and count mentioned in control file 
            # get data file count from control file 
            # bucket = storage_client.get_bucket(bucket_name)
            blob_ctl = bucket.get_blob('/'.join(ctl_file_uri.split("/")[3:]))            
            control_file_data = blob_ctl.download_as_text()
            count_ctl_file = int(control_file_data.split("|")[1])
            
            count_dat_file = int(df_gz.count())
            try:
                if(count_ctl_file==count_dat_file):
                    file_count = file_count + count_dat_file
                    logger.info(f"data file count: {count_dat_file} and count mentioned in control file: {count_ctl_file} is matching ")
                else:
                    logger.info(f"data file count: {count_dat_file} and count mentioned in control file: {count_ctl_file} is not matching, please check with source team ") 
                    stts_msg = stts_msg + " | data_file count and control_file counts are not matching for file:" + file_name_without_extention
                       
            except Exception as e:
                print(f"An error occurred {e}")
                
            # Add Audit columns for Load time and source file name , Select only required columns from dataframe as per BQ table
            try:
                df_gz = df_gz.withColumn("bq_load_dts",lit(today_formatted)).withColumn("bq_source_file_name",lit(dat_file_uri.split("/")[-1]))
                current_table = f"{project_name}.{dataset_name}.{table_name}"
                print('current table :', current_table)
                
                if current_table == "anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.medical_service_lines":
                    df_gz = df_gz.withColumn('DOS_Y_M', date_format(df_gz['date_of_service'], 'yyyy-MM'))
    
                df_gz = df_gz.select(*bq_table_columns)
    
                
                for colmn in df_gz.columns:
                    df_gz = df_gz.withColumn(colmn,df_gz[colmn].cast(StringType()))
                    #print('again df is', df_gz)
                    
                if current_table in partition_columns:
                    partition_col = partition_columns[current_table]
                    print('partition column is ', partition_col)
                    df_gz = df_gz.withColumn(partition_col, to_date(df_gz[partition_col], 'yyyy-MM-dd'))
                    print('df is', df_gz)
                    
            except Exception as e:
                print(f"An error Occurred: {e}")
            
            try:
                if truncate_table:
                    df_gz.write.format("bigquery").option("table", table_id).option("temporaryGcsBucket", "dao-chs-int-data-hcb-prod").mode("overwrite").save()
                    truncate_table=False
                else:
                    df_gz.write.format("bigquery").option("table", table_id).option("temporaryGcsBucket", "dao-chs-int-data-hcb-prod").mode("append").save()
            except Exception as e:
                print(f"An Error occurred: {e}")
            
            # Load Control Files if needed 
            try:
                if prt_flg: print("Loading ctl files..")
                if(load_control_files_flag=="yes"):
                    control_table_id = f"{table_id}_summaries"
                    if create_table_if_not_exist_ctl:
                        qry_create_table_job_ctl = bq_client.query(f"CREATE TABLE IF NOT EXISTS {control_table_id} (file_name STRING, num_rows STRING, s3_load_date STRING)")
                        qry_create_table_job_ctl.result()
                        create_table_if_not_exist_ctl = False
    
                    schema_ctl = StructType([StructField("file_name", StringType(), True),StructField("num_rows", StringType(), True),StructField("s3_load_date", StringType(), True)])   
                    df_gz_ctl = spark.read.csv(ctl_file_uri,schema=schema_ctl,sep="|")
    
                    for colmn in df_gz_ctl.columns:
                        df_gz_ctl = df_gz_ctl.withColumn(colmn,df_gz_ctl[colmn].cast(StringType()))
                    
                    current_table = f"{project_name}.{dataset_name}.{table_name}_summaries"
                    print('current table in ctl is', current_table)
                    if current_table in partition_columns:
                        partition_col = partition_columns[current_table]
                        df_gz_ctl = df_gz_ctl.withColumn(partition_col, to_date(df_gz_ctl[partition_col], 'yyyy-MM-dd')) 
                        
                    if truncate_table_ctl:
                        df_gz_ctl.write.format("bigquery").option("table", control_table_id).option("temporaryGcsBucket", "dao-chs-int-data-hcb-prod").mode("overwrite").save()
                        truncate_table_ctl=False
                    else:
                        df_gz_ctl.write.format("bigquery").option("table", control_table_id).option("temporaryGcsBucket", "dao-chs-int-data-hcb-prod").mode("append").save()
            except Exception as e:
                print(f"AN error occurred: {e}")
             
            # Archive files once they are loaded to Bigquery 
            dat_file_uri_archive = f"{destination_gcs_archive_uri}{dat_file_foldername}/"
            ctl_file_uri_archive = f"{destination_gcs_archive_uri}{ctl_file_foldername}/"
            command_archive_dat = f'gsutil mv {dat_file_uri} {dat_file_uri_archive}'
            command_archive_ctl = f'gsutil mv {ctl_file_uri} {ctl_file_uri_archive}'
            os.popen(command_archive_dat).read()
            os.popen(command_archive_ctl).read()
            
            tst_file_uri_archive = f"{destination_gcs_archive_uri}{tst_file_foldername}/"            
            if tst_file_exists:
                command_archive_tst = f'gsutil mv {tst_file_uri} {tst_file_uri_archive}'
                os.popen(command_archive_tst).read()
            elif archive_tst_onefile:
                command_archive_tst_onefile = f'gsutil mv {tst_file_uri_onefile} {tst_file_uri_archive}'
                os.popen(command_archive_tst_onefile).read()
                archive_tst_onefile = False
            else:
                pass
        
        spark.stop()  
        
        bq_count_qry = f"SELECT COUNT(*) FROM {table_id} "
        qry_count_job = bq_client.query(bq_count_qry)
        result = qry_count_job.result()
        for val in result:
            bq_count  = int(val[0])
    
        t = time()-t
        if prt_flg: print("File Load from GCS to BQ task completed. Time taken: {}".format(t))
        logger.info("File Load from GCS to BQ task completed. Time taken: {}".format(t))
        
        job_monitoring('Success',f"Data loaded succesfully from {destination_gcs_uri} to {project_name}.{dataset_name}.{table_name} | {stts_msg} ",file_count,bq_count,'') 
    except Exception as e:
        job_monitoring('Failed',f"Unable to load data from  {destination_gcs_uri} to {project_name}.{dataset_name}.{table_name} | {stts_msg}  ",'','',e)      

def list_blobs_with_prefix(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    return [blob.name for blob in bucket.list_blobs(prefix=prefix)]


def file_map_generator():
    
    file_map = []

    dat_files = list_blobs_with_prefix(bucket_name, prefix=f"{destination_prefix}_dat/")
    ctl_files = list_blobs_with_prefix(bucket_name, prefix=f"{destination_prefix}_ctl/")
    tst_files = list_blobs_with_prefix(bucket_name, prefix=f"{destination_prefix}_tst/")

    #check 1
    if len(dat_files) == len(ctl_files):
        print(f"Number of DAT files ({dat_files}) and CTL files ({ctl_files}) are same. ")
        

    #create file map for dat, clt and tst files (list of tuples)
    for dat_file in sorted(dat_files, reverse=True):
        base_name = os.path.basename(dat_file).replace(".dat.gz", "")
        ctl_file = f"{destination_prefix}_ctl/{base_name}.ctl"
        #check if only one tst file is present in _tst folder
        if f"{destination_prefix}_tst/{table_name_s3}.tst" in tst_files and len(tst_files)==1:
            print("Only one test file present. Setting archive_tst_onefile flag ")
            tst_file = f"{destination_prefix}_tst/{table_name_s3}.tst"
            archive_tst_onefile=True
        else: 
            tst_file = f"{destination_prefix}_tst/{base_name}.tst"

        #check if the 
        if ctl_file in ctl_files and tst_file in tst_files:
            file_map.append((dat_file,ctl_file, tst_file))


    return file_map

processed_files = set()
def gcs_to_bq_mp(file_set):
        
    retry_count = 5
    for attempt in range(retry_count):
        if file_set in processed_files:
            logging.info(f"Skipping already processed file: {file_set}")
            return
        print("gcs to bq running .....")
        dat_file,ctl_file,tst_file = file_set


        dat_file_foldername = aws_uri_list[0].split("/")[-2]
        ctl_file_foldername = aws_uri_list[1].split("/")[-2]
        tst_file_foldername = aws_uri_list[2].split("/")[-2]
        
        prt_flg=True
        
        partition_columns= {
            'anbc-hcb-prod.eds_srcapp_komodombrmdcd_hcb_prod.provider_summaries':'reactivation_date',
            'anbc-hcb-prod.eds_srcapp_komodombr_hcb_prod.provider_summaries':'reactivation_date',
            'anbc-hcb-prod.eds_srcapp_komodombr_hcb_prod.enc_provider_summaries':'reactivation_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.medical_headers':'claim_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.medical_service_lines':'DOS_Y_M',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.pharmacy':'date_of_service',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.enc_pharmacy':'date_of_service',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.prvdr_hcp':'reactivation_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.allowed_amounts_summaries':'s3_load_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.medical_headers_summaries':'s3_load_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.medical_service_lines_summaries':'s3_load_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.npi_hco_summaries':'s3_load_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.patient_summaries':'s3_load_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.payer_summaries':'s3_load_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.pharmacy_summaries':'s3_load_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.prvdr_hco_summaries':'s3_load_date',
            'anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.prvdr_hcp_summaries':'s3_load_date'

        }
        
        
        # # Using Spark Session
        # spark = SparkSession.builder.appName("s3tobq") \
        # .config("spark.sql.shuffle.partitions", 200) \
        # .config("spark.shuffle.io.maxRetries",3)\
        # .config("spark.task.maxFailures",20)\
        # .config("spark.SparkContext.setLogLevel","DEBUG")\
        # .config("spark.executor.memory.", "100g") \
        # .config("spark.driver.memory", "50g") \
        # .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.0.jar') \
        # .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery_2.12:0.22.0") \
        # .config("spark.executor.cores",16)\
        # .config("spark.executor.instances",32)\
        # .config("spark.dynamicAllocation.enabled","true")\
        # .config("spark.dynamicAllocation.minExecutors",5)\
        # .config("spark.dynamicAllocation.maxExecutors",1000)\
        # .config("spark.dynamicAllocation.initialExecutors",10)\
        # .config("spark.executor.memoryOverhead","4g")\
        # .config("spark.default.parallelism","2400")\
        # .config("spark.speculation","True")\
        # .config("spark.hadoop.io.compression.codecs","org.apache.hadoop.io.compress.GzipCodec")\
        # .config("spark.kryoserializer.buffer.max","1024m")\
        # .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")\
        # .getOrCreate()


        storage_client = storage.Client()
        truncate_table=truncate_table_ctl=archive_tst_onefile=False
        create_table_if_not_exist = create_table_if_not_exist_ctl = True
        archive_tst_onefile=False
        try:
            t = time()
            file_count = 0
            stts_msg =  ""
            
            dat_file_uri = f"gs://{bucket_name}/" + dat_file
            ctl_file_uri = f"gs://{bucket_name}/" + ctl_file
            tst_file_uri = f"gs://{bucket_name}/" + tst_file


            file_name_without_extention = (dat_file_uri.split("/")[-1]).split(".")[0]#ex: medical_service_lines_9_7_0

            tst_filename_onefile = (dat_file_uri.split("/")[-2]).replace("_dat","")#ex: medical_service_lines
            tst_file_uri_onefile = f"{destination_gcs_uri}{tst_file_foldername}/{tst_filename_onefile}.tst"
            
            # Check for tst file if respective tst file each dat file exist or not 
            bucket = storage_client.get_bucket(bucket_name)
            blob_tst = bucket.blob('/'.join(tst_file_uri.split("/")[3:])) 
            tst_file_exists = blob_tst.exists()
            try:
                if tst_file_exists:
                    schema_lst = spark.read.text(tst_file_uri).rdd.map(lambda x: (x.value.replace('"','') + '_' )if '"' in x.value else x.value).collect()
                    print("schema list is",schema_lst)
                elif archive_tst_onefile:
                    schema_lst = spark.read.text(tst_file_uri_onefile).rdd.map(lambda x: (x.value.replace('"','') + '_' )if '"' in x.value else x.value).collect()
                    print("schema list is",schema_lst)
                else:
                    if prt_flg: print(f"scehma list passing -----{tst_file_exists}")
                    pass
            except Exception as e:
                print(f"A error occurred: {e}")
                
            if create_table_if_not_exist:
                column_sql = ",\n".join([f"{col_name} STRING" for col_name in schema_lst])
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_id} ({column_sql}) ;"
                qry_create_table_job = bq_client.query(create_table_sql)
                qry_create_table_job.result()
                bq_table_columns = get_bq_table_columns()
                create_table_if_not_exist = False
            
            # Validate number columns present in Source file Vs BQ table
            try:
                if((len(bq_table_columns)) == len(schema_lst)):
                    logger.info(f"Bq and tst file:{file_name_without_extention} schema size is matching ")
                else:
                    logger.info(f"Bq and tst file:{file_name_without_extention} schema size is not matching, please check with source team for schema changes in the source file ")
                    stts_msg = stts_msg + " | schema size is not matching for file:" + file_name_without_extention
            except Exception as e:
                print(f"An error occurred:: {e}")
                
            # Read data from dat file to Spark dataframe along with tst file's schema
            if prt_flg: print("Reading data from dat file to Spark dataframe along with tst file's schema")
            schema_fields = []
            for scma in schema_lst :
                schema_fields.append(StructField(scma, StringType(), True))
            schema = StructType(schema_fields)   
            
            df_gz = spark.read.csv(
                dat_file_uri,
                schema=schema,  
                sep="|"
            )
            
            # Validate count from data file and count mentioned in control file 
            # get data file count from control file 
            # bucket = storage_client.get_bucket(bucket_name)
            blob_ctl = bucket.get_blob('/'.join(ctl_file_uri.split("/")[3:]))            
            control_file_data = blob_ctl.download_as_text()
            count_ctl_file = int(control_file_data.split("|")[1])
            
            count_dat_file = int(df_gz.count())
            try:
                if(count_ctl_file==count_dat_file):
                    file_count = file_count + count_dat_file
                    logger.info(f"data file count: {count_dat_file} and count mentioned in control file: {count_ctl_file} is matching ")
                else:
                    logger.info(f"data file count: {count_dat_file} and count mentioned in control file: {count_ctl_file} is not matching, please check with source team ") 
                    stts_msg = stts_msg + " | data_file count and control_file counts are not matching for file:" + file_name_without_extention
                    
            except Exception as e:
                print(f"An error occurred {e}")
                
            # Add Audit columns for Load time and source file name , Select only required columns from dataframe as per BQ table
            try:
                df_gz = df_gz.withColumn("bq_load_dts",lit(today_formatted)).withColumn("bq_source_file_name",lit(dat_file_uri.split("/")[-1]))
                current_table = f"{project_name}.{dataset_name}.{table_name}"
                print('current table :', current_table)
                
                if current_table == "anbc-hcb-prod.eds_srcapp_komodononmbr_hcb_prod.medical_service_lines":
                    df_gz = df_gz.withColumn('DOS_Y_M', date_format(df_gz['date_of_service'], 'yyyy-MM'))

                df_gz = df_gz.select(*bq_table_columns)

                
                for colmn in df_gz.columns:
                    df_gz = df_gz.withColumn(colmn,df_gz[colmn].cast(StringType()))
                    #print('again df is', df_gz)
                    
                if current_table in partition_columns:
                    partition_col = partition_columns[current_table]
                    print('partition column is ', partition_col)
                    df_gz = df_gz.withColumn(partition_col, to_date(df_gz[partition_col], 'yyyy-MM-dd'))
                    print('df is', df_gz)
                    
            except Exception as e:
                print(f"An error Occurred: {e}")
            
            try:
                if truncate_table:
                    df_gz.write.format("bigquery").option("table", table_id).option("temporaryGcsBucket", "dao-chs-int-data-hcb-prod").mode("overwrite").save()
                    truncate_table=False
                else:
                    df_gz.write.format("bigquery").option("table", table_id).option("temporaryGcsBucket", "dao-chs-int-data-hcb-prod").mode("append").save()
            except Exception as e:
                print(f"An Error occurred: {e}")
            
            # Load Control Files if needed 
            try:
                if prt_flg: print("Loading ctl files..")
                if(load_control_files_flag=="yes"):
                    control_table_id = f"{table_id}_summaries"
                    if create_table_if_not_exist_ctl:
                        qry_create_table_job_ctl = bq_client.query(f"CREATE TABLE IF NOT EXISTS {control_table_id} (file_name STRING, num_rows STRING, s3_load_date STRING)")
                        qry_create_table_job_ctl.result()
                        create_table_if_not_exist_ctl = False

                    schema_ctl = StructType([StructField("file_name", StringType(), True),StructField("num_rows", StringType(), True),StructField("s3_load_date", StringType(), True)])   
                    df_gz_ctl = spark.read.csv(ctl_file_uri,schema=schema_ctl,sep="|")

                    for colmn in df_gz_ctl.columns:
                        df_gz_ctl = df_gz_ctl.withColumn(colmn,df_gz_ctl[colmn].cast(StringType()))
                    
                    current_table = f"{project_name}.{dataset_name}.{table_name}_summaries"
                    print('current table in ctl is', current_table)
                    if current_table in partition_columns:
                        partition_col = partition_columns[current_table]
                        df_gz_ctl = df_gz_ctl.withColumn(partition_col, to_date(df_gz_ctl[partition_col], 'yyyy-MM-dd')) 
                        
                    if truncate_table_ctl:
                        df_gz_ctl.write.format("bigquery").option("table", control_table_id).option("temporaryGcsBucket", "dao-chs-int-data-hcb-prod").mode("overwrite").save()
                        truncate_table_ctl=False
                    else:
                        df_gz_ctl.write.format("bigquery").option("table", control_table_id).option("temporaryGcsBucket", "dao-chs-int-data-hcb-prod").mode("append").save()
            except Exception as e:
                print(f"AN error occurred: {e}")
                
                # Archive files once they are loaded to Bigquery 
                dat_file_uri_archive = f"{destination_gcs_archive_uri}{dat_file_foldername}/"
                ctl_file_uri_archive = f"{destination_gcs_archive_uri}{ctl_file_foldername}/"
                command_archive_dat = f'gsutil mv {dat_file_uri} {dat_file_uri_archive}'
                command_archive_ctl = f'gsutil mv {ctl_file_uri} {ctl_file_uri_archive}'
                os.popen(command_archive_dat).read()
                os.popen(command_archive_ctl).read()
                
                tst_file_uri_archive = f"{destination_gcs_archive_uri}{tst_file_foldername}/"            
                if tst_file_exists:
                    command_archive_tst = f'gsutil mv {tst_file_uri} {tst_file_uri_archive}'
                    os.popen(command_archive_tst).read()
                elif archive_tst_onefile:
                    command_archive_tst_onefile = f'gsutil mv {tst_file_uri_onefile} {tst_file_uri_archive}'
                    os.popen(command_archive_tst_onefile).read()
                    archive_tst_onefile = False
                else:
                    pass
            
            # spark.stop()  
            
            bq_count_qry = f"SELECT COUNT(*) FROM {table_id} "
            qry_count_job = bq_client.query(bq_count_qry)
            result = qry_count_job.result()
            for val in result:
                bq_count  = int(val[0])
        
            t = time()-t
            if prt_flg: print("File Load from GCS to BQ task completed. Time taken: {}".format(t))
            logger.info("File Load from GCS to BQ task completed. Time taken: {}".format(t))
            
            job_monitoring('Success',f"Data loaded succesfully from {destination_gcs_uri} to {project_name}.{dataset_name}.{table_name} | {stts_msg} ",file_count,bq_count,'') 

            #Mark file has processed
            processed_files.add(file_set)
            break # Exit the retry loop if the operation was successful
        except Forbidden as e:
            #Check if the error is due to rate limit exceeded "403 Exceeded rate limits"
            if '403 Exceeded rate limits' in str(e):
                logging.error(f"Rate limit exceeded procsessing file {file_set} on attempt {attempt+1}: {e}")
                if attempt < retry_count-1:
                    
                    sleep_time = (2 ** attempt) + random.uniform(0, 1) #Exponential backoff with jitter
                    logging.info(f"Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                else:
                    logging.error(f"Failed to process {file_set} file after {retry_count} attempts due to rate limits")
            else:
                logging.error(f"Error processing file {file_set} on attempt {attempt+1}: {e}")
                break
     
        except Exception as e:
            job_monitoring('Failed',f"Unable to load data from  {destination_gcs_uri} to {project_name}.{dataset_name}.{table_name} | {stts_msg}  ",'','',e)
            logging.error(f"Unexpected error processing file {file_set[0]}")
            break

def get_bq_table_columns():
    dataset_ref = bq_client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    table = bq_client.get_table(table_ref)
    #last_modified = table.modified
    schema = table.schema
    bq_schema=[]
    for field in schema:
        column_name = field.name
        bq_schema.append(column_name)
    return bq_schema

def job_monitoring(job_status,status_message,file_count,bq_count,e):
    load_date = today_formatted
    task_start_time = est_time     # before extract           
    task_end_time = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(est_tz)      #after s3 copy
    task_duration = task_end_time - task_start_time
    sorce_s3_uri = aws_uri_list[0]
    #print(load_date,table_id,task_start_time,task_end_time,task_duration,data_file,schema_file,control_file,job_status,status_message,s3_target_bucket)    
    print(DAG_ID,load_date,table_id,task_start_time,task_end_time,task_duration,job_status,status_message,sorce_s3_uri,destination_gcs_uri,file_count,bq_count)    
    sql_qry = f"INSERT INTO `anbc-hcb-prod.dao_chs_int_hcb_prod.s3_to_bq_job_monitoring` (dag_id,load_date,table_id,task_start_time,task_end_time,task_duration,job_status,status_message,sorce_s3_uri,destination_gcs_uri,file_count,bq_count)  VALUES ('{DAG_ID}','{load_date}','{table_id}','{task_start_time}','{task_end_time}','{task_duration}','{job_status}','{status_message}','{sorce_s3_uri}','{destination_gcs_uri}','{file_count}','{bq_count}')"
    qry_job = bq_client.query(sql_qry)
    qry_job.result()

    if(e==''):
        pass
    else:
        raise Exception(f"{status_message} | Error message : {e}")       
  
if __name__ == "__main__":
    copy_file_s3_to_gcs()

    #generate file_map for MP
    file_map = file_map_generator()

    # Using Spark Session
    spark = SparkSession.builder.appName("s3tobq") \
    .config("spark.sql.shuffle.partitions", 200) \
    .config("spark.shuffle.io.maxRetries",3)\
    .config("spark.task.maxFailures",20)\
    .config("spark.SparkContext.setLogLevel","DEBUG")\
    .config("spark.executor.memory.", "100g") \
    .config("spark.driver.memory", "50g") \
    .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.0.jar') \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery_2.12:0.22.0") \
    .config("spark.executor.cores",16)\
    .config("spark.executor.instances",32)\
    .config("spark.dynamicAllocation.enabled","true")\
    .config("spark.dynamicAllocation.minExecutors",5)\
    .config("spark.dynamicAllocation.maxExecutors",1000)\
    .config("spark.dynamicAllocation.initialExecutors",10)\
    .config("spark.executor.memoryOverhead","4g")\
    .config("spark.default.parallelism","2400")\
    .config("spark.speculation","True")\
    .config("spark.hadoop.io.compression.codecs","org.apache.hadoop.io.compress.GzipCodec")\
    .config("spark.kryoserializer.buffer.max","1024m")\
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")\
    .getOrCreate()

    # gcs_to_bq()
    #Run gcs to bq in parallel using MP
    # print(f"MP CPU COUNT {mp.cpu_count()}")
    # with Pool(mp.cpu_count()) as pool:
    #     pool.map(gcs_to_bq_mp,file_map)

    #Using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(gcs_to_bq_mp, file_set): file_set for file_set in file_map}
        for future in as_completed(futures):
            file_set = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"File processing failed for {file_set}:{e}")

    #Stop spark session
    spark.stop()
