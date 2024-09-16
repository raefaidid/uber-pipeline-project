from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from google.cloud import bigquery


PROJECT_ID = "uber-data-analytics-435409"
DATASET_ID = "uber_data_analytics"



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def pull_raw_data_from_gcs(**context):

    # Fetch data from Google Cloud Storage
    df = pd.read_parquet(
        "https://storage.googleapis.com/uber-datalake-raef/yellow_tripdata_2016-03.parquet"
    )
    df = df.iloc[:100000, :]

    # Push the DataFrame to XCom as JSON
    context["ti"].xcom_push(key="raw_uber_data", value=df.to_json())


def create_dim_datetime_table(**context):

    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)

    # Convert date columns to datetime types
    if "tpep_pickup_datetime" in df.columns:
        df["tpep_pickup_datetime"] = pd.to_datetime(df.tpep_pickup_datetime)
    if "tpep_dropoff_datetime" in df.columns:
        df["tpep_dropoff_datetime"] = pd.to_datetime(df.tpep_dropoff_datetime)

    # Create dim_datetime table
    dim_datetime = (
        df[["tpep_pickup_datetime", "tpep_dropoff_datetime"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim_datetime = dim_datetime.assign(
        pickup_year=dim_datetime.tpep_pickup_datetime.dt.year,
        pickup_month=dim_datetime.tpep_pickup_datetime.dt.month,
        pickup_day=dim_datetime.tpep_pickup_datetime.dt.day,
        pickup_weekday=dim_datetime.tpep_pickup_datetime.dt.weekday,
        dropoff_year=dim_datetime.tpep_dropoff_datetime.dt.year,
        dropoff_month=dim_datetime.tpep_dropoff_datetime.dt.month,
        dropoff_day=dim_datetime.tpep_dropoff_datetime.dt.day,
        dropoff_weekday=dim_datetime.tpep_dropoff_datetime.dt.weekday,
        datetime_id=dim_datetime.index,
    )

    dim_datetime["tpep_pickup_datetime"] = dim_datetime.tpep_pickup_datetime.astype(
        "string"
    )
    dim_datetime["tpep_dropoff_datetime"] = dim_datetime.tpep_dropoff_datetime.astype(
        "string"
    )

    dim_datetime = dim_datetime[
        [
            "datetime_id",
            "tpep_pickup_datetime",
            "pickup_year",
            "pickup_month",
            "pickup_day",
            "pickup_weekday",
            "tpep_dropoff_datetime",
            "dropoff_year",
            "dropoff_month",
            "dropoff_day",
            "dropoff_weekday",
        ]
    ]

    context["ti"].xcom_push(key="dim_datetime_tbl", value=dim_datetime.to_json())


def push_dim_datetime_tbl(**context):
    
    TABLE_NAME = 'datetimes'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="dim_datetime_tbl")
    
    df = pd.read_json(df_json)

    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )
   
    
def create_dim_rate_code_tbl(**context):
    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)

    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    rate_code_dim = rate_code_dim.assign(
        rate_code_id = rate_code_dim.index,\
        rate_code_name = rate_code_dim['RatecodeID'].map(rate_code_type)
    )

    rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID', 'rate_code_name']]
    
    context["ti"].xcom_push(key="rate_code_dim_tbl", value=rate_code_dim.to_json())
    

def push_dim_rate_code_tbl(**context):
    
    TABLE_NAME = 'rate_codes'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="rate_code_dim_tbl")
    
    df = pd.read_json(df_json)
    
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )


def create_dim_payment_type_tbl(**context):
    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)

    payment_types = {
        1 : "Credit Card",
        2 : 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5 : 'Unknown',
        6 : 'Voided trip'
    }

    payment_type_dim = payment_type_dim.assign(
        payment_type_id = payment_type_dim.index,\
        payment_type_name = payment_type_dim.payment_type.map(payment_types)
        
    )

    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]
    
    context["ti"].xcom_push(key="payment_type_dim_tbl", value=payment_type_dim.to_json())
    


    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)

    payment_types = {
        1 : "Credit Card",
        2 : 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5 : 'Unknown',
        6 : 'Voided trip'
    }

    payment_type_dim = payment_type_dim.assign(
        payment_type_id = payment_type_dim.index,\
        payment_type_name = payment_type_dim.payment_type.map(payment_types)
        
    )

    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]
    
    context["ti"].xcom_push(key="payment_type_dim_tbl", value=payment_type_dim.to_json())

    
def push_dim_payment_type_tbl(**context):
    
    TABLE_NAME = 'payment_types'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="payment_type_dim_tbl")
    
    df = pd.read_json(df_json)
    
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )
        

def create_dim_dropoff_location_tbl(**context):
    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    df_taxi_zones = pd.read_csv('https://storage.googleapis.com/uber-datalake-raef/taxi_zones.csv', usecols=['zone', 'LocationID','borough'])

    df_taxi_zones_lookup = df_taxi_zones.drop_duplicates()
    df_taxi_zones_lookup = df_taxi_zones_lookup[['LocationID', 'borough','zone']]

    dropoff_location_dim = df[['DOLocationID']].drop_duplicates().reset_index(drop=True)

    dropoff_location_dim = dropoff_location_dim.assign(
        dropoff_location_id = dropoff_location_dim.index
    )

    dropoff_location_dim = dropoff_location_dim.merge(df_taxi_zones_lookup, how='left', left_on= 'DOLocationID', right_on='LocationID')

    dropoff_location_dim = dropoff_location_dim.rename(columns={'zone' : 'dropoff_location_zone',
                                        'borough' : 'dropoff_location_borough'})

    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id', 'DOLocationID', 'dropoff_location_zone','dropoff_location_borough']]

    context["ti"].xcom_push(key="dropoff_location_dim_tbl", value=dropoff_location_dim.to_json())

    
def push_dim_dropoff_location_tbl(**context):
    
    TABLE_NAME = 'dropoff_locations'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="dropoff_location_dim_tbl")
    
    df = pd.read_json(df_json)
    
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )
       
       
def create_dim_pickup_location_tbl(**context):
    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    df_taxi_zones = pd.read_csv('https://storage.googleapis.com/uber-datalake-raef/taxi_zones.csv', usecols=['zone', 'LocationID','borough'])

    df_taxi_zones_lookup = df_taxi_zones.drop_duplicates()
    df_taxi_zones_lookup = df_taxi_zones_lookup[['LocationID', 'borough','zone']]

    pickup_location_dim = df[['PULocationID']].drop_duplicates().reset_index(drop=True)

    pickup_location_dim = pickup_location_dim.assign(
        pickup_location_id = pickup_location_dim.index
    )

    pickup_location_dim = pickup_location_dim.merge(df_taxi_zones_lookup, how='left', left_on= 'PULocationID', right_on='LocationID')

    pickup_location_dim = pickup_location_dim.rename(columns={'zone' : 'pickup_location_zone',
                                        'borough' : 'pickup_location_borough'})

    pickup_location_dim = pickup_location_dim[['pickup_location_id', 'PULocationID', 'pickup_location_zone','pickup_location_borough']]

    context["ti"].xcom_push(key="pickup_location_dim_tbl", value=pickup_location_dim.to_json())

    
def push_dim_pickup_location_tbl(**context):
    
    TABLE_NAME = 'pickup_locations'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="pickup_location_dim_tbl")
    
    df = pd.read_json(df_json)
    
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )
       

def create_dim_trip_distance_tbl(**context):
    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)

    trip_distance_dim = trip_distance_dim.assign(
        trip_distance_id = trip_distance_dim.index
    )

    trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]

    context["ti"].xcom_push(key="trip_distance_dim_tbl", value=trip_distance_dim.to_json())

    
def push_dim_trip_distance_tbl(**context):
    
    TABLE_NAME = 'trip_distances'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="trip_distance_dim_tbl")
    
    df = pd.read_json(df_json)
    
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )
       

def create_dim_passenger_count_tbl(**context):
    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    passenger_count_dim =  df[['passenger_count']].drop_duplicates().reset_index(drop=True)

    passenger_count_dim = passenger_count_dim.assign(
        passenger_count_id = passenger_count_dim.index
    )

    passenger_count_dim = passenger_count_dim[['passenger_count_id', 'passenger_count']]

    context["ti"].xcom_push(key="passenger_count_dim_tbl", value=passenger_count_dim.to_json())

    
def push_dim_passenger_count_tbl(**context):
    
    TABLE_NAME = 'passenger_counts'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="passenger_count_dim_tbl")
    
    df = pd.read_json(df_json)
    
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )

       
def create_dim_vendor_tbl(**context):
    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    vendor_dim = df[['VendorID']].drop_duplicates().reset_index(drop=True)

    vendor_names = {
        1 : "Creative Mobile Technologies",
        2 : 'VeriFone Inc'
    }

    vendor_dim = vendor_dim.assign(
        vendor_id = vendor_dim['VendorID'].index,\
        vendor_name = vendor_dim.VendorID.map(vendor_names)
    )

    vendor_dim = vendor_dim[['vendor_id','VendorID', 'vendor_name']]

    context["ti"].xcom_push(key="vendor_dim_tbl", value=vendor_dim.to_json())

    
def push_dim_vendor_tbl(**context):
    
    TABLE_NAME = 'vendors'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="vendor_dim_tbl")
    
    df = pd.read_json(df_json)
    
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )
       

def create_dim_fare_tbl(**context):
    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    fare_dim = df[['fare_amount', 'extra', 'mta_tax',
                'tip_amount', 'tolls_amount', 'improvement_surcharge',
                'total_amount', 'congestion_surcharge', 'airport_fee']]\
                    .drop_duplicates().reset_index(drop=True)

    fare_dim = fare_dim.assign(
        fare_id = fare_dim.index
    )

    fare_dim = fare_dim[['fare_id','fare_amount', 'extra', 'mta_tax',
                        'tip_amount', 'tolls_amount', 'improvement_surcharge',
                        'total_amount', 'congestion_surcharge', 'airport_fee']]

    context["ti"].xcom_push(key="fare_dim_tbl", value=fare_dim.to_json())

    
def push_dim_fare_tbl(**context):
    
    TABLE_NAME = 'fares'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="fare_dim_tbl")
    
    df = pd.read_json(df_json)
    
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )
       

def create_dim_store_and_forward_tbl(**context):
    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    store_and_forward_dim = df[['store_and_fwd_flag']].drop_duplicates().reset_index(drop=True)

    flag_bool_type = {
        'N': 0,
        'Y': 1,
    }

    store_and_forward_dim = store_and_forward_dim.assign(
        store_and_forward_id = store_and_forward_dim.index,\
        store_and_forward_bool = store_and_forward_dim['store_and_fwd_flag'].map(flag_bool_type)
        )

    store_and_forward_dim.store_and_forward_bool = store_and_forward_dim.store_and_forward_bool.astype('boolean')

    store_and_forward_dim = store_and_forward_dim[['store_and_forward_id', 'store_and_forward_bool', 'store_and_fwd_flag']]

    context["ti"].xcom_push(key="store_and_forward_dim_tbl", value=store_and_forward_dim.to_json())

    
def push_dim_store_and_forward_tbl(**context):
    
    TABLE_NAME = 'store_and_forward_flags'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="store_and_forward_dim_tbl")
    
    df = pd.read_json(df_json)
    
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )
       

def create_trips_fct_tbl(**context):
    # Catch JSON data from xcom and read as DataFrame
    df_json = context["ti"].xcom_pull(key="raw_uber_data")
    df = pd.read_json(df_json)
    
    vendor_dim_json = context["ti"].xcom_pull(key="vendor_dim_tbl")
    vendor_dim = pd.read_json(vendor_dim_json)
    
    datetime_dim_json = context["ti"].xcom_pull(key="dim_datetime_tbl")
    datetime_dim = pd.read_json(datetime_dim_json)
    
    rate_code_dim_json = context["ti"].xcom_pull(key="rate_code_dim_tbl")
    rate_code_dim = pd.read_json(rate_code_dim_json)
    
    payment_type_dim_json = context["ti"].xcom_pull(key="payment_type_dim_tbl")
    payment_type_dim = pd.read_json(payment_type_dim_json)
    
    dropoff_location_dim_json = context["ti"].xcom_pull(key="dropoff_location_dim_tbl")
    dropoff_location_dim = pd.read_json(dropoff_location_dim_json)
    
    pickup_location_dim_json = context["ti"].xcom_pull(key="pickup_location_dim_tbl")
    pickup_location_dim = pd.read_json(pickup_location_dim_json)
    
    trip_distance_dim_json = context["ti"].xcom_pull(key="trip_distance_dim_tbl")
    trip_distance_dim = pd.read_json(trip_distance_dim_json)
    
    passenger_count_dim_json = context["ti"].xcom_pull(key="passenger_count_dim_tbl")
    passenger_count_dim = pd.read_json(passenger_count_dim_json)
    
    store_and_forward_dim_json = context["ti"].xcom_pull(key="store_and_forward_dim_tbl")
    store_and_forward_dim = pd.read_json(store_and_forward_dim_json)
    
    fare_dim_json = context["ti"].xcom_pull(key="fare_dim_tbl")
    fare_dim = pd.read_json(fare_dim_json)
    
        # Convert date columns to datetime types
    if "tpep_pickup_datetime" in df.columns:
        df["tpep_pickup_datetime"] = pd.to_datetime(df.tpep_pickup_datetime)
    if "tpep_dropoff_datetime" in df.columns:
        df["tpep_dropoff_datetime"] = pd.to_datetime(df.tpep_dropoff_datetime)
        
    df["tpep_pickup_datetime"] = df.tpep_pickup_datetime.astype(
        "string"
    )
    df["tpep_dropoff_datetime"] = df.tpep_dropoff_datetime.astype(
        "string"
    )
    
    datetime_dim["tpep_pickup_datetime"] = datetime_dim.tpep_pickup_datetime.astype(
        "string"
    )
    datetime_dim["tpep_dropoff_datetime"] = datetime_dim.tpep_dropoff_datetime.astype(
        "string"
    )
    
    
    fct_table = df\
        .merge(vendor_dim[['vendor_id', 'VendorID']], how='left', on='VendorID')\
        .merge(datetime_dim[['datetime_id','tpep_pickup_datetime','tpep_dropoff_datetime']], how='left', on=['tpep_pickup_datetime','tpep_dropoff_datetime'])\
        .merge(rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']], how='left', on=['RatecodeID'])\
        .merge(payment_type_dim[['payment_type_id','payment_type','payment_type_name']], how='left', on=['payment_type'])\
        .merge(dropoff_location_dim[['dropoff_location_id','DOLocationID','dropoff_location_zone', 'dropoff_location_borough']], how='left', on=['DOLocationID'])\
        .merge(pickup_location_dim[['pickup_location_id','PULocationID','pickup_location_zone', 'pickup_location_borough']], how='left', on=['PULocationID'])\
        .merge(trip_distance_dim[['trip_distance_id','trip_distance']], how='left', on=['trip_distance'])\
        .merge(passenger_count_dim[['passenger_count_id','passenger_count']], how='left', on=['passenger_count'])\
        .merge(store_and_forward_dim[['store_and_forward_id','store_and_fwd_flag']], how='left', on=['store_and_fwd_flag'])\
        .merge(fare_dim, how='left', on=['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge','total_amount', 'congestion_surcharge', 'airport_fee'])\
        .assign(event_id = lambda x: x.index)
            
    columns_in_fct_table = ['event_id','vendor_id', 'datetime_id','passenger_count_id','trip_distance_id','pickup_location_id',
                            'dropoff_location_id','rate_code_id','payment_type_id','store_and_forward_id', 'fare_id']
        
    fct_table = fct_table.loc[:, columns_in_fct_table]

    context["ti"].xcom_push(key="trips_fct_tbl", value=fct_table.to_json())

    
def push_trips_fct_tbl(**context):
    
    TABLE_NAME = 'trips'
    
    TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

    client = bigquery.Client(project=PROJECT_ID)

    df_json = context["ti"].xcom_pull(key="trips_fct_tbl")
    
    df = pd.read_json(df_json)
    
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )
       

with DAG(dag_id="uber-yellow-taxi-pipeline", start_date=datetime(2024, 9, 13), schedule=None,) as dag:
    # Task 1: Pull Raw Data from GCS
    pull_raw_data_task = PythonOperator(
        task_id="pull_raw_data_from_gcs", python_callable=pull_raw_data_from_gcs
    )

    # Task 2: Create datetime dimension
    create_datetime_dim_task = PythonOperator(
        task_id="create_dim_datetime_table", python_callable=create_dim_datetime_table
    )
    
    # Task 3: Push datetime dimesion table to BigQuery
    push_dim_datetime_tbl_task = PythonOperator(
        task_id="push_dim_datetime_tbl", python_callable=push_dim_datetime_tbl
    )
    
     # Task 4: Push datetime dimesio table to BigQuery
    create_dim_rate_code_tbl_task = PythonOperator(
        task_id="create_dim_rate_code_tbl", python_callable=create_dim_rate_code_tbl
    )
    
    # Task 5: Push datetime dimesio table to BigQuery
    push_dim_rate_code_tbl_task = PythonOperator(
        task_id="push_dim_rate_code_tbl", python_callable=push_dim_rate_code_tbl
    )
    
    # Task 6: Push datetime dimesio table to BigQuery
    create_dim_payment_type_tbl_task = PythonOperator(
        task_id="create_dim_payment_type_tbl", python_callable=create_dim_payment_type_tbl
    )
    
    # Task 7: Push datetime dimesio table to BigQuery
    push_dim_payment_type_tbl_task = PythonOperator(
        task_id="push_dim_payment_type_tbl", python_callable=push_dim_payment_type_tbl
    )
    
    # Task 8: Push datetime dimesio table to BigQuery
    create_dim_dropoff_location_tbl_task = PythonOperator(
        task_id="create_dim_dropoff_location_tbl", python_callable=create_dim_dropoff_location_tbl
    )
    
    # Task 9: Push datetime dimesio table to BigQuery
    push_dim_dropoff_location_tbl_task = PythonOperator(
        task_id="push_dim_dropoff_location_tbl", python_callable=push_dim_dropoff_location_tbl
    )
    
    # Task 10: Create PU location dimesion table to BigQuery
    create_dim_pickup_location_tbl_task = PythonOperator(
        task_id="create_dim_pickup_location_tbl", python_callable=create_dim_pickup_location_tbl
    )
    
    # Task 11: Push PU location dimesion table to BigQuery
    push_dim_pickup_location_tbl_task = PythonOperator(
        task_id="push_dim_pickup_location_tbl", python_callable=push_dim_pickup_location_tbl
    )
    
    # Task 12: Create trip distance dimesion table to BigQuery
    create_dim_trip_distance_tbl_task = PythonOperator(
        task_id="create_dim_trip_distance_tbl", python_callable=create_dim_trip_distance_tbl
    )
    
    # Task 13: Push trip distance dimesion table to BigQuery
    push_dim_trip_distance_tbl_task = PythonOperator(
        task_id="push_dim_trip_distance_tbl", python_callable=push_dim_trip_distance_tbl
    )
    
    # Task 14: Create passenger count table to BigQuery
    create_dim_passenger_count_tbl_task = PythonOperator(
        task_id="create_dim_passenger_count_tbl", python_callable=create_dim_passenger_count_tbl
    )
    
    # Task 15: Push passenger count dimesion table to BigQuery
    push_dim_passenger_count_tbl_task = PythonOperator(
        task_id="push_dim_passenger_count_tbl", python_callable=push_dim_passenger_count_tbl
    )
    
    # Task 14: Create vendor table to BigQuery
    create_dim_vendor_tbl_task = PythonOperator(
        task_id="create_dim_vendor_tbl", python_callable=create_dim_vendor_tbl
    )
    
    # Task 15: Push vendor dimesion table to BigQuery
    push_dim_vendor_tbl_task = PythonOperator(
        task_id="push_dim_vendor_tbl", python_callable=push_dim_vendor_tbl
    )
    
    # Task 16: Create fare table to BigQuery
    create_dim_fare_tbl_task = PythonOperator(
        task_id="create_dim_fare_tbl", python_callable=create_dim_fare_tbl
    )
    
    # Task 17: Push fare dimesion table to BigQuery
    push_dim_fare_tbl_task = PythonOperator(
        task_id="push_dim_fare_tbl", python_callable=push_dim_fare_tbl
    )
    
    # Task 16: Create store and forward flag table to BigQuery
    create_dim_store_and_forward_tbl_task = PythonOperator(
        task_id="create_dim_store_and_forward_tbl", python_callable=create_dim_store_and_forward_tbl
    )
    
    # Task 17: Push store and forward flag dimesion table to BigQuery
    push_dim_store_and_forward_tbl_task = PythonOperator(
        task_id="push_dim_store_and_forward_tbl", python_callable=push_dim_store_and_forward_tbl
    )
    
    # Task 18: Create fact table to BigQuery
    create_trips_fct_tbl_task = PythonOperator(
        task_id="create_trips_fct_tbl", python_callable=create_trips_fct_tbl
    )
    
    # Task 19: Push fact table to BigQuery
    push_trips_fct_tbl_task = PythonOperator(
        task_id="push_trips_fct_tbl", python_callable=push_trips_fct_tbl
    )
    
    # Set task dependencies
    pull_raw_data_task >>\
    create_datetime_dim_task >> push_dim_datetime_tbl_task >>\
    create_dim_rate_code_tbl_task >> push_dim_rate_code_tbl_task >>\
    create_dim_payment_type_tbl_task >> push_dim_payment_type_tbl_task >>\
    create_dim_dropoff_location_tbl_task >> push_dim_dropoff_location_tbl_task >>\
    create_dim_pickup_location_tbl_task >> push_dim_pickup_location_tbl_task >>\
    create_dim_trip_distance_tbl_task >> push_dim_trip_distance_tbl_task >>\
    create_dim_passenger_count_tbl_task >> push_dim_passenger_count_tbl_task >>\
    create_dim_vendor_tbl_task >> push_dim_vendor_tbl_task >>\
    create_dim_fare_tbl_task >> push_dim_fare_tbl_task >>\
    create_dim_store_and_forward_tbl_task >> push_dim_store_and_forward_tbl_task>>\
    create_trips_fct_tbl_task >> push_trips_fct_tbl_task