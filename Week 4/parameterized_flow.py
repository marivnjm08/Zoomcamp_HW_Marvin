from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
import os

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame,taxi_type: str) -> pd.DataFrame:
    """Fix dtype issues"""
    
    if taxi_type == "fhv":
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
        df['PUlocationID'] = df['PUlocationID'].astype('str')
        df['DOlocationID'] = df['DOlocationID'].astype('str')
        df['SR_Flag'] = df['SR_Flag'].astype('str')
    elif taxi_type == "yellow":
        df['tpep_pickup_datetime'] = pd.to_datetime(df["tpep_pickup_datetime"])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df["tpep_dropoff_datetime"])
    elif taxi_type == "green":
        df['lpep_pickup_datetime'] = pd.to_datetime(df["lpep_pickup_datetime"])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df["lpep_dropoff_datetime"])
    
    print(f"Taxi Type: {taxi_type}")
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str,taxi_type:str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{taxi_type}\{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(name="Sub-flow",log_prints=True)
def etl_web_to_gcs(year: int, month: int, taxi_type: str) -> None:
    """The main ETL function"""
    dataset_file = f"{taxi_type}_tripdata_{year}-{month:02}"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{dataset_file}.csv.gz"
    
    df = fetch(dataset_url)
    df_clean = clean(df,taxi_type)
    path = write_local(df_clean,  dataset_file,taxi_type)
    write_gcs(path)


@flow(name="Ingest Data | Master Flow",log_prints=True)
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, taxi_type: str = "default"
):
    os.mkdir(fr"C:\Users\stude\Documents\Zoomcamp DE\Week 3\Zoomcamp_HW_Marvin\Week 4\{taxi_type}")
    for month in months:
        etl_web_to_gcs(year, month,taxi_type)


if __name__ == "__main__":
    # taxi_type = "yellow"
    # taxi_type = "green"
    taxi_type = "fhv" 
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    # months = [ 8, 9, 10, 11, 12]
    year = 2019
    etl_parent_flow(months, year, taxi_type)


