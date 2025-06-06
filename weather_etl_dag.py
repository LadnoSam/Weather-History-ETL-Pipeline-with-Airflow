from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import zipfile
import pandas as pd
import sqlite3
import numpy as np

default_args = {
    'owner': 'DE_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='project_dag',
        default_args=default_args,
        description='Extract, transform, validation, load Weather History data from Kaggle',
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
) as dag:
    def download_weather_data(**kwargs):
        """
        Download Weather History dataset from Kaggle using the Kaggle API.
        """
        # Set up the Kaggle credentials
        os.environ['KAGGLE_CONFIG_DIR'] = os.path.expanduser("~/.kaggle")

        # Settings
        kaggle_dataset = "muthuj7/weather-dataset"
        output_dir = "/tmp/weather_data" # Guys don't forget to change to ure directory to make it works on ur pc
        os.makedirs(output_dir, exist_ok=True)

        # Download dataset
        os.system(f"kaggle datasets download -d {kaggle_dataset} -p {output_dir}")

        # Pass the path of the downloaded file to XCom
        zip_path = os.path.join(output_dir, f"{kaggle_dataset.split('/')[-1]}.zip")
        kwargs['ti'].xcom_push(key='zip_path', value=zip_path)


    def unzip_and_save(**kwargs):
        """
        Unzip the downloaded dataset and save the CSV file path.
        """
        # Retrieve ZIP file path from XCom
        zip_path = kwargs['ti'].xcom_pull(key='zip_path', task_ids='download_weather_data')
        output_dir = "/tmp/weather_data"

        # Unzip the file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)

        # Identify the extracted CSV file
        csv_file = next(
            (os.path.join(output_dir, file) for file in os.listdir(output_dir) if file.endswith('.csv')), None
        )
        if not csv_file:
            raise FileNotFoundError("CSV file not found in the extracted content.")

        # Pass CSV file path to XCom
        kwargs['ti'].xcom_push(key='csv_file', value=csv_file)


    # Define tasks
    download_task = PythonOperator(
        task_id='download_weather_data',
        python_callable=download_weather_data,
        provide_context=True,
    )

    unzip_task = PythonOperator(
        task_id='unzip_and_save',
        python_callable=unzip_and_save,
        provide_context=True,
    )

def transform_data(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='csv_file')
    df = pd.read_csv(file_path)

    #Data cleaning:
    #Convert to proper date format
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], errors='coerce', utc=True)
    df['month'] = df['Formatted Date'].dt.strftime("%Y-%m")
    df['day'] = df['Formatted Date'].dt.strftime("%Y-%m-%d")

    #Handle any missing or erroneous data in critical columns
    df.dropna(inplace=True)
    #Check for duplicates and remove if necessary
    df.drop_duplicates(inplace=True)

    #Feature Engineering:
    #Daily averages:
    avg_daily_temperature = df.groupby('day')['Temperature (C)'].mean()
    avg_daily_humidity = df.groupby('day')['Humidity'].mean()
    avg_daily_wind_speed = df.groupby('day')['Wind Speed (km/h)'].mean()

    #Monthly Mode for Presipitation Type
    #Group data by month and calculate the mode of Precip Type for each month.
    #If there's no clear mode, mark it as NaN.
    def mode_with_ties(series):
        mode_counts = series.value_counts()
        if len(mode_counts) == 0:
            return np.nan
        elif (len(mode_counts) > 1) and (mode_counts.iloc[0] == mode_counts.iloc[1]):
            return np.nan
        else:
            return mode_counts.iloc[0]
        
    monthly_mode_precip = df.groupby(df['month'])['Precip Type'].apply(mode_with_ties)


    #Wind Strength Categorization
    
    def wind_strength_categorization(wind_speed):
        wind_speed = wind_speed / 3.6
        if wind_speed <= 1.5:
            return 'Calm'
        elif wind_speed <= 3.3:
            return 'Light Air'
        elif wind_speed <= 5.4:
            return 'Light Breeze'
        elif wind_speed <= 7.9:
            return 'Gentle Breeze'
        elif wind_speed <= 10.7:
            return 'Moderate Breeze'
        elif wind_speed <= 13.8:
            return 'Fresh Breeze'
        elif wind_speed <= 17.1:
            return 'Strong Breeze'
        elif wind_speed <= 20.7:
            return 'Near Gale'
        elif wind_speed <= 24.4:
            return 'Gale'
        elif wind_speed <= 28.4:
            return 'Strong Gale'
        elif wind_speed <= 32.6:
            return 'Storm'
        else:
            return 'Violent Storm'
        
    df['wind_strength'] = df['Wind Speed (km/h)'].apply(wind_strength_categorization)

    #Monthly Aggregates
    #Calculate monthly averages 
    avg_monthly_temperature = df.groupby(df['month'])['Temperature (C)'].mean()
    avg_monthly_humidity = df.groupby(df['month'])['Humidity'].mean()
    avg_monthly_wind_speed = df.groupby(df['month'])['Wind Speed (km/h)'].mean()
    avg_monthly_visibility = df.groupby(df['month'])['Visibility (km)'].mean()
    avg_monthly_pressure = df.groupby(df['month'])['Pressure (millibars)'].mean()

    #Save daily and monthly transformed daily and monthly data to new csv files.
    daily_averages = pd.DataFrame()
    daily_averages['daily average temperature (c)'] = avg_daily_temperature
    daily_averages['daily average humidity'] = avg_daily_humidity
    daily_averages['daily average wind speed km/h'] = avg_daily_wind_speed
    daily_averages['day'] = daily_averages.index

    monthly_averages = pd.DataFrame()
    monthly_averages['monthly average temperature (C)'] = avg_monthly_temperature
    monthly_averages['monthly average pressure (millibars)'] = avg_monthly_pressure
    monthly_averages['monthly average humidity'] = avg_monthly_humidity
    monthly_averages['monthly average visibility (km)'] = avg_monthly_visibility
    monthly_averages['monthly average wind speed (km/h)'] = avg_monthly_wind_speed
    monthly_averages['monthly mode precip type'] = monthly_mode_precip #Saving monthly precip type 
    monthly_averages['month'] = monthly_averages.index 


    #XCom for transformation, transformed daily and monthly data to the validation step.

    transformed_file_path = '/tmp/transformed_weather_history_data.csv'
    df.to_csv(transformed_file_path, index=False)
    kwargs['ti'].xcom_push(key='transformed_file_path', value=transformed_file_path)

    daily_file_path = '/tmp/daily_averages.csv'
    daily_averages.to_csv(daily_file_path, index=False)
    kwargs['ti'].xcom_push(key='daily_file_path', value=daily_file_path)

    monthly_file_path = '/tmp/monthly_averages.csv'
    monthly_averages.to_csv(monthly_file_path, index=False)
    kwargs['ti'].xcom_push(key='monthly_file_path', value=monthly_file_path)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context = True,
    dag=dag,
)


def validate_data(**kwargs):
    import logging
    from scipy import stats
    ti = kwargs['ti']
    
    #Pulls the paths of the transformed files from XCom.
    transformed_file_path = ti.xcom_pull(key='transformed_file_path', task_ids='transform_task')
    daily_file_path = ti.xcom_pull(key='daily_file_path', task_ids='transform_task')
    monthly_file_path = ti.xcom_pull(key='monthly_file_path', task_ids='transform_task')

    #Reading the data
    df_transformed = pd.read_csv(transformed_file_path)
    df_daily = pd.read_csv(daily_file_path)
    df_monthly = pd.read_csv(monthly_file_path)

    #Checks for missing values check in critical columns.
    critical_columns = ['Formatted Date', 'Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'wind_strength']
    missing_values = df_transformed[critical_columns].isnull().sum()
    if missing_values.any():
        logging.error(f"Missing values found in critical columns: \n{missing_values}")
        raise ValueError("Missing values found in critical columns in transformed data")

    #Cheking the ranges.
    if not ((df_transformed['Temperature (C)'] >= -50) & (df_transformed['Temperature (C)'] <= 50)).all():
        logging.error("Temperature values out of expected range (-50 to 50°C) in transformed data")
        raise ValueError("Temperature values out of expected range (-50 to 50°C) in transformed data")

    if not ((df_transformed['Humidity'] >= 0) & (df_transformed['Humidity'] <= 1)).all():
        logging.error("Humidity values out of expected range (0 to 1) in transformed data")
        raise ValueError("Humidity values out of expected range (0 to 1) in transformed data")

    if not (df_transformed['Wind Speed (km/h)'] >= 0).all():
        logging.error("Wind Speed values out of expected range (>= 0 km/h) in transformed data")
        raise ValueError("Wind Speed values out of expected range(>= 0 km/h) in transformed data")

    #Temperature(C) outlier detection.
    df_transformed['Temperature_zscore'] = stats.zscore(df_transformed['Temperature (C)'])
    outliers = df_transformed[(df_transformed['Temperature_zscore'].abs() > 3)]
    if not outliers.empty:
        logging.info(f"Found {len(outliers)} temperature outliers:")
        logging.info(outliers[['Formatted Date', 'Temperature (C)', 'Temperature_zscore']])
    else:
        logging.info("No temperature outliers found.")

    #Clean up of the DataFrame.
    df_transformed.drop(columns=['Temperature_zscore'], inplace=True)

    #Pass the validated data onwards with XCom.
    ti.xcom_push(key='validated_transformed_file_path', value=transformed_file_path)
    ti.xcom_push(key='validated_daily_file_path', value=daily_file_path)
    ti.xcom_push(key='validated_monthly_file_path', value=monthly_file_path)

 #Define tasks.
validation_task = PythonOperator(
        task_id='validation_task',
        python_callable=validate_data,
        provide_context=True,
        trigger_rule='all_success',
    )

#Load task
#A function fo creating the final versions of wanted tables
def final_tables(**kwargs):
    ti = kwargs['ti']
    #pulling file paths from XCom for transformed, daily and monthly files
    transformed_file_path = ti.xcom_pull(key='transformed_file_path', task_ids='transform_task')
    daily_file_path = ti.xcom_pull(key='daily_file_path', task_ids='transform_task')
    
    #reading the data from csv files
    df_transformed_final= pd.read_csv(transformed_file_path)
    df_daily_final = pd.read_csv(daily_file_path)

    #adding necessary columns to final version of the daily data
    df_daily_final['formatted_date']=df_transformed_final['Formatted Date']
    df_daily_final['temperature_c']=df_transformed_final['Temperature (C)']
    df_daily_final['apparent_temperature_c']=df_transformed_final['Apparent Temperature (C)']
    df_daily_final['humidity']=df_transformed_final['Humidity']
    df_daily_final['wind_speed_kmh']=df_transformed_final['Wind Speed (km/h)']
    df_daily_final['visibility_km']=df_transformed_final['Visibility (km)']
    df_daily_final['wind_strength']=df_transformed_final['wind_strength']
    df_daily_final['pressure_millibars']=df_transformed_final['Pressure (millibars)']

    #creating a final daily file and pushing it to xcom
    daily_final_path = '/tmp/daily_final.csv'
    df_daily_final.to_csv(daily_final_path, index=False)
    kwargs['ti'].xcom_push(key='daily_final_path', value=daily_final_path)

#Defining the task
final_tables_task=PythonOperator(
    task_id='final_tables_task',
    python_callable=final_tables,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)
#path for the database
db_path='/home/core/airflow/databases/weather_database.db'

#task for creating a database and loading the data
def load_data(**kwargs):
    ti = kwargs['ti']

    #pulling file paths from XCom
    daily_final_path=ti.xcom_pull(key='daily_final_path', task_ids='final_tables_task')
    monthly_file_path = ti.xcom_pull(key='monthly_file_path', task_ids='transform_task')
    #reading the csv files
    df_daily_final = pd.read_csv(daily_final_path)
    df_monthly = pd.read_csv(monthly_file_path)

    #loading to sql
    conn=sqlite3.connect(db_path)
    #creating two different tables per task description
    df_daily_final.to_sql('daily_weather', conn, if_exists='append', index=False)
    df_monthly.to_sql('monthly_weather', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()

#Defining the load task
load_data_task=PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)
# Task dependencies.
download_task >> unzip_task >> transform_task >> validation_task >> final_tables_task >> load_data_task
