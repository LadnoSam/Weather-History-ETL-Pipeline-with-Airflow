# ⛅ Weather History ETL Pipeline with Airflow

This project uses **Apache Airflow** to build a complete ETL pipeline that processes historical weather data from **Kaggle**, performs data cleaning and transformation, validates the results, and loads them into an **SQLite** database.

---

## 🧭 Pipeline Stages

1. **Extract**  
   - Downloads a weather dataset from Kaggle using the Kaggle API.

2. **Transform**  
   - Cleans the dataset, removes duplicates/missing values.
   - Extracts new features (daily/monthly aggregations, wind strength categories).

3. **Validate**  
   - Ensures value ranges, checks for nulls, and detects outliers.

4. **Load**  
   - Writes the cleaned and aggregated data to a local SQLite database.

---

## 📂 Project Structure

```
project/
├── dags/
│   └── weather_etl_dag.py          # Main DAG script with tasks
├── databases/
│   └── weather_database.db         # SQLite database
└── tmp/
    └── weather_data/               # Temporary folder for data files
        ├── daily_averages.csv
        ├── monthly_averages.csv
        ├── daily_final.csv
```

---

## ⚙️ Requirements

- Python 3.7+
- Apache Airflow
- SQLite3
- Kaggle CLI with API credentials
- Python libraries:
  ```bash
  pip install apache-airflow pandas numpy kaggle scipy
  ```

---

## 🔐 Kaggle Authentication

1. Create your API key at: https://www.kaggle.com/account  
2. Save the `kaggle.json` key in this location:

   ```bash
   ~/.kaggle/kaggle.json
   ```

---

## 🚀 Usage Instructions

1. Initialize Airflow and start services:

   ```bash
   airflow db init
   airflow scheduler
   airflow webserver
   ```

2. Copy the DAG (`weather_etl_dag.py`) into your Airflow `dags/` folder.

3. Open the Airflow UI at:  
   [http://localhost:8080](http://localhost:8080)

4. Enable and trigger the DAG named `project_dag`.

---

## 📊 Output Data

The pipeline produces:

- `daily_final.csv`: Daily cleaned weather records
- `monthly_averages.csv`: Monthly aggregated metrics
- `weather_database.db`: SQLite DB with two tables:
  - `daily_weather`
  - `monthly_weather`

---

## 🧠 Key Features

- Daily and monthly aggregations
- Wind strength classification (Beaufort scale logic)
- Mode calculation for monthly precipitation type
- Z-score based outlier detection
- Robust validation checks on temperature, humidity, and wind speed
- Fully modular and testable pipeline using PythonOperator tasks

---

## 📝 Notes

- Change `/tmp/weather_data/` to a suitable path for your OS.
- SQLite DB is saved at: `/home/core/airflow/databases/weather_database.db` (modify as needed).

---

## 🧹 Cleanup

To remove temporary files and reset state:

```bash
rm -rf /tmp/weather_data/
rm /home/core/airflow/databases/weather_database.db
```

---

## 🙌 Credits

Dataset: [Weather Dataset on Kaggle](https://www.kaggle.com/datasets/muthuj7/weather-dataset) 
 
---
