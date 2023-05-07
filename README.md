# sparkify airflow pipeline
### Airflow pipeline that extracts imagianry user data from s3 buckets and loads them to redshift.

## Files

### Create_tables.sql
Sets up the data warehouse schema in Redshift

### dags/Notes.ipynb
Lists the steps performed to build this pipeline

### dags/dag.py
Main Airflow dag

### plugins/helpers/sql_queries.py
Inserts data from the staging area to the warehouse

### plugins/operators
Directory holding the Custom Airflow operators used to build the pipeline
