# Maritess ng Ekonomiya

Maritess ng Ekonomiya is a project dedicated to collecting news from the following sources:
PhilStar, and the Daily Inquirer which will then combine into one CSV file. 

# docker-compose.yaml
To be able to run the Airflow environment it is required to install the following files first:
Python, WSL (if on windows), and Docker. Once these have been installed the following commands can be run

```bash
docker compose up airflow init
```
This is to initialize airflow,
```
docker compose up
```
this command will be run to be able to access Airflow and the GUI
## Usage
To log in airflow use the default credentials which are username: airflow and password: airflow. Once you have accessed the GUI simply click on the play button on rich_scraper_dag to run the DAG. You will be able to access the output files in your selected GCS bucket.


# Other requirements
GCS Keys to be able to upload on bucket
.env file AIRFLOW_UID = 501 (For default credentials)
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.
