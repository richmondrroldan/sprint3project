# Maritess ng Ekonomiya

Maritess ng Ekonomiya is a project dedicated to collecting news from the following sources:
PhilStar RSS, and the Daily Inquirer RSS which will then combine into one CSV file. <br/>

The goal of the project is to collect news over a long period of time and make possible investors aware of the current economic state of the country based on the news articles that'll be spit out by the RSS feed.

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
# Editing the DAG file
To change sources or add sources, edit rich_scraper_dag and go to the tasks portion
```
@task(task_id="inquirer_feed")
def inquirer_feed(ds=None, **kwargs):
    upload_formatted_rss_feed_rich("https://www.inquirer.net/fullfeed", "inquirer")
    return True
```
Source can be changed by changing the link after the parentheses, as long as it is an RSS Feed.
# Other requirements
GCS Keys to be able to upload on bucket<br/>
.env file AIRFLOW_UID = 501 (For default credentials)


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.
