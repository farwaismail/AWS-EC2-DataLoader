# STA 9760: Project 1: Analyzing NYC Open Parking and Camera Violations

![PARKING](assets/kibanadashboard.png)

## Description
The aim of this project was to leverage what we have learnt about handling big data using AWS EC2 and Elasticsearch instances and use this knowledge to create a system ready to do proper analysis on a large database. In this case the database we chose to do analysis on was the [NYC Open Parking and Camera Violations](https://data.cityofnewyork.us/City-Government/Open-Parking-and-Camera-Violations/nc67-uf89)

The provided code fullfills the following objectives:
- Get data piece by piece from the NYC Open Parking Violations dataset using the Socratic Open Data API
- Load that data onto an Elasticsearch cluster on AWS

Each row in the dataset is a particular violation issued by NYC authorities back to 2016. A full explanation of the different columns in the data can be found [here](https://data.cityofnewyork.us/City-Government/Open-Parking-and-Camera-Violations/nc67-uf89)

### Speed Optimizations
Additionaly for speed I have done the following things:
- The code parallelizes the API calls to and from the required servers. It uses python's `Threading` module to run the code's process on multiple threads in a concurrent way. The max threads have been set at `20`.
- I added a completely optional setting for the script (see: `--bulk="True"`), that uses Elasticsearch's BULK upload API to upload one whole batch of rows (= one page) in one API call, as opposed to doing one API call for each row. 


## Usage
### Step 1: Build the docker image 
```
docker build -t bigdata1:1.0 project01/
```

### Step 2: Run the docker container 
```
docker run \
	-e DATASET_ID="nc67-uf89" \
	-e APP_TOKEN="XXXX" \
	-e ES_HOST="XXXX" \
	-e ES_USERNAME="XXXX" \
	-e ES_PASSWORD="XXXX=" \
	--network="host" \
	bigdata1:1.0 --num_pages=100 --page_size=100
```

all versions of `DATASET_ID`,`APP_TOKEN`, `ES_HOST`, `ES_USERNAME` and `ES_PASSWORD` shown below work
```
DATA_ID, DATASET_ID, dataset_id
APP_TOKEN, app_token
ES_HOST, es_host
ES_USERNAME, es_username
ES_PASSWORD,es_password
```

### Step2b: Script Options
Notice that there are a few options for the script which we will outline now:
The docker scrpt above takes in multiple options:
##### `--page_size`:
- This is the number of rows that the script will pull from the Socrata API *per API call*. This number will be called one __page__.
- This option is __required__, the script will throw an error if this is not in present.

##### `--num_pages` (if provided): 
- If this value is provided, this is the number of pages that will be processed by the script.
- The total number of rows processed by this script is `page_size * num_pages`
- This option is optional

##### `--num_pages` (if not provided): 
- If this value is __not__ provided, then the script will pull the __ENTIRE__ database and load it onto the Elasticsearch instance.

#### `--bulk`(defaults to "False" if not provided):
- If this value is `"True"`, each page will be uploaded onto Elasticsearch in one API call using its __Bulk upload API__
- [__Default__] If this value is ANYTHING ELSE OR NOT PROVIDED, the dataset will be uploaded onto Elasticsearch __row by row__

Example:
```docker 
... --num_pages=20 --page_size=1000 --bulk="True"
```


## Code Structure Decisions
I decided to create three different classes for the processing. This allows the code to seperated into disjoint blocks:
##### `OPCV_client`:  
This class uses the Socrata API to handle:
- getting particular pages from the dataset, 
- getting the total row count of the dataset

##### `ES_client`: 
This class uses the requests library to send data to Elasticsearch and handles:
- checking if a particular index exists 
- initialising a dataset with the correct types
- sending one row of data to an Elasticsearch
- bulk sending a bunch of rows of data to Elasticsearch in one API call

##### `data_manager`:
This class combines the previous clients to handle.:
- getting the data from the socrata api and delivering it row by row to the Elasticsearch instance
- getting the data from the socrata api and sending it in bulk to the elasticsearch instance

##### `thread_manager`
Defining a `job` to mean getting one page of the socrata API and sending it to Elasticsearch, this class:
- assigns jobs and lists of jobs to different threads based on the number of jobs needed to be completed. 


