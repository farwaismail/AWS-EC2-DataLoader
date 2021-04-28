import requests
from requests.auth import HTTPBasicAuth
from sodapy import Socrata
import datetime
import time
import json
import threading
from data_juggler_client.conf import index_data_dictionary, fixtypes, DEFAULT_INDEX_NAME, WEBSITE

class OPCV_client:
	def __init__(self,DATASET_ID,APP_TOKEN,WEBSITE=WEBSITE,timeout=60):
		self.DATASET_ID = DATASET_ID
		self.APP_TOKEN = APP_TOKEN
		self.client = Socrata( WEBSITE , APP_TOKEN ,timeout=timeout)
		self.number_of_rows = None

	def get_number_of_rows(self):
		self.number_of_rows = int(self.client.get(self.DATASET_ID,select="COUNT(*)")[0]['COUNT'])
		print(self.number_of_rows)
		return self.number_of_rows
		
	#Gets rows and fixes all the types (strings to floats and etc)
	def get_rows(self,page_size,offset):
		return list(map(fixtypes,self.client.get(self.DATASET_ID,limit=page_size,offset=offset)))


class ES_client:
	def __init__(self,ES_HOST,ES_USERNAME,ES_PASSWORD, index_name = DEFAULT_INDEX_NAME):
		self.ES_HOST = ES_HOST
		self.ES_USERNAME = ES_USERNAME
		self.ES_PASSWORD = ES_PASSWORD
		self.index_name = index_name

	def check_if_index_exists(self,index_name):
		r = requests.head(f"{self.ES_HOST}/{index_name}",auth=HTTPBasicAuth(self.ES_USERNAME, self.ES_PASSWORD))
		print("Here is the response to checking if index exists: ")
		print(r.status_code)
		if r.status_code != 404:
			print("The index already exists")
			print("Deleting existing index")
			self.delete_index(self.index_name)
		return (r.status_code == 404)

	def initialize_index_with_correct_types(self):
		if self.check_if_index_exists(self.index_name):
			url = f"{self.ES_HOST}/{self.index_name}"
			payload = {"mappings": {"properties": index_data_dictionary}}
			r = requests.put(url, auth=HTTPBasicAuth(self.ES_USERNAME, self.ES_PASSWORD), json=payload)
			print("Creating Payload for generating a new index. The payload is:")
			print(payload)
			print("Here is the response to generating a new index: ")
			print(r.json())
			print(" ")

	def send_payload(self,payload):
		url = f"{self.ES_HOST}/{self.index_name}/_doc"
		t_start = time.time()
		r = requests.post(url,auth=HTTPBasicAuth(self.ES_USERNAME, self.ES_PASSWORD),json=payload)
		print(f"Response to sending payload took {time.time() - t_start} seconds")
		print("Here is the response to adding another row: ")
		print(r.json())
		print(" ")
		
	def preprocess_for_bulk_send(self,payload_list):
		newline = "\n"
		data = ""
		for payload in payload_list:
			data += '{"index": {}}' + newline
			data += json.dumps(payload) + newline
		return data
	
	def bulk_send_payload_list(self,payload_list):
		data = self.preprocess_for_bulk_send(payload_list)
		url = f"{self.ES_HOST}/{self.index_name}/_bulk"
		t_start = time.time()
		r = requests.post(url,auth=HTTPBasicAuth(self.ES_USERNAME, self.ES_PASSWORD),headers={'content-type':'application/json', 'charset':'UTF-8'},data=data)
		print(f"Response to bulk sending payload took {time.time() - t_start} seconds")
		print("Here is the response to bulk adding another row: ")
		print(r.json())
		print(" ")

	def delete_index(self,index_name):
		url = f"{self.ES_HOST}/{index_name}"
		r = requests.delete(url,auth=HTTPBasicAuth(self.ES_USERNAME, self.ES_PASSWORD))
		print("Here is the response to deleting an index: ")
		print(r.status_code)
		print(" ")


class data_worker:
	def __init__(self,ES_client,OP_client):
		self.ES_client = ES_client
		self.OP_client = OP_client

	def transfer_data(self,page_size,offset):
		t_start = time.time()
		data = self.OP_client.get_rows(page_size,offset)
		t_get = time.time()
		print(f"Getting Data for this job took {t_get - t_start}")
		for row in data:
			self.ES_client.send_payload(row)
		print(f"Transfering this row took{time.time() - t_start} seconds")
	
	def bulk_transfer_data(self,page_size,offset):
		t_start = time.time()
		data = self.OP_client.get_rows(page_size,offset)
		t_get = time.time()
		print(f"Getting Data for this job took {t_get - t_start}")
		self.ES_client.bulk_send_payload_list(data)
		print(f"Transfering this row took{time.time() - t_start} seconds")

class thread_manager:
	def __init__(self,num_workers,num_pages):
		self.num_workers = num_workers
		self.num_pages = num_pages
		self.threads = []
		self.JobAssignment = {worker_id : [] for worker_id in range(self.num_workers)}

	def make_schedule(self,worker_function,list_of_jobs):
		#Assign the jobs as equitably as possible to all workers
		for i,job in enumerate(list_of_jobs):
			self.JobAssignment[i%self.num_workers].append(job)

		#Create some lambda functions so that we can pass functions that can do
		#multiple jobs over lists and unpack dictionary arguments as well
		worker_function_accepts_dict = lambda x: worker_function(**x)
		work_on_list_of_jobs = lambda the_jobs: list(map(worker_function_accepts_dict, the_jobs))

		for worker, jobs in self.JobAssignment.items():
			self.threads.append(threading.Thread(target=work_on_list_of_jobs, args=[jobs]))

	def let_it_rip(self):
		for a_thread in self.threads:
			print("starting threads")
			a_thread.start()

		for a_thread in self.threads:
			a_thread.join()
		



def get_first_non_none(x):
	return [i for i in x if i is not None][0]
