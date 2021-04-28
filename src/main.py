from data_juggler_client import OPCV_client, ES_client, data_worker, thread_manager, get_first_non_none
from sodapy import Socrata
import os
import argparse
import threading

if __name__ == "__main__":
    
    #######################################################################
    # SETTING UP ALL THE VARIABLES
    #######################################################################
    
    #Get the Dataset ID
    DATA_ID = os.getenv('DATA_ID')
    DATASET_ID = os.getenv('DATASET_ID')
    dataset_id = os.getenv('dataset_id')
    DATASET_ID = get_first_non_none([DATA_ID,DATASET_ID,dataset_id])
    
    #Get the App Token
    APP_TOKEN = os.getenv('APP_TOKEN')
    app_token = os.getenv('app_token')
    APP_TOKEN = get_first_non_none([APP_TOKEN,app_token])
    
    #Get the ES host
    ES_HOST = os.getenv('ES_HOST')
    es_host = os.getenv('es_host')
    ES_HOST = get_first_non_none([ES_HOST,es_host])
    ES_HOST = ES_HOST.strip("/")
    
    
    #Get the ES Username
    ES_USERNAME = os.getenv('ES_USERNAME')
    es_username = os.getenv('es_username')
    ES_USERNAME = get_first_non_none([ES_USERNAME,es_username])
    
    
    #Get the ES Password
    ES_PASSWORD = os.getenv('ES_PASSWORD')
    es_password = os.getenv('es_password')
    ES_PASSWORD = get_first_non_none([ES_PASSWORD,es_password])
    
    #Setting up argument parser so we can do --stuff on the command line
    parser = argparse.ArgumentParser()
    requiredArgs = parser.add_argument_group('required arguments')
    requiredArgs.add_argument("-psize", "--page_size", type=int, help="set the page size per API call", required = True)
    parser.add_argument("-pnum", "--num_pages", type=int, help="set the number of pages pulled")
    parser.add_argument("-blk", "--bulk", type=str, help="sets the option to bulk upload data")
    args = parser.parse_args()
    
    #Prints for debugging
    print(f"Page size is {args.page_size}")
    print(f"Page num is {args.num_pages}")
    print(f"Bulk uploading is {args.bulk}")
    PAGE_SIZE = args.page_size
    NUM_PAGES = args.num_pages
    BULK = args.bulk
    if (BULK is None) or not (BULK == "True"):
        BULK = False
    else:
        BULK = True
    
    
    #######################################################################
    # DOING THE DATA JUGGLING
    #######################################################################
    
    # Create new clients
    ES = ES_client(ES_HOST,ES_USERNAME,ES_PASSWORD) # Sends data to Elasticsearch
    OP = OPCV_client(DATASET_ID,APP_TOKEN,timeout=60) # Brings data from OPCV
    DW = data_worker(ES,OP) # Class that has functions to perform the data transfer
    
    # Initialize an index if it doesn't already exist
    # If it does exist, delete it and initialise a new one (for now)
    # This function also makes sure to intitialize the index
    # with the right types. (ex. date fields are intialized to dates)
    ES.initialize_index_with_correct_types() 
    
    # In the case num_pages is not provided, the whole dataset should be pushed 
    # onto elastic search
    if NUM_PAGES is None:
    	# Get number of rows in the OPCV dataset so we can set the number of pages
    	num_rows = OP.get_number_of_rows()
    	NUM_PAGES = num_rows // PAGE_SIZE
    	INITIAL_OFFSET = 0
    else:
        INITIAL_OFFSET = 0
    
    
    # Make multiple workers that will call the API and use them concurrently
    num_workers = min(NUM_PAGES,20) #Not more than 20 threads
    
    # We can make a worker function (in this case, it is a 
    # function that can transfer data from OPCV to Elasticsearch) that we will use:
    if BULK:
        worker_function = DW.bulk_transfer_data
    else:
        worker_function = DW.transfer_data
    
    # We can make an iterator of all arguments we will pass to the worker function. 
    # The arguments will be stored as a dictionary
    # DW.transfer_data takes in two arguments: page_size and offset
    # We will pass the offset as a multiple of the page size as mentioned in the
    # documentation: https://dev.socrata.com/docs/queries/offset.html
    all_jobs = ({'page_size': PAGE_SIZE, 'offset': PAGE_SIZE*i + INITIAL_OFFSET} for i in range(NUM_PAGES)) 
     
    # Using a class I created to manage multiple jobs and assign them in an equitable
    # manner.
    all_tasks_as_threads = thread_manager(num_workers,NUM_PAGES)
    
    # Equitably define jobs for all the workers
    all_tasks_as_threads.make_schedule(worker_function,all_jobs)
    
    # Print the task assignmentL
    num_of_job_assignment = {k:len(v) for k,v in all_tasks_as_threads.JobAssignment.items()}
    print(num_of_job_assignment)
    
    # Do all the the assigned tasks
    all_tasks_as_threads.let_it_rip()