import dask
import logging
import time
import papermill as pm
import os
import threading
import shutil
from glob import glob
from tqdm.notebook import tqdm
from pathlib import Path
from datetime import datetime

# Declare Constant Folder Paths
kSCRIPT_WORKING_DIRECTORY, kFILENAME = os.path.split(os.path.abspath(__file__))
kBASE_FOLDER = '/home/'

kDATA_FOLDER = kBASE_FOLDER + 'Data/'
kDAILY_LOG_DIRECTORY = kSCRIPT_WORKING_DIRECTORY + '/' + 'daily_logs/'
kPAPERMILL_OUTPUT_DIR = kSCRIPT_WORKING_DIRECTORY + '/' + 'out' + '/'

# current date and time
date_time = datetime.now()
kDAILY_LOG_FOLDER_PATH = kDAILY_LOG_DIRECTORY + datetime.today().strftime('%Y') + '/' + datetime.today().strftime("%B") + '/' + datetime.today().strftime('%Y-%m-%d')

print("Log Folder Path", kDAILY_LOG_FOLDER_PATH)
# Check whether the specified path exists or not
isExist = os.path.exists(kDAILY_INDEPENDENT_LOG_FOLDER_PATH)
#printing if the path exists or not
print(isExist)
if not isExist:
   # Create a new directory because it does not exist
   os.makedirs(kDAILY_LOG_FOLDER_PATH)
   print("The new log directory is created!")

# Check whether the specified weekly path exists or not

kSCRIPT_WORKING_DIRECTORY, kFILENAME = os.path.split(os.path.abspath(__file__))
kSCRIPT_WORKING_DIRECTORY = kSCRIPT_WORKING_DIRECTORY + "/"

print(kSCRIPT_WORKING_DIRECTORY, kFILENAME)

# Create a threading.Lock
notebook_counter_lock = threading.Lock()

os.chdir('/home/')
# get the current working directory
kCURRENT_WORKING_DIRECTORY = os.getcwd()

# Start the timer
script_start_time = time.time()
# Configure logging
loggerFilePath = kDAILY_LOG_FOLDER_PATH + '/' + 'daily.log'
logging.basicConfig(level=logging.INFO, filename=loggerFilePath, filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
notebook_result_file = kDAILY_LOG_FOLDER_PATH + '/' + "daily_result.out"
kNOTEBOOK_RESULT_HANDLE = open(notebook_result_file, 'w')
kNOTEBOOK_INDEX = 0

######################################### DECLARE NOTEBOOK DICTIONARY WITH ASSOCIATED KERNEL NAMES ###################################
all_notebooks_list = {kDATA_FOLDER + 'notebook.ipynb' : "kernel"}

print("Current Working Directory ===> ", kSCRIPT_WORKING_DIRECTORY)
print("Executing Python Script ===> ", __file__)
logging.info(f"Current Working Directory ===> {kSCRIPT_WORKING_DIRECTORY}")
logging.info(f"Executing Python Script ===> {__file__}")

def copy_notebook_result_files():
    source_file = open("%s" % notebook_result_file, 'rb')
    destination_file = open("%s/notebook_result.out" % kSCRIPT_WORKING_DIRECTORY, 'wb')

    # Copy Result File.
    shutil.copyfileobj(source_file, destination_file)

def update_notebook_counter():
    global kNOTEBOOK_INDEX
    with notebook_counter_lock:
        kNOTEBOOK_INDEX += 1
    return kNOTEBOOK_INDEX

##################################### Notebook execution function to run jupyter notebook with "kernel_name" Kernell ##########################################
def execute_notebook_with_kernelname(notebook):
    notebook_name = os.path.relpath(notebook, kCURRENT_WORKING_DIRECTORY)
    temp_output_name = kPAPERMILL_OUTPUT_DIR + notebook_name
    try:
        notebook_start_time = time.time() 
        logging.info("Papermill execution started for {}".format(notebook))
        pm.execute_notebook(notebook, notebook_name, kernel_name="kernelname",)
        notebook_end_time = time.time()

        # Calculate the computation time
        notebook_computation_time = notebook_end_time - notebook_start_time
        index = update_notebook_counter()
        kNOTEBOOK_RESULT_HANDLE.write(f"{index}) {notebook_name} ({notebook_computation_time})\t\t\t PASS\n")
        logging.info("Papermill execution completed successfully for {} and time taken {}.".format(notebook, notebook_computation_time))
    except Exception as e:
        index = update_notebook_counter()
        kNOTEBOOK_RESULT_HANDLE.write(f"{index}) {notebook_name} \t\t\t FAIL\n")
        logging.error("Papermill execution failed for %s.", notebook)
        logging.error("Papermill execution failed: %s", str(e))
    finally:
        logging.info(f"Done with {notebook} execution.")

#### Since running all notebooks at once causing issue. Will run in buckets.
bucket1 = {} 
iterator = 0
print(all_notebooks_list.keys())
for notebook_key in all_notebooks_list.keys():
    iterator += 1
    print("{}) {}".format(iterator, notebook_key))
    if (iterator <= 1):
        bucket1[notebook_key] = all_notebooks_list[notebook_key]
    else:
        print("Invalid Argument.")
        logging.info("Invalid Argument.")

print("Bucket1 list ===> ", bucket1)
logging.info(f"Bucket1 list ===> {bucket1}")


############################################## Execute Bucket1 Notebooks with associated kernel ####################################################
execution_tasks_queue = []
for bucket1_key in bucket1.keys():
    if (bucket1[bucket1_key] == "kernelname"):
        execution_tasks_queue.append(dask.delayed(execute_notebook_with_automation_py37_kernel)(bucket1_key))
    else:
        print("Invalid Kernel Specified.")
        logging.info("Bucket1 === Invalid Kernel Specified.")

print("Bucket1 Tasks are ", len(execution_tasks_queue), execution_tasks_queue)
logging.info("Bucket1 Tasks are {} and details are {}".format(len(execution_tasks_queue), execution_tasks_queue))
results = dask.compute(execution_tasks_queue)
print("Bucket1 Tasks are executed successfully.")
logging.info(f"Bucket1 Tasks are executed successfully.")
execution_tasks_queue.clear()

kNOTEBOOK_RESULT_HANDLE.close()
# Stop the timer
script_end_time = time.time()

# Calculate the elapsed time
computation_time = script_end_time - script_start_time
logging.info(f"Papermill execution time taken {computation_time} seconds")
print(f"Papermill execution time taken {computation_time} seconds")
copy_notebook_result_files()
print("Done with all notebook execution")
logging.info(f"Done with all notebook execution")
