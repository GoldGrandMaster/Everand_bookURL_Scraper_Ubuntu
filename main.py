import pymongo
import concurrent.futures
import subprocess
import shutil
import os

# Replace these variables with your MongoDB connection details
mongo_host = "127.0.0.1"
mongo_port = 27017  # Default MongoDB port
mongo_database = "bookUrlList"
mongo_collection = "books"

# Create a MongoDB client
client = pymongo.MongoClient(host=mongo_host, port=mongo_port)
database = client[mongo_database]
collection = database[mongo_collection]

# Function to run script2.py and return "success" or "failed"
def run_script(argument):
    try:
        # Execute run.py
        book_id = argument["id"]
        book_title = argument["title"]
        book_url = argument["url"]

        process = subprocess.Popen(["python3", "run.py", book_url], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.communicate()

        # Check if run.py ran successfully
        if process.returncode == 0:
            return f"successfully download {book_id} {book_title}"
        else:
            return f"failed download {book_id} {book_title}"
    except Exception as e:
        return f"failed exception {book_id} {book_title}"

total_documents = collection.count_documents({})
batch_size = 500  # Process documents in batches of 500

# Number of instances to run concurrently
concurrent_limit = 100

# Create a ProcessPoolExecutor with the concurrent limit
for offset in range(0, total_documents, batch_size):
    batch = collection.find().skip(offset).limit(batch_size)
    with concurrent.futures.ProcessPoolExecutor(max_workers=concurrent_limit) as executor:
        futures = [executor.submit(run_script, arg) for arg in batch]

        # Wait for all tasks to complete and print results
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            print(result)

client.close()
