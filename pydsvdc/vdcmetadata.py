import pymongo
from pymongo import MongoClient
from datetime import datetime, timezone
import re
import os

def get_mongo_conn_str():
    mongo_cred = os.environ.get("VDC_CREDENTIALS", None)
    mongo_sock = os.environ.get("VDC_SOCKET", None) 
    if mongo_cred is None or mongo_sock is None:
        print("ERROR: please set environment variables VDC_CREDENTIALS and VDC_SOCKET")
        return(None)
    return f'mongodb://{mongo_cred}@{mongo_sock}/?authSource=api'

def find_nearest_times_with_limit_and_sort(
    platform_identifier, start_time, limit=1, sort_direction="ascending"
):
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None

    # Create a MongoClient instance to connect to the MongoDB server
    client = MongoClient(uri)

    try:
        # Access the "api" database and "observation" collection
        db = client["api"]
        collection = db["observation"]

        # Replace the "." with "3" and "6" based on user input to handle variations in platform identifiers
        if "RadM" in platform_identifier:
            platform_identifier_replaced = platform_identifier.replace(".", "[36]")
        elif "FDCC" in platform_identifier:
            platform_identifier_replaced = platform_identifier.replace(".", "[46]")
        else:
            # Handle other cases or raise an error if needed
            platform_identifier_replaced = platform_identifier

        results = []

        # Convert the start_time parameter to a UTC datetime object
        start_time_utc = start_time.astimezone(timezone.utc)

        # Construct the query to find documents matching the modified platform identifier and start time
        query = {
            "Platform Identifier": {"$regex": platform_identifier_replaced},
            "Observation Start Date & Time": {"$gte": start_time_utc}
        }

        # Define the sort order based on the sort_direction parameter
        sort_order = pymongo.ASCENDING if sort_direction == "ascending" else pymongo.DESCENDING

        if limit is not None:
            # Use the limit parameter to limit the number of results returned and sort by observation start time
            cursor = collection.find(query).sort("Observation Start Date & Time", sort_order).limit(limit)
        else:
            # If limit is None, retrieve all matching documents from the given point onwards and sort by observation start time
            cursor = collection.find(query).sort("Observation Start Date & Time", sort_order)

        # Retrieve data points and store them in a list of tuples
        for document in cursor:
            file_name = document["File Name"]
            index = document["Index"]
            results.append((file_name, index))

        return results

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # Close the MongoDB client connection
        client.close()

def get_entries_between_times(database_uri, start_time, end_time, platform_identifier):
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None

    # Create a MongoClient instance with the provided URI
    client = MongoClient(uri)

    # Access the "api" database and "observation" collection
    db = client["api"]
    collection = db["observation"]

    # Replace the "." with "3" and "6" based on user input
    if "RadM" in platform_identifier:
        platform_identifier_replaced = platform_identifier.replace(".", "3")
    elif "FDCC" in platform_identifier:
        platform_identifier_replaced = platform_identifier.replace(".", "6")
    else:
        raise ValueError("Invalid platform identifier")

    # Convert start_time and end_time to datetime objects
    start_datetime = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_datetime = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ")

    # Define the query to filter documents
    query = {
        "Platform Identifier": platform_identifier_replaced,
        "Observation Start Date & Time": {"$gte": start_datetime, "$lte": end_datetime}
    }

    # Projection to include only File Name and Index fields
    projection = {
        "File Name": 1,
        "Index": 1,
        "_id": 0
    }

    # Query the database and retrieve matching documents
    cursor = collection.find(query, projection)

    # Create a list of tuples from the matching documents
    results = [(doc["File Name"], doc["Index"]) for doc in cursor]

    # Close the MongoDB client
    client.close()

    return results

def find_extreme_power_between_dates(platform_identifier, start_time, end_time, power_field, extreme_type="maximum", limit=None):
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None

    # Create a MongoClient instance to connect to the MongoDB server
    client = MongoClient(uri)

    # Access the "api" database
    database = client.get_database("api")

    # Access the "observation" collection
    collection = database["observation"]

    # Replace the "." with "3" or "6" based on the user input to handle variations in platform identifiers
    if "RadM" in platform_identifier:
        platform_identifier = platform_identifier.replace(".", "[36]")
    elif "FDCC" in platform_identifier:
        platform_identifier = platform_identifier.replace(".", "[46]")

    # Define the query based on the start and end times, platform identifier, and power field
    query = {
        "Observation Start Date & Time": {"$gte": start_time},
        "Observation End Date & Time": {"$lte": end_time},
        "Platform Identifier": {"$regex": platform_identifier}
    }

    # Find documents matching the query and sort them by the specified power field in ascending or descending order
    cursor = collection.find(query).sort(power_field, -1 if extreme_type == "maximum" else 1)

    # If limit is specified, apply it; otherwise, retrieve all matching documents
    if limit:
        cursor = cursor.limit(limit)

    # Initialize a list to store the tuples
    results = []

    # Iterate through the results and find the extreme power values
    for document in cursor:
        file_name = document.get("File Name", "N/A")
        index = document.get("Index", "N/A")
        power_value = document.get(power_field, "N/A")
        results.append((file_name, index))

    # Close the MongoDB connection
    client.close()

    return results

def sort_and_filter_power_values(platform_identifier, start_time, end_time, power_field, extreme_type="maximum", value=None, limit=None):
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None
    
    # Create a MongoClient instance to connect to the MongoDB server
    client = MongoClient(uri)

    # Access the "api" database
    database = client.get_database("api")

    # Access the "observation" collection
    collection = database["observation"]

    # Replace the "." with "3" or "6" based on the user input to handle variations in platform identifiers
    if "RadM" in platform_identifier:
        platform_identifier = platform_identifier.replace(".", "[36]")
    elif "FDCC" in platform_identifier:
        platform_identifier = platform_identifier.replace(".", "[46]")

    # Define the query based on the start and end times, platform identifier, and power field
    query = {
        "Observation Start Date & Time": {"$gte": start_time},
        "Observation End Date & Time": {"$lte": end_time},
        "Platform Identifier": {"$regex": platform_identifier}
    }

    # Define the sorting order based on extreme_type
    sort_order = -1 if extreme_type == "maximum" else 1

    # Define the value comparison operator based on extreme_type
    value_operator = "$gte" if extreme_type == "maximum" else "$lte"

    # If a value is provided, add it to the query
    if value is not None:
        query[power_field] = {value_operator: value}

    # Find documents matching the query and sort them by the specified power field
    cursor = collection.find(query).sort(power_field, sort_order)

    # If limit is specified, apply it; otherwise, retrieve all matching documents
    if limit:
        cursor = cursor.limit(limit)

    # Initialize a list to store the tuples
    results = []

    # Iterate through the results and find the matching power values
    for document in cursor:
        file_name = document.get("File Name", "N/A")
        index = document.get("Index", "N/A")
        power_value = document.get(power_field, "N/A")
        results.append((file_name, index))

    # Close the MongoDB connection
    client.close()

    return results

def find_earliest_and_latest_start_times(platform_identifier):
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None
    
    # Create a MongoClient instance to connect to the MongoDB server
    client = MongoClient(uri)

    # Access the "api" database
    database = client.get_database("api")

    # Access the "observation" collection
    collection = database["observation"]

    # Define the query based on the platform identifier
    query = {
        "Platform Identifier": platform_identifier
    }

    # Sort the documents by start time in ascending order to find the earliest start time
    cursor = collection.find(query).sort("Observation Start Date & Time", 1)

    # Initialize variables to track earliest and latest start times
    earliest_start_time = None
    latest_start_time = None

    # Iterate through the cursor and track the start times
    for document in cursor:
        start_time = document.get("Observation Start Date & Time")
        if not earliest_start_time or start_time < earliest_start_time:
            earliest_start_time = start_time
        if not latest_start_time or start_time > latest_start_time:
            latest_start_time = start_time

    # Close the MongoDB connection
    client.close()

    # Check if any matching documents were found
    if earliest_start_time is not None and latest_start_time is not None:
        return earliest_start_time, latest_start_time
    else:
        return "N/A", "N/A"

def insert_data_into_mongodb():
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None

    # Create a MongoClient instance
    client = MongoClient(uri)

    # Access the "api" database
    database = client.get_database("api")

    # Access the "observation" collection
    collection = database["observation"]

    # Prompt the user for input
    file_name = input("Enter File Name: ")
    timestamp = input("Enter Observation Start Date & Time (e.g., 2023-09-12T14:30:00): ")
    index = input("Enter Index: ")
    
    # Parse metadata as a dictionary
    metadata = {}
    while True:
        key = input("Enter Metadata Key (leave blank to finish): ")
        if not key:
            break
        value = input(f"Enter Metadata Value for {key}: ")
        metadata[key] = value

    # Create a document to insert into MongoDB
    document = {
        "File Name": file_name,
        "Observation Start Date & Time": timestamp,
        "Index": index,
        "Metadata": metadata
    }

    # Insert the document into the collection
    collection.insert_one(document)

    # Close the MongoDB connection
    client.close()

def retrieve_data_from_mongodb(file_name, timestamp, index):
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None

    # Create a MongoClient instance
    client = MongoClient(uri)

    # Access the "api" database
    database = client.get_database("api")

    # Access the "observation" collection
    collection = database["observation"]

    # Define the query based on the provided criteria
    query = {
        "File Name": file_name,
        "Observation Start Date & Time": timestamp,
        "Index": index
    }

    # Find documents matching the query
    cursor = collection.find(query)

    # Initialize a list to store the results
    results = []

    # Iterate through the results and append them to the list
    for document in cursor:
        results.append(document)

    # Close the MongoDB connection
    client.close()

    return results

def find_earliest_and_latest_start_times(platform_identifier):
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None
   
    # Create a MongoClient instance
    client = MongoClient(uri)

    # Access the "api" database
    database = client.get_database("api")

    # Access the "observation" collection
    collection = database["observation"]

    # Define the query based on the platform identifier
    query = {
        "Platform Identifier": platform_identifier
    }

    # Sort the documents by start time in ascending order to find the earliest start time
    cursor = collection.find(query).sort("Observation Start Date & Time", 1)

    # Initialize variables to track earliest and latest start times
    earliest_start_time = None
    latest_start_time = None

    # Iterate through the cursor and track the start times
    for document in cursor:
        start_time = document.get("Observation Start Date & Time")
        if not earliest_start_time or start_time < earliest_start_time:
            earliest_start_time = start_time
        if not latest_start_time or start_time > latest_start_time:
            latest_start_time = start_time

    # Close the MongoDB connection
    client.close()

    return earliest_start_time, latest_start_time

def find_dates_within_range(platform_identifier, start_date, end_date):
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None

    # Create a MongoClient instance
    client = MongoClient(uri)

    # Access the "api" database
    database = client.get_database("api")

    # Access the "observation" collection
    collection = database["observation"]

    # Convert user input strings to datetime objects
    start_date = datetime.fromisoformat(start_date)
    end_date = datetime.fromisoformat(end_date)

    # Define the query based on the platform identifier and date range
    query = {
        "Platform Identifier": platform_identifier,
        "Observation Start Date & Time": {
            "$gte": start_date,
            "$lte": end_date
        }
    }

    # Sort the documents by start time in ascending order to find the earliest start time
    cursor = collection.find(query).sort("Observation Start Date & Time", 1)

    # Initialize a list to store the file names and indexes
    file_name_and_index = []

    # Initialize a list to store the tuples with (start_time, file_name, index)
    results = []

    # Iterate through the cursor and collect the dates, file names, and indexes
    for document in cursor:
        start_time = document.get("Observation Start Date & Time")
        file_name = document.get("File Name", "N/A")
        index = document.get("Index", "N/A")

        # Append to the file_name_and_index list
        file_name_and_index.append((file_name, index))

        # Append to the results list with (start_time, file_name, index)
        results.append((start_time, file_name, index))

    # Close the MongoDB connection
    client.close()

    return file_name_and_index, results

def find_earliest_and_latest_start_times(platform_identifier):
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None

    # Create a MongoClient instance
    client = MongoClient(uri)

    # Access the "api" database
    database = client.get_database("api")

    # Access the "observation" collection
    collection = database["observation"]

    # Define the query based on the platform identifier
    query = {
        "Platform Identifier": platform_identifier
    }

    # Sort the documents by start time in ascending order to find the earliest start time
    cursor = collection.find(query).sort("Observation Start Date & Time", 1)

    # Initialize variables to track earliest and latest start times
    earliest_start_time = None
    latest_start_time = None

    # Iterate through the cursor and track the start times
    for document in cursor:
        start_time = document.get("Observation Start Date & Time")
        if not earliest_start_time or start_time < earliest_start_time:
            earliest_start_time = start_time
        if not latest_start_time or start_time > latest_start_time:
            latest_start_time = start_time

    # Close the MongoDB connection
    client.close()

    return earliest_start_time, latest_start_time

def find_dates_within_range(platform_identifier, start_date, end_date):
    # Connection URI for MongoDB
    uri = get_mongo_conn_str()
    if uri is None:
        return None

    # Create a MongoClient instance
    client = MongoClient(uri)

    # Access the "api" database
    database = client.get_database("api")

    # Access the "observation" collection
    collection = database["observation"]

    # Convert user input strings to datetime objects
    start_date = datetime.fromisoformat(start_date)
    end_date = datetime.fromisoformat(end_date)

    # Define the query based on the platform identifier and date range
    query = {
        "Platform Identifier": platform_identifier,
        "Observation Start Date & Time": {
            "$gte": start_date,
            "$lte": end_date
        }
    }

    # Sort the documents by start time in ascending order to find the earliest start time
    cursor = collection.find(query).sort("Observation Start Date & Time", 1)

    # Initialize a list to store the dates and associated file names with indexes within the range
    results = []

    # Iterate through the cursor and collect the dates, file names, and indexes
    for document in cursor:
        start_time = document.get("Observation Start Date & Time")
        file_name = document.get("File Name", "N/A")
        index = document.get("Index", "N/A")
        results.append((start_time, file_name, index))

    # Close the MongoDB connection
    client.close()

    return results
