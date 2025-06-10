from pymongo import MongoClient
from datetime import datetime
import os
import time
import socketio
import json
from pymongo import MongoClient
import socketio
import ssl
import os

sio = socketio.Client(ssl_verify=False)  # Disable verification for self-signed certs

mongo_client = MongoClient("mongodb+srv://doadmin:4T81NSqj572g3o9f@db-mongodb-blr1-27716-c2bd0cae.mongo.ondigitalocean.com/admin?tls=true&authSource=admin")
db = mongo_client["nnx"]
atlanta_collection = db['atlanta']
distinct_atlanta_collection = db['distinctAtlanta']
vehicle_inventory_collection = db['vehicle_inventory']

def update_distinct_atlanta():
    try:
        print("Successfully running distinct Vehicle")
        all_documents = list(atlanta_collection.aggregate([
                {"$match": {"gps": "A"}},
                {"$sort": {"date_time": -1}},
                {
                    "$group": {
                        "_id": "$imei",
                        "latest_doc": {"$first": "$$ROOT"}
                    }
                },
                {"$replaceRoot": {"newRoot": "$latest_doc"}}
            ]))
        
        print(f"Fetched {len(all_documents)} documents from the atlanta collection")

        for doc in all_documents:
            doc.pop('_id', None)

        distinct_atlanta_collection.delete_many({})

        if all_documents:
            distinct_atlanta_collection.insert_many(all_documents)

        print('Distinct documents updated successfully')

    except Exception as e:
        print(f'Error updating distinct documents: {str(e)}')

if __name__ == '__main__':
    while True:
        update_distinct_atlanta()
        time.sleep(60)  # Wait for 60 seconds before running the function again