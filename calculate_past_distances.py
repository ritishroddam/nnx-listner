from pymongo import MongoClient
from datetime import datetime, timedelta
import time
import os

mongo_client = MongoClient("mongodb+srv://doadmin:4T81NSqj572g3o9f@db-mongodb-blr1-27716-c2bd0cae.mongo.ondigitalocean.com/admin?tls=true&authSource=admin")
db = mongo_client["nnx"]

atlanta_collection = db['atlanta']
distance_travelled_collection = db['distanceTravelled']

def calculate_distance_for_past_days():
    try:
        today = datetime.now().strftime('%d%m%y')
        past_days = atlanta_collection.distinct('date', {'date': {'$ne': today}})

        for date_str in past_days:
            # Check if the date already exists in the distanceTravelled collection
            if distance_travelled_collection.find_one({"date": date_str}):
                # print(f'Skipping calculation for {date_str} as it already exists in the distanceTravelled collection.')
                continue

            imei_data = atlanta_collection.find({'date': date_str}).distinct('imei')
            total_distance = 0

            for imei in imei_data:
                records = list(atlanta_collection.find({'date': date_str, 'imei': imei}).sort('time'))
                if len(records) >= 2:
                    start_odometer = float(records[0].get('odometer', 0))
                    end_odometer = float(records[-1].get('odometer', 0))
                    distance = end_odometer - start_odometer
                    total_distance += distance

            distance_travelled_collection.update_one(
                {"date": date_str},
                {"$set": {"totalDistance": total_distance}},
                upsert=True
            )
            print(f'Updated total distance for {date_str}: {total_distance} km')

        print('Past distances calculated successfully')
    except Exception as e:
        print(f'Error calculating past distances: {str(e)}')

if __name__ == '__main__':
    while True:
        calculate_distance_for_past_days()
        time.sleep(12*60*60)
    