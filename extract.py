import requests
#from secret import CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN
import pandas as pd
import sqlite3
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime

#EXTRACT

#First you'll need to authenticate by using pasting this into your browser URL
# https://www.strava.com/oauth/authorize?client_id=[YOUR CLIENT ID]&response_type=code&redirect_uri=[YOUR APP URL]&approval_prompt=force&scope=activity:read
#copy the code in the resulting URL and make a post request 
# https://www.strava.com/api/v3/oauth/token?client_id=[YOUR CLIENT ID]&client_secret=[YOUR CLIENT SECRET]&code=[CODE]&grant_type=authorization_code 
# to get the refresh token


#load_dotenv() 

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
refresh_token = os.getenv('REFRESH_TOKEN')
# google_ip = os.getenv('GOOGLE_PUBLIC_IP_ADDRESS')
# username = os.getenv('USERNAME')
# password = os.getenv('PASSWORD')


url = 'https://www.strava.com/api/v3/oauth/token'

data = {
    'client_id': client_id,
    'client_secret': client_secret,
    'grant_type': 'refresh_token',
    'refresh_token': refresh_token
}

response = requests.post(url, data=data)

# Check if the request was successful
if response.status_code == 200:
    results = response.json()
    access_token = results['access_token']
    print(f"Successfully refreshed access token: {access_token}")
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
else:
    raise Exception(f"Failed to refresh access token: {response.status_code}, {response.text}")


per_page = 200 #max activities per API call
page = 1
all_activities = []

activities_url = 'https://www.strava.com/api/v3/athlete/activities'

while True:
    params = {
        'per_page': per_page, 
        'page': page
    }

    activities_response = requests.get(activities_url, headers=headers, params=params)

    if activities_response.status_code == 200:
        activities = activities_response.json()

        if not activities: 
            break 

        all_activities.extend(activities) 
        print(f"Fetched {len(activities)} activities.")

        page +=1 

    else:
        print(f"Failed to fetch activities: {activities_response.status_code}, {activities_response.text}")

#checking outpout
for activity in all_activities:
    print(f"Activity ID: {activity['id']}, Name: {activity['name']}, Sports Type: {activity['sport_type']}, Date: {activity['start_date']}")

df=pd.DataFrame(all_activities)


#LOAD

# # CONNECT TO GOOGLE CLOUD SQL (MySQL)
# db_connection = mysql.connector.connect(
#     host= google_ip,  # Get from Google Cloud SQL
#     user= username,
#     password= password,
#     database="strava_db"  
# )
# cursor = db_connection.cursor()

# # CREATE TABLE (if not exists)
# cursor.execute('''
# CREATE TABLE IF NOT EXISTS activities (
#     activity_id BIGINT PRIMARY KEY,
#     name VARCHAR(255),
#     type VARCHAR(50),
#     start_date DATETIME,
#     distance FLOAT,
#     moving_time INT,
#     elapsed_time INT,
#     average_speed FLOAT,
#     max_speed FLOAT
# )
# ''')
# db_connection.commit()
# print("Database and table created successfully.")


# # INSERT DATA INTO CLOUD SQL
# for index, row in df.iterrows():
#     activity_id = int(row['id'])
#     start_date = datetime.strptime(row['start_date'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

#     cursor.execute("SELECT COUNT(*) FROM activities WHERE activity_id = %s", (activity_id,))
#     result = cursor.fetchone()

#     if result[0] == 0:
#         cursor.execute('''
#             INSERT INTO activities (activity_id, name, type, start_date, distance, moving_time, elapsed_time, average_speed, max_speed)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#         ''', (
#             row['id'],
#             row['name'],
#             row['sport_type'],
#             start_date,
#             row['distance'],
#             row['moving_time'],
#             row['elapsed_time'],
#             row['average_speed'],
#             row['max_speed']
#         ))
#         db_connection.commit()
#         print(f"Inserted activity ID {activity_id}")
#     else:
#         print(f"Activity ID {activity_id} already exists. Skipping insert.")

# cursor.close()
# db_connection.close()
# print("Data successfully uploaded to Google Cloud SQL.")


conn = sqlite3.connect('strava.db')
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS activities (
    activity_id INTEGER PRIMARY KEY,
    name TEXT,
    type TEXT,
    start_date TEXT,
    distance REAL,
    moving_time INTEGER,
    elapsed_time INTEGER,
    average_speed REAL,
    max_speed REAL
)
''')
conn.commit()
print("database created successfully")


for index, row in df.iterrows():
    activity_id = int(row['id'])  
    
    # Check if the activity ID already exists in the database
    cur.execute("SELECT COUNT(*) FROM activities WHERE activity_id = ?", (activity_id,))
    result = cur.fetchone()
    
    if result[0] == 0: 
        cur.execute('''
            INSERT OR REPLACE INTO activities (activity_id, name, type, start_date, distance, moving_time, elapsed_time, average_speed, max_speed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['id'],              
            row['name'],            
            row['sport_type'],      
            row['start_date_local'],      
            row['distance'],        
            row['moving_time'],     
            row['elapsed_time'],    
            row['average_speed'],   
            row['max_speed']        
        ))
        conn.commit()
        print(f"Inserted activity ID {activity_id}")

    else:
        print(f"Activity ID {activity_id} already exists. Skipping insert.")
    
conn.close()




