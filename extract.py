import requests
from secret import client_id, client_secret, refresh_token
import pandas as pd
import sqlite3

#EXTRACT

#First you'll need to authenticate by using pasting this into your browser URL
# https://www.strava.com/oauth/authorize?client_id=[YOUR CLIENT ID]&response_type=code&redirect_uri=[YOUR APP URL]&approval_prompt=force&scope=activity:read
#copy the code in the resulting URL and make a post request 
# https://www.strava.com/api/v3/oauth/token?client_id=[YOUR CLIENT ID]&client_secret=[YOUR CLIENT SECRET]&code=[CODE]&grant_type=authorization_code 
# to get the refresh token


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


## SCHEDULING

