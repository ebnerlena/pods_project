# Data Preprocessing for PODS
# https://github.com/statsbomb/open-data/tree/master/data/events

import os
import json
import pandas as pd
from datetime import datetime, timedelta

# Function to extract data from a single file
def process_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        # print(data)

    events = data
    match_id = file_path.split("/").pop().split(".json")[0]

    rows = []
    for event in events:
        case_id = event['id']
        action = event['type']['name']
        event_type = event['type']['id']
        play_pattern = event['play_pattern']['name']
        recipient = event.get('pass', {}).get('recipient', {}).get('name', None)

        # Timestamps
        start_time = event['timestamp']
        timestamp_str = event['timestamp']
        timestamp = datetime.strptime(timestamp_str, "%H:%M:%S.%f")

        duration = event.get('duration', 0.0)
        # Calculate end_time by adding duration to timestamp
        end_time = timestamp + timedelta(seconds=duration)

        period = event['period']
        duration = event.get('duration', 0.0)
        possession_team = event['possession_team']['name']
        team_action = event['team']['name']

        # Check if 'player' key exists in the event dictionary
        player_data = event.get('player', {})
        player = player_data.get('name', None)

        body_part = event.get('body_part', {}).get('name', None)

        start_location = event.get('location', [None, None])
        start_x = start_location[0]
        start_y = start_location[1]

        # Access 'end_location' based on the event type
        if action == 'Carry':
            end_location = event.get('carry', {}).get('end_location', [None, None])
        elif action == 'Pass':
            end_location = event.get('pass', {}).get('end_location', [None, None])
        else:
            end_location = [None, None]
        
        end_x = end_location[0]
        end_y = end_location[1]

        result = event.get('outcome', {}).get('name', None)

        row = [case_id, action, event_type, play_pattern, recipient, start_time, end_time, period, duration,
               possession_team, team_action, player, body_part, start_x, start_y, end_x, end_y, result, match_id]

        rows.append(row)

    return rows

# Function to process all files in a directory
def process_directory(directory_path):
    all_rows = []
    for filename in os.listdir(directory_path):
        print(filename)
        if filename.endswith('.json'):
            file_path = os.path.join(directory_path, filename)
            all_rows.extend(process_file(file_path))

    return all_rows

# Specify the directory containing the JSON files
data_directory = './open-data/data/events'

# Process all files in the directory
rows = process_directory(data_directory)

# Define column names
columns = ['case_id', 'action', 'type', 'play_pattern', 'recipient', 'start_time', 'end_time', 'period', 'duration',
           'possession_team', 'team_action', 'player', 'body_part', 'start_x', 'start_y', 'end_x', 'end_y', 'result', 'match_id']

# Create a DataFrame
df = pd.DataFrame(rows, columns=columns)

# Display the DataFrame
# print(df)

# Write file

df.to_csv("./data.csv", index=False)


