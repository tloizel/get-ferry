import requests
from google.transit import gtfs_realtime_pb2
from datetime import datetime
import os
import zipfile
import io
import csv

def download_gtfs_data():
    url = "http://nycferry.connexionz.net/rtt/public/utility/gtfs.aspx"
    response = requests.get(url)

    if response.status_code == 200:
        with open('gtfs_data.zip', 'wb') as f:
            f.write(response.content)

        # Unzip the downloaded files
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall('gtfs_data')  # Extract to a folder named gtfs_data

        print("GTFS text files downloaded and extracted successfully.")
    else:
        print(f"Failed to download GTFS data: {response.status_code}")

def get_next_departures_from_feed(stop_ids):
    url = "http://nycferry.connexionz.net/rtt/public/utility/gtfsrealtime.aspx/tripupdate"
    response = requests.get(url)
    feed_departures = {}

    if response.status_code == 200:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        current_time = datetime.now().timestamp()

        for stop_id in stop_ids:
            feed_departures[stop_id] = []  # Initialize list for this stop_id

            for entity in feed.entity:
                if entity.HasField('trip_update'):
                    trip_update = entity.trip_update
                    direction_id = trip_update.trip.direction_id

                    # Only consider trips with direction_id == 0
                    if direction_id == 0:
                        for stop_time_update in trip_update.stop_time_update:
                            if stop_time_update.stop_id == str(stop_id):
                                # Check for future departure times only
                                departure_time = stop_time_update.departure.time
                                if departure_time > current_time:
                                    # Calculate minutes to departure
                                    minutes_to_departure = int((departure_time - current_time) / 60) + 1
                                    feed_departures[stop_id].append({
                                        "trip_id": trip_update.trip.trip_id,
                                        "minutes_to_next_departure": minutes_to_departure,
                                        "source": "Feed"  # Indicate source
                                    })

    return feed_departures

def get_next_departures_from_gtfs(stop_ids):
    next_departures = {}
    current_time = datetime.now().timestamp()

    # Determine if today is a weekend
    current_day = datetime.now().weekday()
    is_weekend = current_day >= 5  # Check if it's Saturday or Sunday

    # Load trips.txt to create a service_id and direction_id mapping
    service_mapping = {}
    with open('gtfs_data/trips.txt', newline='', encoding='utf-8-sig') as csvfile:
        trips_reader = csv.DictReader(csvfile)
        for row in trips_reader:
            service_mapping[row['trip_id']] = {
                'service_id': row['service_id'],
                'direction_id': row['direction_id']
            }

    for stop_id in stop_ids:
        departures = []  # To store upcoming departures for the current stop_id

        # Reset the stop_times_reader for each stop_id
        with open('gtfs_data/stop_times.txt', newline='', encoding='utf-8-sig') as csvfile:
            stop_times_reader = csv.DictReader(csvfile)

            for row in stop_times_reader:
                if row['stop_id'] == str(stop_id):
                    trip_id = row['trip_id']  # Get trip_id for the current stop_time
                    trip_info = service_mapping.get(trip_id)  # Get service_id and direction_id

                    if trip_info:
                        service_id = trip_info['service_id']
                        direction_id = trip_info['direction_id']

                        # Filter based on the current day and direction_id
                        if direction_id == '0' and ((is_weekend and service_id == '2') or (not is_weekend and service_id == '1')):
                            # Convert the departure time from GTFS format to timestamp
                            departure_time = row['departure_time']
                            # Assuming departure_time format is 'HH:MM:SS'
                            departure_timestamp = datetime.strptime(departure_time, '%H:%M:%S').replace(
                                year=datetime.now().year,
                                month=datetime.now().month,
                                day=datetime.now().day
                            ).timestamp()

                            # Check for future departure times only
                            if departure_timestamp > current_time:
                                # Calculate minutes to departure
                                minutes_to_departure = int((departure_timestamp - current_time) / 60) + 1
                                departures.append({
                                    "trip_id": trip_id,
                                    "minutes_to_next_departure": minutes_to_departure,
                                    "source": "GTFS"  # Indicate source
                                })

        # Sort departures by minutes and keep the next two
        departures.sort(key=lambda x: x['minutes_to_next_departure'])
        next_departures[stop_id] = departures[:2]  # Include the next two departures

    return next_departures

def get_combined_departures(stop_ids):
    feed_departures = get_next_departures_from_feed(stop_ids)
    gtfs_departures = get_next_departures_from_gtfs(stop_ids)
    combined_results = {}

    for stop_id in stop_ids:
        combined_results[stop_id] = []
        feed_trip_ids = set()  # Keep track of trip_ids from the feed

        # Use feed data if available
        if stop_id in feed_departures and feed_departures[stop_id]:
            combined_results[stop_id].extend(feed_departures[stop_id])
            # Collect trip_ids from feed results to avoid duplicates
            feed_trip_ids.update(trip['trip_id'] for trip in feed_departures[stop_id])

        # Now add GTFS data only if it's not already included from feed data
        if stop_id in gtfs_departures:
            for gtfs_trip in gtfs_departures[stop_id]:
                if gtfs_trip['trip_id'] not in feed_trip_ids:
                    combined_results[stop_id].append(gtfs_trip)

    return combined_results


# Example usage
download_gtfs_data()  # Download the GTFS data

# Get combined next departures from feed and GTFS data
combined_result = get_combined_departures([4, 90])  # Check both stop IDs

# Print the combined results
print("Combined Results:", combined_result)
