import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime

# ElasticSearch cluster details
ELASTIC_URL = ""
INDEX = ""

# Authentication details
USERNAME = ""
PASSWORD = ""

# Headers for the request
headers = {
    "Content-Type": "application/json"
}

# Generate the current timestamp
def get_current_timestamp():
    return datetime.now().isoformat() + "Z"

# Search for open alerts
def search_open_alerts(elastic_url, index, username, password, timestamp_gte, timestamp_lte):
    # Payload for searching open alerts within specific timestamp
    search_payload = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "kibana.alert.workflow_status": "open"
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": timestamp_gte,
                                "lte": timestamp_lte
                            }
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "@timestamp": {
                    "order": "desc"
                }
            }
        ],
        "size": 1000    # Search for 1000 alerts 
    }
    
    # Endpoint for searching alerts
    url = f"{elastic_url}/{index}/_search"
    
    try:
        # Send the HTTP POST request to search for open alerts
        response = requests.post(url, headers=headers, data=json.dumps(search_payload), auth=HTTPBasicAuth(username, password))
        
        # Check for successful response
        if response.status_code == 200:
            open_alerts = response.json()
            alert_hits = open_alerts['hits']['total']['value']
            print(f"Total alert matches: {alert_hits}")
            print("Open Alerts:")
            for alert in open_alerts['hits']['hits']:
                alert_id = alert['_id']
                alert_index = alert['_index']
                # alert_time = alert['_source']['@timestamp']
                print(f"Alert ID: {alert_id}, Alert Index: {alert_index}")
                close_alert(elastic_url, alert_index, alert_id, USERNAME, PASSWORD)
            return alert_hits
        else:
            print(f"Failed to search for open alerts. Status Code: {response.status_code}")
            print(f"Response: {response.text}")
            return -1
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return -1


# Close the alert
def close_alert(elastic_url, alert_index, alert_id, username, password):
    # Payload to update the alert workflow status to 'closed' and update the date
    payload = {
        "doc": {
            "kibana.alert.workflow_status": "closed",
            "kibana.alert.workflow_status_updated_at": get_current_timestamp()
        }
    }

    # Endpoint for updating the alert
    url = f"{elastic_url}/{alert_index}/_update/{alert_id}"
    
    try:
        # Send the HTTP POST request to update the alert's workflow status
        response = requests.post(url, headers=headers, data=json.dumps(payload), auth=HTTPBasicAuth(username, password))
        # Check for successful response
        if response.status_code == 200:
            print(f"Alert {alert_id} closed successfully!")
        else:
            print(f"Failed to close alert. Status Code: {response.status_code}")
            print(f"Response: {response.text}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

def start(currentMonth):
    print(f"Current month: {currentMonth}")
    # Format the month properly with leading zero for single digits
    month_str = f"0{currentMonth}" if currentMonth < 10 else str(currentMonth)
    timestamp_gte = f"2024-{month_str}-01T00:00:00"
    timestamp_lte = f"2024-{(currentMonth + 1):02}-01T00:00:00" if currentMonth < 12 else "2025-01-01T00:00:00"
    
    # Call the function to search for open alerts
    alertCount = search_open_alerts(ELASTIC_URL, INDEX, USERNAME, PASSWORD, timestamp_gte, timestamp_lte)

    # Stop when month = 12
    if currentMonth == 12:
        return
    
    # Check if no alerts were found, and move to the next month if true
    if alertCount == 0:
        print(f"No open alerts found for month {currentMonth}. Moving to next month.")
        start(currentMonth + 1)
    else:
        print(f"{alertCount} alerts found for month {currentMonth}. Continuing to process.")
        start(currentMonth)


# Set initial month
month = 1
# Call the function to start    
start(month)
