import requests
import json

url = "https://dashboard.smartproxy.com/subscription-api/v1/api/public/statistics/traffic"

# Replace with your actual API token
api_token = "0eafbfc36f0e67f185c82ea4409d986a50a180d85e15bc4c84869f791ae43501f088d2647e31bcb7e6bd9b240ae3c3d34d069bb928403c32f0e0ccaa60452fa056216979c7a591ea062710276ae661ca2a646d3a8a0d"

payload = {
    "proxyType": "residential_proxies",
    "startDate": "2024-09-01 00:00:00",
    "endDate": "2024-10-01 00:00:00",
    "groupBy": "target",
    "limit": 500,
    "page": 1,
    "sortBy": "grouping_key",
    "sortOrder": "asc"
}

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "Authorization": f"Token {api_token}"
}

try:
    # Use POST (not GET) when sending a JSON payload for this endpoint.
    response = requests.post(url, json=payload, headers=headers, timeout=10)
    response.raise_for_status()  # Raises an error for 4xx/5xx responses
    data = response.json()
    print(json.dumps(data, indent=2))
except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
except requests.exceptions.ConnectionError as conn_err:
    print(f"Error connecting: {conn_err}")
except requests.exceptions.Timeout as timeout_err:
    print(f"Timeout error: {timeout_err}")
except requests.exceptions.RequestException as req_err:
    print(f"An error occurred: {req_err}")
