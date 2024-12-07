import requests
import json
import pandas as pd

response = requests.get(
  "https://developer.nps.gov/api/v1/activities/parks",
  headers={
    "accept": "application/json"
  },
  params={
    "api_key": "7fRCZjCFt0SLvkSwkYQwhPcqKGsdunZXaho0R7fX",
    # "page": 1,
    # "limit": 1
  }
)

data = response.json()
print(json.dumps(data, indent=4))

# Normalize the JSON data
df = pd.json_normalize(data['data'], 'parks', ['id', 'name'], record_prefix='park_')

# Display the DataFrame
print(df.head())

# Save the DataFrame to a CSV file
df.to_csv('parks_data.csv', index=False)