import csv
import os
from dotenv import load_dotenv
import requests

load_dotenv()
BEARER_TOKEN = os.getenv("COINCAP_KEY")

# Read data from API
url = "https://rest.coincap.io/v3/exchanges"
headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}
response = requests.get(url, headers=headers)
data = response.json()["data"]

# Write data to CSV
with open("exchanges.csv", "w") as f:
    fieldnames = [
        "exchangeId",
        "name",
        "rank",
        "percentTotalVolume",
        "volumeUsd",
        "tradingPairs",
        "socket",
        "exchangeUrl",
        "updated",
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for each in data:
        writer.writerow(each)