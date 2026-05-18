import boto3, requests, time, os, json
from datetime import datetime, timedelta, timezone

aws_region = os.environ["AWS_REGION"]
log_group = os.environ["LOG_GROUP"]
loki_url = os.environ["LOKI_URL"]
fetch_days = int(os.environ.get("FETCH_DAYS", "7"))

STATE_FILE = "/tmp/state.json"

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f).get("start_time", 0)
    except:
        return 0

def save_state(ts):
    with open(STATE_FILE, "w") as f:
        json.dump({"start_time": ts}, f)

def get_initial_start_time():
    return int((datetime.now(timezone.utc) - timedelta(days=fetch_days)).timestamp() * 1000)

client = boto3.client("logs", region_name=aws_region)

saved = load_state()
if saved == 0:
    start_time = get_initial_start_time()
    print(f"Fresh start — fetching last {fetch_days} days")
else:
    start_time = saved
    print(f"Resuming from saved timestamp: {start_time}")

while True:
    try:
        paginator = client.get_paginator("filter_log_events")
        pages = paginator.paginate(
            logGroupName=log_group,
            startTime=start_time,
            PaginationConfig={"MaxItems": 1000}
        )

        entries = []
        last_time = start_time

        for page in pages:
            for event in page["events"]:
                ts = str(event["timestamp"] * 1000000)
                entries.append([ts, event["message"]])
                if event["timestamp"] > last_time:
                    last_time = event["timestamp"]

        if entries:
            payload = {
                "streams": [{
                    "stream": {
                        "job": "cloudwatch",
                        "log_group": log_group
                    },
                    "values": entries
                }]
            }
            response = requests.post(
                loki_url + "/loki/api/v1/push",
                json=payload
            )
            print(f"Pushed {len(entries)} logs → status {response.status_code}")
            start_time = last_time + 1
            save_state(start_time)
        else:
            print(f"No new logs — waiting 30s")

        time.sleep(30)

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(10)


# import boto3, requests, time, os

# aws_region = os.environ["AWS_REGION"]
# log_group = os.environ["LOG_GROUP"]
# loki_url = os.environ["LOKI_URL"]

# client = boto3.client("logs", region_name=aws_region)
# start_time = 0

# while True:
#     try:
#         paginator = client.get_paginator("filter_log_events")
#         pages = paginator.paginate(logGroupName=log_group, startTime=start_time, PaginationConfig={"MaxItems": 100})
#         entries = []
#         last_time = start_time
#         for page in pages:
#             for event in page["events"]:
#                 ts = str(event["timestamp"] * 1000000)
#                 entries.append([ts, event["message"]])
#                 if event["timestamp"] > last_time:
#                     last_time = event["timestamp"]
#         if entries:
#             payload = {"streams": [{"stream": {"job": "cloudwatch", "log_group": log_group}, "values": entries}]}
#             requests.post(loki_url + "/loki/api/v1/push", json=payload)
#             print(f"Pushed {len(entries)} logs")
#             start_time = last_time + 1
#         time.sleep(30)
#     except Exception as e:
#         print(f"Error: {e}")
#         time.sleep(10)