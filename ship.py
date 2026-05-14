import boto3, requests, time, os

aws_region = os.environ["AWS_REGION"]
log_group = os.environ["LOG_GROUP"]
loki_url = os.environ["LOKI_URL"]

client = boto3.client("logs", region_name=aws_region)
start_time = int((time.time() - 3600) * 1000)

while True:
    try:
        paginator = client.get_paginator("filter_log_events")
        pages = paginator.paginate(logGroupName=log_group, startTime=start_time, PaginationConfig={"MaxItems": 100})
        entries = []
        last_time = start_time
        for page in pages:
            for event in page["events"]:
                ts = str(event["timestamp"] * 1000000)
                entries.append([ts, event["message"]])
                if event["timestamp"] > last_time:
                    last_time = event["timestamp"]
        if entries:
            payload = {"streams": [{"stream": {"job": "cloudwatch", "log_group": log_group}, "values": entries}]}
            requests.post(loki_url + "/loki/api/v1/push", json=payload)
            print(f"Pushed {len(entries)} logs")
            start_time = last_time + 1
        time.sleep(30)
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(10)