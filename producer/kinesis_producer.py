import boto3, json, time, uuid

def send_to_kinesis(stream_name, file_path, interval=0.01):
    kinesis = boto3.client('kinesis', region_name='us-east-1')
    with open(file_path) as f:
        for line in f:
            data = json.loads(line)
            kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey=str(uuid.uuid4())
            )
            print(f"Sent: {data}")
            time.sleep(interval)

if __name__ == "__main__":
    send_to_kinesis('youtube-metrics-stream', './dataset/tiny_dataset.json', interval=0.01)
#send_to_kinesis('youtube-metrics-stream', './dataset/medium_dataset.json', interval=0.01)
    #send_to_kinesis('youtube-metrics-stream', './dataset/large_dataset.json', interval=0.01)

