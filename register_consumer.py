import boto3

STREAM_NAME = 'TweetStreamOutput'
# Initializing kinesis client
kinesis_client = boto3.client("kinesis", aws_access_key_id="AKIAVXZXCRBCOEPN44VS", aws_secret_access_key="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", region_name="eu-central-1")

try:
    stream_register_response = kinesis_client.register_stream_consumer(
        StreamARN='arn:aws:kinesis:eu-central-1:394715629636:stream/TweetStreamOutput',
        ConsumerName='Twitter_Consumer'
    )
    print("Twitter Consumer registerd")
except Exception as e:
    print("Consumer is already registered")