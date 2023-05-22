# Importing required libs
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
import boto3, os, time
import datetime

# Initializing Flask App
app = Flask(__name__)
socketio = SocketIO(app)

ACCESS_KEY=os.environ.get('AWS_ACCESS_KEY')
SECRET_KEY=os.environ.get('AWS_SECRET_KEY')

# Kinesis stream name of consume tweet count
STREAM_NAME = 'TweetStreamOutput'
# Initializing kinesis client
kinesis_client = boto3.client("kinesis",aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,region_name="eu-central-1")

# Reading messaging from kinesis stream
response = kinesis_client.describe_stream(StreamName=STREAM_NAME)
my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

# Loading HTML Page
@app.route('/')
def index():
    return render_template('index.html')

# Checking server connectivity
@socketio.on('connect')
def test_connect():
    emit('after connect', {'data': 'Server Connected!'})

# Sending live messages to frontend HTML page
@socketio.on('stream_event')
def handle_my_event(message):

    try:
        stream_register_response = kinesis_client.register_stream_consumer(
            StreamARN='arn:aws:kinesis:eu-central-1:394715629636:stream/TweetStreamOutput',
            ConsumerName='Twitter_Consumer'
        )
        consumer_arn = stream_register_response['Consumer']['ConsumerARN']
        print("Twitter Consumer registerd")
        time.sleep(5)
    except Exception as e:
        print("Consumer is already registered")

    describe_consumer_response = kinesis_client.describe_stream_consumer(
        StreamARN='arn:aws:kinesis:eu-central-1:394715629636:stream/TweetStreamOutput',
        ConsumerName='Twitter_Consumer'
    )
    print("Describe Consumer", describe_consumer_response)

    # Getting messages from kinesis stream
    subscribe_response = kinesis_client.subscribe_to_shard(
        ConsumerARN=describe_consumer_response['ConsumerDescription']['ConsumerARN'],
        ShardId=my_shard_id,
        StartingPosition={
            'Type': 'LATEST'
        }
    )
    print("Subscribe to shard called!")

    for item in subscribe_response['EventStream']:
        tweet_count = 0
        for record in item['SubscribeToShardEvent']['Records']:
            tweet_count = eval(record['Data'].decode("utf-8"))['tweet_count']
        emit('get_tweet_data', tweet_count, broadcast=True)
        print("tweet count: ", tweet_count)
        socketio.sleep(10)

if __name__ == '__main__':
    # Running flask application
    socketio.run(app, debug=True, host='0.0.0.0')
