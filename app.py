from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
import redis
import json
from flask_cors import CORS
app = Flask(__name__)
CORS(app)
# Cấu hình Redis
redis_client = redis.StrictRedis(host='localhost', port=6379,password='' ,decode_responses=True)

# Cấu hình Kafka
KAFKA_TOPIC = 'chat-messages'
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='chat-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

from datetime import datetime

@app.route('/chat', methods=['POST'])
def send_message():
    data = request.json
    username = data.get('username')
    message = data.get('message')
    event_code = data.get('eventCode')
    
    if not (username and message and event_code):
        return jsonify({'error': 'Missing required fields'}), 400

    # Tạo payload cho tin nhắn
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    payload = {
        'username': username,
        'message': message,
        'timestamp': timestamp
    }

    # Gửi tin nhắn đến Kafka
    producer.send(KAFKA_TOPIC, {'eventCode': event_code, **payload})
    producer.flush()

    # Lưu tin nhắn vào Redis
    redis_key = f"chat:{event_code}"
    redis_client.rpush(redis_key, json.dumps(payload))

    return jsonify({'message': 'Message sent successfully'}), 200



@app.route('/chat/<event_code>', methods=['GET'])
def get_messages(event_code):
    redis_key = f"chat:{event_code}"
    # Lấy danh sách tin nhắn từ Redis
    messages = redis_client.lrange(redis_key, 0, -1)
    messages = [json.loads(msg) for msg in messages]

    return jsonify({'eventCode': event_code, 'messages': messages}), 200
def kafka_consumer():
    for message in consumer:
        data = message.value
        event_code = data['eventCode']
        redis_key = f"chat:{event_code}"

        # Lưu tin nhắn vào Redis
        redis_client.rpush(redis_key, json.dumps(data))



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=15001, debug=True)
