from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
import redis
import json
from flask_cors import CORS
app = Flask(__name__)
CORS(app)
# Cấu hình Redis
redis_client = redis.StrictRedis(host='10.40.0.10', port=6379,password='123456' ,decode_responses=True)

# Cấu hình Kafka
KAFKA_BROKER = '192.168.120.131:9092'
KAFKA_TOPIC = 'chat-messages'
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
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


@app.route('/streams', methods=['POST'])
def save_stream():
    data = request.json
    stream_key = data.get('streamKey')
    user_name = data.get('userName')
    titleLive = data.get('titleLive')

    if not (stream_key and user_name and titleLive):
        return jsonify({'error': 'Missing required fields: streamKey, userName, or titleLive'}), 400

    # Tạo object lưu trong Redis
    stream_data = {
        'userName': user_name,
        'titleLive': titleLive
    }

    # Lưu dữ liệu dưới dạng JSON
    redis_client.set(f"stream:{stream_key}", json.dumps(stream_data))

    return jsonify({'message': 'Stream information saved successfully'}), 200


# API GET: Lấy tất cả streamKey cùng thông tin userName và titleLive từ Redis
@app.route('/streams/getall', methods=['GET'])
def get_streams():
    # Lấy tất cả các key có dạng "stream:*"
    keys = redis_client.keys("stream:*")
    streams = []

    for key in keys:
        # Lấy dữ liệu JSON từ Redis và parse về dict
        stream_data = json.loads(redis_client.get(key))
        streams.append({
            'streamKey': key.split("stream:")[1],  # Tách bỏ prefix "stream:"
            'userName': stream_data['userName'],
            'titleLive': stream_data['titleLive']
        })

    return jsonify({'streams': streams}), 200

@app.route('/streams/delete', methods=['POST'])
def delete_stream():
    data = request.json
    stream_key = data.get('streamKey')

    if not stream_key:
        return jsonify({'error': 'Missing required field: streamKey'}), 400

    # Xóa dữ liệu từ Redis
    redis_key = f"stream:{stream_key}"
    if redis_client.exists(redis_key):
        redis_client.delete(redis_key)
        return jsonify({'message': f'Stream with key {stream_key} deleted successfully'}), 200
    else:
        return jsonify({'error': f'Stream with key {stream_key} does not exist'}), 404

if __name__ == '__main__':
    app.run(host='192.168.120.213', port=15001, debug=True)
