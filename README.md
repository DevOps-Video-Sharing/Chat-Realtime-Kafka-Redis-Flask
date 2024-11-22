# Chat-Realtime-Kafka-Redis-Flask
curl http://192.168.2.240:15001/chat/event123


curl -X POST http://192.168.2.240:15001/chat \
                   -H "Content-Type: application/json" \
                   -d '{
                 "username": "Alice",
                 "message": "Hello, world!",
                 "eventCode": "event123"
               }'
