# Chat-Realtime-Kafka-Redis-Flask
curl http://localhost:15001/chat/event123


curl -X POST http://103.9.157.149:15001/chat \
                   -H "Content-Type: application/json" \
                   -d '{
                 "username": "Alice",
                 "message": "Hello, world!",
                 "eventCode": "event123"
               }'
