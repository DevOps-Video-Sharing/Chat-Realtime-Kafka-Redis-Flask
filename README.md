# Chat-Realtime-Kafka-Redis-Flask
curl http://192.168.2.240:32408/chat/event1234


curl -X POST http://192.168.2.240:32408/chat \
                   -H "Content-Type: application/json" \
                   -d '{
                 "username": "Bao",
                 "message": "Test",
                 "eventCode": "event1234"
               }'
