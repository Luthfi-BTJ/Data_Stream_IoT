from kafka import KafkaProducer
import json
import uuid
import random
import time
import datetime

# Kafka Configuration to http://5.189.154.248:8080
BOOTSTRAP_SERVERS = '5.189.154.248:9092'
TOPIC_NAME = 'luthfi_iot_sensor_data'

# Producer Initialization
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Kafka Producer initialized. Sending messages...")
print("Press Ctrl+C to stop.")

def sensor_data_generator():
    try:
        while True:
            # Generate random sensor data
            device_id = random.choice(['device1', 'device2', 'device3', 'device4', 'device5'])
            temperature = random.uniform(19.0, 32.0)
            humidity = random.uniform(30.0, 70.0)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Create message  
            message = {
                'device_id': device_id,
                'temperature': round(temperature, 2),
                'humidity': round(humidity, 2),
                'timestamp': timestamp
            }
            
            # Send message to Kafka topic
            producer.send(TOPIC_NAME, value=message)
            print(f"Sent message: {message}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        # Close the producer
        producer.close()
        producer.flush()
        print("Producer closed.")

# Run the sensor data generator
if __name__ == "__main__":
    sensor_data_generator()