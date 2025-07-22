from kafka import KafkaConsumer
import json
import psycopg2

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = '5.189.154.248:9092'
KAFKA_TOPIC_NAME = 'luthfi_iot_sensor_data'
KAFKA_CONSUMER_GROUP = 'luthfi_consumer_group'

# PostgreSQL Configuration
POSTGRES_HOST = '5.189.154.248'
POSTGRES_DB = 'postgres_db'
POSTGRES_USER = 'postgres_user'
POSTGRES_PASSWORD = 'postgres_password'

# Kafka Consumer Initialization
consumer = KafkaConsumer(
    KAFKA_TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_CONSUMER_GROUP,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def database_connection():
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        # Create a cursor object
        cur = conn.cursor()
        print("Connected to PostgreSQL database successfully.")
    except Exception as e:  
        print(f"Error connecting to PostgreSQL database: {e}")
        exit()

def message_processing():
    try:
        # Process messages from Kafka
        for message in consumer:
            # Deserialize message
            data = message.value
            print(f"Received message: {data}")
            try:
                # Insert the sensor data into PostgreSQL
                insert_query = ("INSERT INTO luthfi_iot_sensor_reading (device_id, temperature, humidity, timestamp) VALUES (%s, %s, %s, %s)")
                record_to_insert = (data['device_id'], data['temperature'], data['humidity'], data['timestamp'])
                cur.execute(insert_query, record_to_insert)
                
                # Commit the transaction
                conn.commit()
                print(f"Inserted record: {data['device_id']}")
            except Exception as e:
                print(f"Error inserting record into PostgreSQL: {e}")
                conn.rollback() # Rollback in case of error
    except KeyboardInterrupt:
        print("Kafka Consumer stopped.")
    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()
        consumer.close()
        print("Database connection closed.")

# Main function to run the consumer
def run_consumer():
    database_connection()
    print("Kafka Consumer initialized. Listening for messages...")
    message_processing()

# Run the consumer if this script is executed directly
if __name__ == "__main__":
    run_consumer()