import producer_IoT
import consumer_IoT

def main():
    # Start the IoT producer
    producer_IoT.sensor_data_generator()

    # Start the IoT consumer
    consumer_IoT.run_consumer()

if __name__ == "__main__":
    main()