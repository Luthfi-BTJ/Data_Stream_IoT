# Data_Stream_IoT
Simple project to simulate IoT sensor data for Apache Kafka and storing proccessed data into PostgreSQL

## Requirements
- Python 3.8+
- Apache Kafka
- PostgreSQL
- Python Package (see requirements.txt)

## Project Structures
- main.py = Controller script to run all the program at once.
- producer_IoT.py = Generating dummy IoT sensor data for Apache Kafka.
- consumer_IoT.py = Process messages from Apache Kafka and inserting it into PostgreSQL.

## How to run the program
Before running the program, make sure your Apache Kafka and PostgreSQL running.

Install dependencies in your python by run:
`bash
pip install -r requirements.txt

There are two ways to run the program (all of them could be run on Terminal):
  1. Run the program separately:
     - Run "python producer_IoT.py" to produce dummy data.
     - Run "python consumer_IoT.py" for message processing.

  2. Run all the program:
     - Run "python main.py"
