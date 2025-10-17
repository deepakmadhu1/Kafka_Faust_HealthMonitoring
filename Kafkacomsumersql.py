import mysql.connector
import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

# Kafka setup
consumer = KafkaConsumer(
    'patient_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Constants
HIGH_HEART_RATE_THRESHOLD = 100
HIGH_BP_SYSTOLIC_THRESHOLD = 130
HIGH_GLUCOSE_THRESHOLD = 120

# Function to establish a database connection
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            port=3310,
            user="root",
            password="Thrissur@123",
            database="data"
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

# Initialize MySQL connection
conn = get_db_connection()
if conn is None:
    raise Exception("Failed to connect to MySQL database")
cursor = conn.cursor()

# Create table with separate id column as PRIMARY KEY
cursor.execute('''
    CREATE TABLE IF NOT EXISTS patient_status (
        id INT AUTO_INCREMENT PRIMARY KEY,
        patient_id INT,
        timestamp DATETIME,
        heart_rate INT,
        systolic_bp INT,
        diastolic_bp INT,
        glucose_level INT,
        high_heart_rate BOOLEAN,
        high_bp BOOLEAN,
        high_glucose BOOLEAN,
        alert BOOLEAN,
        alert_occurrence INT,
        sport_mode BOOLEAN
    )
''')
conn.commit()

def check_alert_conditions(heart_rate, systolic, glucose):
    return (heart_rate > HIGH_HEART_RATE_THRESHOLD or
            systolic > HIGH_BP_SYSTOLIC_THRESHOLD or
            glucose > HIGH_GLUCOSE_THRESHOLD)

def insert_patient_status(data):
    now = datetime.now()

    high_heart_rate = data['heart_rate'] > HIGH_HEART_RATE_THRESHOLD
    high_bp = data['systolic_bp'] > HIGH_BP_SYSTOLIC_THRESHOLD
    high_glucose = data['glucose_level'] > HIGH_GLUCOSE_THRESHOLD
    alert = check_alert_conditions(data['heart_rate'], data['systolic_bp'], data['glucose_level'])

    cursor.execute('''
        INSERT INTO patient_status (patient_id, timestamp, heart_rate, systolic_bp, diastolic_bp, glucose_level,
                                    high_heart_rate, high_bp, high_glucose, alert, alert_occurrence, sport_mode)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        data['patient_id'], now, data['heart_rate'], data['systolic_bp'], data['diastolic_bp'], data['glucose_level'],
        high_heart_rate, high_bp, high_glucose, alert, 0, data['sport_mode']
    ))
    conn.commit()

def send_alert_data_to_kafka(patient_id, alert_status):
    message = {
        "patient_id": patient_id,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "alert_status": alert_status
    }
    producer.send('patient_alert_data', value=message)

def process_stream():
    for message in consumer:
        data = message.value
        insert_patient_status(data)
        if data['alert']:
            send_alert_data_to_kafka(data['patient_id'], data['alert'])

if __name__ == '__main__':
    process_stream()
