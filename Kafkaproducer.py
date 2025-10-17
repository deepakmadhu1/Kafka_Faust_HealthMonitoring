from datetime import datetime
import random
import time
import json
from kafka import KafkaProducer

# Constants
NUM_PATIENTS = 1000
HEART_RATE_VARIATION = 10
BP_VARIATION = 5
MAX_HEART_RATE = 200
MIN_HEART_RATE = 50
MAX_SYSTOLIC_BP = 140
MIN_SYSTOLIC_BP = 90
MAX_GLUCOSE_LEVEL = 140
MIN_GLUCOSE_LEVEL = 70
SPORT_MODE_DURATION = 120  # Duration in seconds for sport mode
SPORT_MODE_MAX_HR = 150    # Max heart rate in sport mode
HIGH_HEART_RATE_THRESHOLD = 100
HIGH_BP_SYSTOLIC_THRESHOLD = 130
HIGH_GLUCOSE_THRESHOLD = 120
TIME_STEP = 1  # Time step for simulation in seconds

# Kafka setup
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Initialization
sport_mode_patients = {}
last_blood_pressures = {patient_id: (random.randint(100, 120), random.randint(60, 80))
                        for patient_id in range(1, NUM_PATIENTS + 1)}
current_heart_rates = {patient_id: random.randint(60, 100)
                       for patient_id in range(1, NUM_PATIENTS + 1)}
alert_occurrences = {patient_id: 0 for patient_id in range(1, NUM_PATIENTS + 1)}

def simulate_dynamic_heart_rate(patient_id):
    global current_heart_rates, sport_mode_patients

    if patient_id in sport_mode_patients:
        mode_info = sport_mode_patients[patient_id]
        time_in_mode = (datetime.now() - mode_info['start_time']).total_seconds()
        if time_in_mode > SPORT_MODE_DURATION * 2:  # Reset after two cycles
            mode_info['start_time'] = datetime.now()
            time_in_mode = 0

        # Increase heart rate for the first half, then decrease
        if time_in_mode < SPORT_MODE_DURATION:
            current_heart_rates[patient_id] = min(SPORT_MODE_MAX_HR, current_heart_rates[patient_id] + 5)
        else:
            current_heart_rates[patient_id] = max(MIN_HEART_RATE, current_heart_rates[patient_id] - 5)
    else:
        heart_rate_change = random.randint(-HEART_RATE_VARIATION, HEART_RATE_VARIATION)
        current_heart_rates[patient_id] = max(MIN_HEART_RATE, min(MAX_HEART_RATE,
                                      current_heart_rates[patient_id] + heart_rate_change))

    return current_heart_rates[patient_id]

def generate_blood_pressure(patient_id):
    last_systolic, last_diastolic = last_blood_pressures[patient_id]
    systolic_change = random.randint(-BP_VARIATION, BP_VARIATION)
    new_systolic = max(MIN_SYSTOLIC_BP, min(MAX_SYSTOLIC_BP, last_systolic + systolic_change))
    diastolic_change = random.randint(-BP_VARIATION, BP_VARIATION)
    new_diastolic = max(MIN_SYSTOLIC_BP - 40, min(MAX_SYSTOLIC_BP - 40, last_diastolic + diastolic_change))
    last_blood_pressures[patient_id] = (new_systolic, new_diastolic)
    return new_systolic, new_diastolic

def generate_glucose_level():
    return random.randint(MIN_GLUCOSE_LEVEL, MAX_GLUCOSE_LEVEL)

def check_alert_conditions(heart_rate, systolic, glucose):
    return (heart_rate > HIGH_HEART_RATE_THRESHOLD or
            systolic > HIGH_BP_SYSTOLIC_THRESHOLD or
            glucose > HIGH_GLUCOSE_THRESHOLD)

def send_data_to_kafka(patient_id, timestamp, heart_rate, systolic, diastolic, glucose):
    sport_mode = patient_id in sport_mode_patients
    high_heart_rate = heart_rate > HIGH_HEART_RATE_THRESHOLD
    high_bp = systolic > HIGH_BP_SYSTOLIC_THRESHOLD
    high_glucose = glucose > HIGH_GLUCOSE_THRESHOLD
    alert = check_alert_conditions(heart_rate, systolic, glucose)
    if alert:
        alert_occurrences[patient_id] += 1

    message = {
        "patient_id": patient_id,
        "timestamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        "heart_rate": heart_rate,
        "systolic_bp": systolic,
        "diastolic_bp": diastolic,
        "glucose_level": glucose,
        "high_heart_rate": high_heart_rate,
        "high_bp": high_bp,
        "high_glucose": high_glucose,
        "alert": alert,
        "alert_occurrence": alert_occurrences[patient_id],
        "sport_mode": sport_mode
    }
    producer.send('patient_data', value=message)

def activate_sport_mode(patient_id):
    sport_mode_patients[patient_id] = {'start_time': datetime.now()}

def run_simulation():
    try:
        while True:
            timestamp = datetime.now()
            for patient_id in range(1, NUM_PATIENTS + 1):
                heart_rate = simulate_dynamic_heart_rate(patient_id)
                systolic, diastolic = generate_blood_pressure(patient_id)
                glucose = generate_glucose_level()
                send_data_to_kafka(patient_id, timestamp, heart_rate, systolic, diastolic, glucose)
            time.sleep(TIME_STEP)
    except KeyboardInterrupt:
        print("Simulation stopped.")

# Example: Activate sport mode for a specific patient
activate_sport_mode(1)

# Start the simulation
run_simulation()
