from flask import Flask, render_template, request
import mysql.connector
from datetime import datetime, timedelta

app = Flask(__name__)

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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    patient_id = request.form.get('patient_id')
    conn = get_db_connection()
    if conn is None:
        return "Database connection error", 500

    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        'SELECT patient_id, timestamp, heart_rate, systolic_bp, diastolic_bp, glucose_level, sport_mode FROM patient_status WHERE patient_id = %s ORDER BY timestamp DESC',
        (patient_id,)
    )
    patient = cursor.fetchone()
    conn.close()
    return render_template('index.html', patient=patient)

@app.route('/condition/<string:condition>')
def condition(condition):
    conn = get_db_connection()
    if conn is None:
        return "Database connection error", 500

    cursor = conn.cursor(dictionary=True)
    query = 'SELECT patient_id, timestamp, heart_rate, systolic_bp, diastolic_bp, glucose_level, sport_mode FROM patient_status WHERE {} = 1 ORDER BY timestamp DESC'.format(condition)
    cursor.execute(query)
    patients = cursor.fetchall()
    conn.close()
    return render_template('index.html', patients=patients, condition=condition)

@app.route('/report/<int:patient_id>')
def report(patient_id):
    conn = get_db_connection()
    if conn is None:
        return "Database connection error", 500

    cursor = conn.cursor(dictionary=True)
    five_minutes_ago = datetime.now() - timedelta(minutes=5)
    formatted_time = five_minutes_ago.strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute('''
        SELECT * FROM patient_status 
        WHERE patient_id = %s AND timestamp >= %s
        ORDER BY timestamp DESC
    ''', (patient_id, formatted_time))
    patient_records = cursor.fetchall()
    conn.close()

    return render_template('report.html', patient_records=patient_records)

if __name__ == '__main__':
    app.run(debug=True)
