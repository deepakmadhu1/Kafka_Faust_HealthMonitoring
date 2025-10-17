from flask import Flask, render_template, request, jsonify
import sqlite3

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('patient_data.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/patient/<int:patient_id>')
def patient_data(patient_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM patient_data WHERE patient_id = ?', (patient_id,))
    patient = cur.fetchone()
    cur.close()
    conn.close()
    return jsonify(dict(patient)) if patient else {}

if __name__ == '__main__':
    app.run(debug=True)
