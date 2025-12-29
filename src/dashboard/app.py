from flask import Flask, render_template, jsonify
from pymongo import MongoClient
import requests
from datetime import datetime, timedelta
import plotly.graph_objs as go
import plotly.express as px
from collections import Counter
import json
import traceback

app = Flask(__name__)

MONGODB_URI = "mongodb://admin:password123@localhost:27017/"
RIAK_URL = "http://localhost:8098"

try:
    mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db = mongo_client['healthinsight']
    patients_collection = db['patients']
   
    mongo_client.server_info()
    print("✓ MongoDB connected successfully")
except Exception as e:
    print(f"✗ MongoDB connection error: {e}")
    patients_collection = None

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/overview')
def get_overview():
    """Get hospital overview statistics"""
    try:
        if patients_collection is None:
            return jsonify({'error': 'Database not connected'}), 500
            
        total_patients = patients_collection.count_documents({})
        
        # High risk patients
        high_risk = patients_collection.count_documents({"risk_score": {"$gte": 7}})
        
        # Average age
        pipeline = [
            {"$group": {"_id": None, "avg_age": {"$avg": "$age"}}}
        ]
        age_result = list(patients_collection.aggregate(pipeline))
        avg_age = age_result[0]['avg_age'] if age_result else 0
        
        # Active monitoring (simulated - patients admitted in last 30 days)
        thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
        active_monitoring = patients_collection.count_documents({
            "admission_date": {"$gte": thirty_days_ago}
        })
        
        return jsonify({
            'total_patients': total_patients,
            'high_risk_patients': high_risk,
            'avg_age': round(avg_age, 1),
            'active_monitoring': active_monitoring
        })
    except Exception as e:
        print(f"Error in get_overview: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/age_distribution')
def get_age_distribution():
    """Get age distribution chart data"""
    try:
        if patients_collection is None:
            return jsonify({'error': 'Database not connected'}), 500
        
        # Get all patients and manually bucket them
        all_patients = list(patients_collection.find({}, {"age": 1, "_id": 0}))
        
        # Count by age groups
        age_18_29 = sum(1 for p in all_patients if p['age'] < 30)
        age_30_49 = sum(1 for p in all_patients if 30 <= p['age'] < 50)
        age_50_64 = sum(1 for p in all_patients if 50 <= p['age'] < 65)
        age_65_plus = sum(1 for p in all_patients if p['age'] >= 65)
        
        labels = ['18-29', '30-49', '50-64', '65+']
        values = [age_18_29, age_30_49, age_50_64, age_65_plus]
        
        fig = go.Figure(data=[
            go.Bar(
                x=labels,
                y=values,
                marker_color=['#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b'],
                text=values,
                textposition='auto'
            )
        ])
        
        fig.update_layout(
            title='Patient Age Distribution',
            xaxis_title='Age Group',
            yaxis_title='Number of Patients',
            template='plotly_white',
            height=400
        )
        
        return jsonify(json.loads(fig.to_json()))
    except Exception as e:
        print(f"Error in age_distribution: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/gender_distribution')
def get_gender_distribution():
    """Get gender distribution pie chart"""
    try:
        if patients_collection is None:
            return jsonify({'error': 'Database not connected'}), 500
            
        pipeline = [
            {"$group": {"_id": "$gender", "count": {"$sum": 1}}}
        ]
        
        results = list(patients_collection.aggregate(pipeline))
        
        labels = [r['_id'] for r in results]
        values = [r['count'] for r in results]
        
        fig = go.Figure(data=[
            go.Pie(
                labels=['Male' if l == 'M' else 'Female' for l in labels],
                values=values,
                marker=dict(colors=['#3b82f6', '#ec4899']),
                hole=0.4
            )
        ])
        
        fig.update_layout(
            title='Gender Distribution',
            template='plotly_white',
            height=400
        )
        
        return jsonify(json.loads(fig.to_json()))
    except Exception as e:
        print(f"Error in gender_distribution: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/risk_distribution')
def get_risk_distribution():
    """Get risk score distribution"""
    try:
        if patients_collection is None:
            return jsonify({'error': 'Database not connected'}), 500
            
        pipeline = [
            {"$group": {"_id": "$risk_score", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        
        results = list(patients_collection.aggregate(pipeline))
        
        risk_scores = [r['_id'] for r in results]
        counts = [r['count'] for r in results]
        
        colors = ['#10b981' if r < 4 else '#f59e0b' if r < 7 else '#ef4444' 
                  for r in risk_scores]
        
        fig = go.Figure(data=[
            go.Bar(
                x=risk_scores,
                y=counts,
                marker_color=colors,
                text=counts,
                textposition='auto'
            )
        ])
        
        fig.update_layout(
            title='Risk Score Distribution',
            xaxis_title='Risk Score (1-10)',
            yaxis_title='Number of Patients',
            template='plotly_white',
            height=400
        )
        
        return jsonify(json.loads(fig.to_json()))
    except Exception as e:
        print(f"Error in risk_distribution: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/city_distribution')
def get_city_distribution():
    """Get geographical distribution"""
    try:
        if patients_collection is None:
            return jsonify({'error': 'Database not connected'}), 500
            
        # Get all patients and extract city from nested contact
        all_patients = list(patients_collection.find({}, {"contact.city": 1, "_id": 0}))
        
        # Count cities manually
        city_counts = {}
        for patient in all_patients:
            city = patient.get('contact', {}).get('city', 'Unknown')
            city_counts[city] = city_counts.get(city, 0) + 1
        
        # Sort by count
        sorted_cities = sorted(city_counts.items(), key=lambda x: x[1], reverse=True)
        
        cities = [item[0] for item in sorted_cities]
        counts = [item[1] for item in sorted_cities]
        
        fig = go.Figure(data=[
            go.Bar(
                y=cities,
                x=counts,
                orientation='h',
                marker_color='#8b5cf6',
                text=counts,
                textposition='auto'
            )
        ])
        
        fig.update_layout(
            title='Patients by City',
            xaxis_title='Number of Patients',
            yaxis_title='City',
            template='plotly_white',
            height=400
        )
        
        return jsonify(json.loads(fig.to_json()))
    except Exception as e:
        print(f"Error in city_distribution: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/conditions_distribution')
def get_conditions_distribution():
    """Get medical conditions distribution"""
    try:
        if patients_collection is None:
            return jsonify({'error': 'Database not connected'}), 500
            
        # Get all patients and extract conditions
        all_patients = list(patients_collection.find({}, {"medical_history.conditions": 1, "_id": 0}))
        
        # Count conditions manually
        condition_counts = {}
        for patient in all_patients:
            conditions = patient.get('medical_history', {}).get('conditions', [])
            for condition in conditions:
                if condition:  # Skip empty strings
                    condition_counts[condition] = condition_counts.get(condition, 0) + 1
        
        # Sort and get top 8
        sorted_conditions = sorted(condition_counts.items(), key=lambda x: x[1], reverse=True)[:8]
        
        conditions = [item[0] for item in sorted_conditions]
        counts = [item[1] for item in sorted_conditions]
        
        fig = go.Figure(data=[
            go.Bar(
                x=conditions,
                y=counts,
                marker_color='#ec4899',
                text=counts,
                textposition='auto'
            )
        ])
        
        fig.update_layout(
            title='Top Medical Conditions',
            xaxis_title='Condition',
            yaxis_title='Number of Patients',
            template='plotly_white',
            height=400,
            xaxis_tickangle=-45
        )
        
        return jsonify(json.loads(fig.to_json()))
    except Exception as e:
        print(f"Error in conditions_distribution: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/blood_type_distribution')
def get_blood_type_distribution():
    """Get blood type distribution"""
    try:
        if patients_collection is None:
            return jsonify({'error': 'Database not connected'}), 500
            
        # Get all patients and extract blood type
        all_patients = list(patients_collection.find({}, {"medical_history.blood_type": 1, "_id": 0}))
        
        # Count blood types manually
        blood_type_counts = {}
        for patient in all_patients:
            blood_type = patient.get('medical_history', {}).get('blood_type', 'Unknown')
            blood_type_counts[blood_type] = blood_type_counts.get(blood_type, 0) + 1
        
        # Sort by count
        sorted_blood_types = sorted(blood_type_counts.items(), key=lambda x: x[1], reverse=True)
        
        blood_types = [item[0] for item in sorted_blood_types]
        counts = [item[1] for item in sorted_blood_types]
        
        fig = go.Figure(data=[
            go.Pie(
                labels=blood_types,
                values=counts,
                marker=dict(colors=px.colors.qualitative.Set3)
            )
        ])
        
        fig.update_layout(
            title='Blood Type Distribution',
            template='plotly_white',
            height=400
        )
        
        return jsonify(json.loads(fig.to_json()))
    except Exception as e:
        print(f"Error in blood_type_distribution: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/recent_patients')
def get_recent_patients():
    """Get list of recently admitted patients"""
    try:
        if patients_collection is None:
            return jsonify({'error': 'Database not connected'}), 500
            
        patients = list(patients_collection.find(
            {},
            {
                'patient_id': 1, 
                'name': 1, 
                'age': 1, 
                'risk_score': 1, 
                'admission_date': 1,
                '_id': 0
            }
        ).sort('admission_date', -1).limit(10))
        
        return jsonify(patients)
    except Exception as e:
        print(f"Error in recent_patients: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/abnormal_readings')
def get_abnormal_readings():
    """Get patients with recent abnormal readings"""
    try:
        # Get keys from Riak
        response = requests.get(f"{RIAK_URL}/buckets/monitoring/keys?keys=true", timeout=5)
        
        if response.status_code != 200:
            return jsonify([])
        
        keys = response.json().get('keys', [])
        abnormal_patients = []
        
        # Check recent events (sample for performance)
        for key in keys[:100]:  # Limit to avoid timeout
            try:
                event_response = requests.get(
                    f"{RIAK_URL}/buckets/monitoring/keys/{key}",
                    timeout=1
                )
                
                if event_response.status_code == 200:
                    event = event_response.json()
                    if event.get('status') == 'ABNORMAL':
                        abnormal_patients.append({
                            'patient_id': event.get('patient_id'),
                            'heart_rate': event.get('heart_rate'),
                            'bp_systolic': event['blood_pressure']['systolic'],
                            'bp_diastolic': event['blood_pressure']['diastolic'],
                            'timestamp': event.get('timestamp'),
                            'severity': 'High' if event.get('heart_rate', 0) > 120 else 'Medium'
                        })
                        
                        if len(abnormal_patients) >= 15:
                            break
            except:
                continue
        
        return jsonify(abnormal_patients)
    except Exception as e:
        print(f"Error in abnormal_readings: {e}")
        traceback.print_exc()
        return jsonify([])

@app.route('/api/monitoring_trends')
def get_monitoring_trends():
    """Get monitoring trends over time (simulated)"""
    try:
        # For demo purposes, create trend data
        hours = list(range(24))
        normal_counts = [45 + (i % 6) * 3 for i in range(24)]
        abnormal_counts = [8 - (i % 4) * 2 if i % 4 != 0 else 12 for i in range(24)]
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=hours,
            y=normal_counts,
            mode='lines+markers',
            name='Normal Readings',
            line=dict(color='#10b981', width=3),
            fill='tozeroy'
        ))
        
        fig.add_trace(go.Scatter(
            x=hours,
            y=abnormal_counts,
            mode='lines+markers',
            name='Abnormal Readings',
            line=dict(color='#ef4444', width=3),
            fill='tozeroy'
        ))
        
        fig.update_layout(
            title='24-Hour Monitoring Trends',
            xaxis_title='Hour of Day',
            yaxis_title='Number of Readings',
            template='plotly_white',
            height=400,
            hovermode='x unified'
        )
        
        return jsonify(json.loads(fig.to_json()))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("=" * 60)
    print("HealthInsight Dashboard Starting...")
    print("=" * 60)
    print("Dashboard URL: http://localhost:5000")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)