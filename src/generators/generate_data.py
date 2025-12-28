"""
HealthInsight Data Generator
Generates synthetic patient data for the healthcare analytics platform
"""

import random
import json
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
import requests

# Configuration
MONGODB_URI = "mongodb://admin:password123@localhost:27017/"
RIAK_URL = "http://localhost:8098"
NUM_PATIENTS = 50
MONITORING_EVENTS_PER_PATIENT = 100

# Medical data ranges
HEART_RATE_NORMAL = (60, 100)
HEART_RATE_ABNORMAL = [(40, 59), (101, 150)]
BP_SYSTOLIC_NORMAL = (90, 120)
BP_SYSTOLIC_ABNORMAL = [(70, 89), (121, 180)]
BP_DIASTOLIC_NORMAL = (60, 80)
BP_DIASTOLIC_ABNORMAL = [(40, 59), (81, 110)]

FIRST_NAMES = ["Ahmed", "Fatima", "Mohammed", "Amina", "Youssef", "Khadija", 
               "Ali", "Zahra", "Omar", "Aisha", "Karim", "Nour"]
LAST_NAMES = ["Benali", "Mansouri", "Kaddour", "Djebbar", "Zerrouki", "Brahimi",
              "Boudiaf", "Chaoui", "Hamdi", "Saidi"]

MEDICAL_CONDITIONS = ["Hypertension", "Diabetes Type 2", "Asthma", "None", 
                      "Heart Disease", "Arthritis"]

def generate_patient_profile(patient_id):
    """Generate a patient profile for MongoDB"""
    age = random.randint(20, 85)
    has_condition = random.random() > 0.3
    
    patient = {
        "patient_id": f"P{patient_id:04d}",
        "name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
        "age": age,
        "gender": random.choice(["M", "F"]),
        "contact": {
            "phone": f"0{random.randint(500000000, 799999999)}",
            "email": f"patient{patient_id}@email.dz",
            "city": random.choice(["Constantine", "Algiers", "Oran", "Annaba"])
        },
        "medical_history": {
            "conditions": [random.choice(MEDICAL_CONDITIONS)] if has_condition else [],
            "allergies": random.choice([[], ["Penicillin"], ["Aspirin"], []]),
            "blood_type": random.choice(["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"])
        },
        "risk_score": random.randint(1, 10),
        "admission_date": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
        "notes": "Regular monitoring required" if has_condition else "Healthy patient"
    }
    return patient

def generate_monitoring_event(patient_id, timestamp):
    """Generate a monitoring event for Riak KV"""
    # 20% chance of abnormal readings
    is_abnormal = random.random() < 0.2
    
    if is_abnormal:
        heart_rate = random.randint(*random.choice(HEART_RATE_ABNORMAL))
        bp_systolic = random.randint(*random.choice(BP_SYSTOLIC_ABNORMAL))
        bp_diastolic = random.randint(*random.choice(BP_DIASTOLIC_ABNORMAL))
    else:
        heart_rate = random.randint(*HEART_RATE_NORMAL)
        bp_systolic = random.randint(*BP_SYSTOLIC_NORMAL)
        bp_diastolic = random.randint(*BP_DIASTOLIC_NORMAL)
    
    event = {
        "patient_id": f"P{patient_id:04d}",
        "timestamp": timestamp.isoformat(),
        "heart_rate": heart_rate,
        "blood_pressure": {
            "systolic": bp_systolic,
            "diastolic": bp_diastolic
        },
        "oxygen_saturation": random.randint(92, 100),
        "temperature": round(random.uniform(36.1, 38.5), 1),
        "status": "ABNORMAL" if is_abnormal else "NORMAL"
    }
    return event

def populate_mongodb():
    """Populate MongoDB with patient profiles"""
    print("Connecting to MongoDB...")
    client = MongoClient(MONGODB_URI)
    db = client['healthinsight']
    patients_collection = db['patients']
    
    # Clear existing data
    patients_collection.delete_many({})
    
    print(f"Generating {NUM_PATIENTS} patient profiles...")
    patients = [generate_patient_profile(i) for i in range(1, NUM_PATIENTS + 1)]
    
    result = patients_collection.insert_many(patients)
    print(f"✓ Inserted {len(result.inserted_ids)} patient profiles into MongoDB")
    
    client.close()
    return patients

def populate_riak():
    """Populate Riak KV with monitoring events"""
    print("\nGenerating monitoring events for Riak KV...")
    
    total_events = 0
    base_time = datetime.now() - timedelta(hours=24)
    
    for patient_id in range(1, NUM_PATIENTS + 1):
        for event_num in range(MONITORING_EVENTS_PER_PATIENT):
            # Events spread over last 24 hours
            event_time = base_time + timedelta(
                minutes=random.randint(0, 1440)
            )
            
            event = generate_monitoring_event(patient_id, event_time)
            event_key = f"P{patient_id:04d}_{int(event_time.timestamp())}"
            
            # Store in Riak
            try:
                response = requests.put(
                    f"{RIAK_URL}/buckets/monitoring/keys/{event_key}",
                    json=event,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code in [200, 204]:
                    total_events += 1
                    if total_events % 500 == 0:
                        print(f"  Stored {total_events} events...")
            except Exception as e:
                print(f"  Warning: Failed to store event {event_key}: {e}")
    
    print(f"✓ Stored {total_events} monitoring events in Riak KV")

def export_for_batch_processing():
    """Export data to JSON files for Hadoop/Spark processing"""
    print("\nExporting data for batch processing...")
    
    # Connect to MongoDB
    client = MongoClient(MONGODB_URI)
    db = client['healthinsight']
    
    # Export patient profiles
    patients = list(db['patients'].find({}, {'_id': 0}))
    with open('data/raw/patients.json', 'w') as f:
        for patient in patients:
            f.write(json.dumps(patient) + '\n')
    
    print(f"✓ Exported {len(patients)} patient profiles to data/raw/patients.json")
    
    # Fetch monitoring events from Riak
    print("  Fetching monitoring events from Riak...")
    events = []
    
    for patient_id in range(1, min(NUM_PATIENTS + 1, 20)):  # Sample subset
        try:
            # List keys for this patient (simplified)
            response = requests.get(
                f"{RIAK_URL}/buckets/monitoring/keys?keys=stream"
            )
            
            if response.status_code == 200:
                print(f"  Fetched events for patient P{patient_id:04d}")
        except Exception as e:
            print(f"  Warning: {e}")
    
    print(f"✓ Data ready for batch processing")
    
    client.close()

def main():
    """Main execution function"""
    print("=" * 60)
    print("HealthInsight Data Generator")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        # Step 1: Populate MongoDB with patient profiles
        populate_mongodb()
        
        # Step 2: Populate Riak with monitoring events
        populate_riak()
        
        # Step 3: Export data for batch processing
        export_for_batch_processing()
        
        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print(f"✓ Data generation completed in {elapsed:.2f} seconds")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()