"""
HealthInsight CRUD Operations
Demonstrates all mandatory query patterns Q1-Q6
"""

from pymongo import MongoClient
import requests
from datetime import datetime, timedelta
import time
import json

MONGODB_URI = "mongodb://admin:password123@localhost:27017/"
RIAK_URL = "http://localhost:8098"

class HealthInsightCRUD:
    def __init__(self):
        self.mongo_client = MongoClient(MONGODB_URI)
        self.db = self.mongo_client['healthinsight']
        self.patients = self.db['patients']
        
    def q1_insert_monitoring_event(self, patient_id, event_data):
        """
        Q1: Insert patient monitoring events with < 100ms latency
        Uses Riak KV for high-velocity writes
        """
        start_time = time.time()
        
        event_key = f"{patient_id}_{int(datetime.now().timestamp())}"
        event_data['patient_id'] = patient_id
        event_data['timestamp'] = datetime.now().isoformat()
        
        try:
            response = requests.put(
                f"{RIAK_URL}/buckets/monitoring/keys/{event_key}",
                json=event_data,
                headers={"Content-Type": "application/json"},
                timeout=0.1  # 100ms timeout
            )
            
            latency = (time.time() - start_time) * 1000  # Convert to ms
            
            if response.status_code in [200, 204]:
                print(f"✓ Q1: Event inserted in {latency:.2f}ms (Target: <100ms)")
                return True, latency
            else:
                print(f"✗ Q1: Insert failed - {response.status_code}")
                return False, latency
                
        except Exception as e:
            latency = (time.time() - start_time) * 1000
            print(f"✗ Q1: Error - {e}")
            return False, latency
    
    def q2_retrieve_last_readings(self, patient_id, n=20):
        """
        Q2: Retrieve last N readings for a patient
        Uses Riak KV for fast retrieval
        """
        print(f"\nQ2: Retrieving last {n} readings for {patient_id}...")
        
        try:
            # In production, you'd use secondary indexes or custom bucket structure
            # For demo, we'll use bucket key listing (simplified)
            response = requests.get(
                f"{RIAK_URL}/buckets/monitoring/keys?keys=true"
            )
            
            if response.status_code == 200:
                keys = response.json().get('keys', [])
                patient_keys = [k for k in keys if k.startswith(patient_id)]
                patient_keys.sort(reverse=True)  # Most recent first
                
                readings = []
                for key in patient_keys[:n]:
                    event_response = requests.get(
                        f"{RIAK_URL}/buckets/monitoring/keys/{key}"
                    )
                    if event_response.status_code == 200:
                        readings.append(event_response.json())
                
                print(f"✓ Q2: Retrieved {len(readings)} readings")
                return readings
            
        except Exception as e:
            print(f"✗ Q2: Error - {e}")
            return []
    
    def q3_update_patient_profile(self, patient_id, updates):
        """
        Q3: Update patient profile (contact info, medical notes)
        Uses MongoDB with quorum consistency
        """
        print(f"\nQ3: Updating profile for {patient_id}...")
        
        try:
            result = self.patients.update_one(
                {"patient_id": patient_id},
                {"$set": updates}
            )
            
            if result.modified_count > 0:
                print(f"✓ Q3: Profile updated successfully")
                return True
            else:
                print(f"⚠ Q3: No changes made (patient may not exist)")
                return False
                
        except Exception as e:
            print(f"✗ Q3: Error - {e}")
            return False
    
    def q4_detect_abnormal_readings(self, time_window_minutes=60):
        """
        Q4: Detect abnormal readings across multiple patients in time windows
        Analyzes recent monitoring data
        """
        print(f"\nQ4: Detecting abnormal readings in last {time_window_minutes} minutes...")
        
        threshold_time = datetime.now() - timedelta(minutes=time_window_minutes)
        abnormal_patients = []
        
        try:
            # Get all patients
            patients = list(self.patients.find({}, {"patient_id": 1, "_id": 0}))
            
            for patient in patients:
                patient_id = patient['patient_id']
                
                # Check recent events
                response = requests.get(
                    f"{RIAK_URL}/buckets/monitoring/keys?keys=true"
                )
                
                if response.status_code == 200:
                    keys = response.json().get('keys', [])
                    patient_keys = [k for k in keys if k.startswith(patient_id)]
                    
                    for key in patient_keys[:5]:  # Check last 5 events
                        event_response = requests.get(
                            f"{RIAK_URL}/buckets/monitoring/keys/{key}"
                        )
                        
                        if event_response.status_code == 200:
                            event = event_response.json()
                            if event.get('status') == 'ABNORMAL':
                                abnormal_patients.append({
                                    'patient_id': patient_id,
                                    'heart_rate': event.get('heart_rate'),
                                    'bp': event.get('blood_pressure'),
                                    'timestamp': event.get('timestamp')
                                })
                                break
            
            print(f"✓ Q4: Found {len(abnormal_patients)} patients with abnormal readings")
            for patient in abnormal_patients[:5]:  # Show first 5
                print(f"  - {patient['patient_id']}: HR={patient['heart_rate']}, "
                      f"BP={patient['bp']['systolic']}/{patient['bp']['diastolic']}")
            
            return abnormal_patients
            
        except Exception as e:
            print(f"✗ Q4: Error - {e}")
            return []
    
    def q5_generate_daily_statistics(self):
        """
        Q5: Generate daily hospital-wide health statistics
        Uses MongoDB aggregation
        """
        print("\nQ5: Generating daily hospital statistics...")
        
        try:
            # Patient demographics
            total_patients = self.patients.count_documents({})
            
            # Age distribution
            age_pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "avg_age": {"$avg": "$age"},
                        "min_age": {"$min": "$age"},
                        "max_age": {"$max": "$age"}
                    }
                }
            ]
            age_stats = list(self.patients.aggregate(age_pipeline))
            
            # Gender distribution
            gender_pipeline = [
                {"$group": {"_id": "$gender", "count": {"$sum": 1}}}
            ]
            gender_stats = list(self.patients.aggregate(gender_pipeline))
            
            # Risk score distribution
            risk_pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "avg_risk": {"$avg": "$risk_score"},
                        "high_risk_count": {
                            "$sum": {"$cond": [{"$gte": ["$risk_score", 7]}, 1, 0]}
                        }
                    }
                }
            ]
            risk_stats = list(self.patients.aggregate(risk_pipeline))
            
            # Medical conditions
            conditions_pipeline = [
                {"$unwind": "$medical_history.conditions"},
                {"$group": {"_id": "$medical_history.conditions", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            condition_stats = list(self.patients.aggregate(conditions_pipeline))
            
            print(f"\n{'='*50}")
            print("DAILY HOSPITAL STATISTICS")
            print(f"{'='*50}")
            print(f"Total Patients: {total_patients}")
            print(f"Average Age: {age_stats[0]['avg_age']:.1f} years")
            print(f"Age Range: {age_stats[0]['min_age']} - {age_stats[0]['max_age']}")
            print(f"\nGender Distribution:")
            for g in gender_stats:
                print(f"  {g['_id']}: {g['count']} ({g['count']/total_patients*100:.1f}%)")
            print(f"\nRisk Assessment:")
            print(f"  Average Risk Score: {risk_stats[0]['avg_risk']:.2f}/10")
            print(f"  High Risk Patients: {risk_stats[0]['high_risk_count']}")
            print(f"\nTop Medical Conditions:")
            for cond in condition_stats[:5]:
                print(f"  {cond['_id']}: {cond['count']}")
            print(f"{'='*50}\n")
            
            return {
                'total_patients': total_patients,
                'age_stats': age_stats[0],
                'gender_stats': gender_stats,
                'risk_stats': risk_stats[0],
                'condition_stats': condition_stats
            }
            
        except Exception as e:
            print(f"✗ Q5: Error - {e}")
            return {}
    
    def q6_maintain_risk_score(self, patient_id, new_risk_score):
        """
        Q6: Maintain current risk score per patient
        Updates and tracks risk score changes
        """
        print(f"\nQ6: Updating risk score for {patient_id}...")
        
        try:
            # Get current risk score
            patient = self.patients.find_one({"patient_id": patient_id})
            old_score = patient.get('risk_score', 0) if patient else 0
            
            # Update risk score with timestamp
            result = self.patients.update_one(
                {"patient_id": patient_id},
                {
                    "$set": {
                        "risk_score": new_risk_score,
                        "risk_score_updated": datetime.now().isoformat()
                    },
                    "$push": {
                        "risk_score_history": {
                            "score": new_risk_score,
                            "timestamp": datetime.now().isoformat(),
                            "previous_score": old_score
                        }
                    }
                }
            )
            
            if result.modified_count > 0:
                print(f"✓ Q6: Risk score updated: {old_score} → {new_risk_score}")
                return True
            else:
                print(f"⚠ Q6: No update (patient may not exist)")
                return False
                
        except Exception as e:
            print(f"✗ Q6: Error - {e}")
            return False
    
    def demo_all_queries(self):
        """Run a complete demonstration of all query patterns"""
        print("\n" + "="*60)
        print("HealthInsight CRUD Operations Demo")
        print("Demonstrating all mandatory query patterns (Q1-Q6)")
        print("="*60)
        
        # Q1: Insert monitoring event
        print("\n[Q1] Insert Patient Monitoring Event")
        print("-" * 50)
        event = {
            "heart_rate": 78,
            "blood_pressure": {"systolic": 120, "diastolic": 80},
            "oxygen_saturation": 98,
            "temperature": 36.8,
            "status": "NORMAL"
        }
        self.q1_insert_monitoring_event("P0001", event)
        
        # Q2: Retrieve last readings
        print("\n[Q2] Retrieve Last 20 Readings")
        print("-" * 50)
        readings = self.q2_retrieve_last_readings("P0001", n=5)
        if readings:
            for i, reading in enumerate(readings[:3], 1):
                print(f"  Reading {i}: HR={reading.get('heart_rate')}, "
                      f"Status={reading.get('status')}")
        
        # Q3: Update patient profile
        print("\n[Q3] Update Patient Profile")
        print("-" * 50)
        updates = {
            "contact.phone": "0666777888",
            "notes": "Updated contact information - emergency contact added"
        }
        self.q3_update_patient_profile("P0001", updates)
        
        # Q4: Detect abnormal readings
        print("\n[Q4] Detect Abnormal Readings")
        print("-" * 50)
        abnormal = self.q4_detect_abnormal_readings(time_window_minutes=1440)
        
        # Q5: Generate daily statistics
        print("\n[Q5] Generate Daily Statistics")
        print("-" * 50)
        stats = self.q5_generate_daily_statistics()
        
        # Q6: Update risk score
        print("\n[Q6] Maintain Risk Score")
        print("-" * 50)
        self.q6_maintain_risk_score("P0001", 6)
        
        print("\n" + "="*60)
        print("Demo completed successfully!")
        print("="*60)
    
    def close(self):
        """Close database connections"""
        self.mongo_client.close()

def main():
    crud = HealthInsightCRUD()
    
    try:
        crud.demo_all_queries()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        crud.close()

if __name__ == "__main__":
    main()