from pymongo import MongoClient
import requests
from datetime import datetime, timedelta
import time
import json
from pprint import pprint

MONGODB_URI = "mongodb://admin:password123@localhost:27017/"
RIAK_URL = "http://localhost:8098"

class HealthInsightCRUD:
    def __init__(self):
        self.mongo_client = MongoClient(MONGODB_URI)
        self.db = self.mongo_client['healthinsight']
        self.patients = self.db['patients']
        
       
        try:
            self.patients.find_one()
            print("✓ MongoDB connected successfully")
        except Exception as e:
            print(f"✗ MongoDB connection failed: {e}")
        
        try:
            response = requests.get(f"{RIAK_URL}/ping", timeout=2)
            if response.status_code == 200:
                print("✓ Riak KV connected successfully")
            else:
                print(f"✗ Riak KV returned status: {response.status_code}")
        except Exception as e:
            print(f"✗ Riak KV connection failed: {e}")
    
    def print_patient_details(self, patient_id, label="CURRENT STATE"):
        patient = self.patients.find_one({"patient_id": patient_id}, {"_id": 0})
        
        if not patient:
            print(f"   Patient {patient_id} not found")
            return None
        
        print(f"\n  {label}:")
        print(f"   {'─' * 60}")
        print(f"   Patient ID    : {patient['patient_id']}")
        print(f"   Name          : {patient['name']}")
        print(f"   Age           : {patient['age']} years")
        print(f"   Gender        : {patient['gender']}")
        print(f"   Phone         : {patient['contact']['phone']}")
        print(f"   City          : {patient['contact']['city']}")
        print(f"   Risk Score    : {patient['risk_score']}/10")
        
        conditions = patient['medical_history'].get('conditions', [])
        print(f"   Conditions    : {', '.join(conditions) if conditions else 'None'}")
        
        allergies = patient['medical_history'].get('allergies', [])
        print(f"   Allergies     : {', '.join(allergies) if allergies else 'None'}")
        
        print(f"   Blood Type    : {patient['medical_history']['blood_type']}")
        print(f"   Notes         : {patient.get('notes', 'N/A')}")
        
        # Show risk score history if exists
        if 'risk_score_history' in patient:
            print(f"\n  Risk Score History:")
            for entry in patient['risk_score_history'][-3:]:  # Last 3 changes
                print(f"      {entry['timestamp'][:19]}: {entry['previous_score']} → {entry['score']}")
        
        print(f"   {'─' * 60}")
        return patient
    
    def q1_insert_monitoring_event(self, patient_id, event_data):
        """Q1: Insert patient monitoring events with < 100ms latency"""
        start_time = time.time()
        
        event_key = f"{patient_id}_{int(datetime.now().timestamp() * 1000)}"
        event_data['patient_id'] = patient_id
        event_data['timestamp'] = datetime.now().isoformat()
        
        print(f"\n  INSERTING EVENT:")
        print(f"   {'─' * 60}")
        print(f"   Patient ID        : {patient_id}")
        print(f"   Heart Rate        : {event_data['heart_rate']} bpm")
        print(f"   Blood Pressure    : {event_data['blood_pressure']['systolic']}/{event_data['blood_pressure']['diastolic']} mmHg")
        print(f"   O2 Saturation     : {event_data['oxygen_saturation']}%")
        print(f"   Temperature       : {event_data['temperature']}°C")
        print(f"   Status            : {event_data['status']}")
        print(f"   Timestamp         : {event_data['timestamp'][:19]}")
        print(f"   Riak Key          : {event_key}")
        
        try:
            response = requests.put(
                f"{RIAK_URL}/buckets/monitoring/keys/{event_key}",
                json=event_data,
                headers={"Content-Type": "application/json"},
                timeout=2
            )
            
            latency = (time.time() - start_time) * 1000
            
            if response.status_code in [200, 204]:
                status = "✓" if latency < 100 else "⚠"
                print(f"\n   {status} EVENT STORED SUCCESSFULLY")
                print(f"   Latency: {latency:.2f}ms (Target: <100ms)")
                print(f"   {'─' * 60}")
                return True, latency
            else:
                print(f"\n   ✗ Insert failed - HTTP {response.status_code}")
                return False, latency
                
        except requests.exceptions.ConnectionError:
            print(f"\n   ✗ Cannot connect to Riak KV on {RIAK_URL}")
            return False, 0
        except Exception as e:
            latency = (time.time() - start_time) * 1000
            print(f"\n   ✗ Error - {e}")
            return False, latency
    
    def q2_retrieve_last_readings(self, patient_id, n=20):
        """Q2: Retrieve last N readings for a patient"""
        print(f"\n   🔍 RETRIEVING LAST {n} READINGS:")
        print(f"   {'─' * 60}")
        
        try:
            response = requests.get(
                f"{RIAK_URL}/buckets/monitoring/keys?keys=true",
                timeout=5
            )
            
            if response.status_code == 200:
                keys = response.json().get('keys', [])
                patient_keys = [k for k in keys if k.startswith(patient_id)]
                patient_keys.sort(reverse=True)
                
                print(f"   Total events found: {len(patient_keys)}")
                print(f"   Retrieving: {min(n, len(patient_keys))} most recent")
                print()
                
                readings = []
                for key in patient_keys[:n]:
                    event_response = requests.get(
                        f"{RIAK_URL}/buckets/monitoring/keys/{key}",
                        timeout=2
                    )
                    if event_response.status_code == 200:
                        readings.append(event_response.json())
                
                if readings:
                    print(f"   READINGS RETRIEVED:")
                    print(f"   {'─' * 60}")
                    for i, reading in enumerate(readings[:5], 1):
                        bp = reading.get('blood_pressure', {})
                        status_icon = "⚠️" if reading.get('status') == 'ABNORMAL' else "✓"
                        print(f"   {i}. [{reading.get('timestamp', '')[:19]}] {status_icon}")
                        print(f"      HR: {reading.get('heart_rate')} bpm | "
                              f"BP: {bp.get('systolic', 'N/A')}/{bp.get('diastolic', 'N/A')} | "
                              f"O2: {reading.get('oxygen_saturation')}% | "
                              f"Temp: {reading.get('temperature')}°C")
                    
                    if len(readings) > 5:
                        print(f"\n   ... and {len(readings) - 5} more readings")
                
                print(f"\n   ✓ Retrieved {len(readings)} readings successfully")
                print(f"   {'─' * 60}")
                return readings
            else:
                print(f"   ✗ HTTP {response.status_code}")
                return []
            
        except requests.exceptions.ConnectionError:
            print(f"   ✗ Cannot connect to Riak KV")
            return []
        except Exception as e:
            print(f"   ✗ Error - {e}")
            return []
    
    def q3_update_patient_profile(self, patient_id, updates):
        """Q3: Update patient profile with before/after comparison"""
        print(f"\n   🔄 UPDATING PATIENT PROFILE:")
        print(f"   {'─' * 60}")
        
        try:
            # Get BEFORE state
            print("\n   📌 BEFORE UPDATE:")
            before = self.print_patient_details(patient_id, "BEFORE UPDATE")
            
            if not before:
                return False
            
            # Show what will be updated
            print(f"\n   🔧 CHANGES TO APPLY:")
            print(f"   {'─' * 60}")
            for key, value in updates.items():
                if '.' in key:
                    parts = key.split('.')
                    old_value = before
                    for part in parts:
                        old_value = old_value.get(part, 'N/A') if isinstance(old_value, dict) else 'N/A'
                else:
                    old_value = before.get(key, 'N/A')
                
                print(f"   {key}:")
                print(f"      OLD: {old_value}")
                print(f"      NEW: {value}")
            
            # Perform update
            result = self.patients.update_one(
                {"patient_id": patient_id},
                {"$set": updates}
            )
            
            if result.modified_count > 0 or result.matched_count > 0:
                # Get AFTER state
                print(f"\n   AFTER UPDATE:")
                self.print_patient_details(patient_id, "AFTER UPDATE")
                
                print(f"\n   ✓ Profile updated successfully")
                print(f"   {'─' * 60}")
                return True
            else:
                print(f"\n   ⚠ No changes made (data already up to date)")
                return True
                
        except Exception as e:
            print(f"\n   ✗ Error - {e}")
            return False
    
    def q4_detect_abnormal_readings(self, time_window_minutes=60):
        """Q4: Detect abnormal readings across multiple patients"""
        print(f"\n    DETECTING ABNORMAL READINGS:")
        print(f"   Time Window: Last {time_window_minutes} minutes")
        print(f"   {'─' * 60}")
        
        threshold_time = datetime.now() - timedelta(minutes=time_window_minutes)
        abnormal_patients = []
        
        try:
            response = requests.get(
                f"{RIAK_URL}/buckets/monitoring/keys?keys=true",
                timeout=5
            )
            
            if response.status_code != 200:
                print(f"   ✗ Cannot retrieve events from Riak")
                return []
            
            keys = response.json().get('keys', [])
            
            # Group by patient
            patient_events = {}
            for key in keys:
                patient_id = key.split('_')[0]
                if patient_id not in patient_events:
                    patient_events[patient_id] = []
                patient_events[patient_id].append(key)
            
            print(f"   Total patients monitored: {len(patient_events)}")
            print(f"   Analyzing events...")
            print()
            
            # Check each patient's recent events
            for patient_id, event_keys in patient_events.items():
                event_keys.sort(reverse=True)
                
                for key in event_keys[:5]:
                    try:
                        event_response = requests.get(
                            f"{RIAK_URL}/buckets/monitoring/keys/{key}",
                            timeout=2
                        )
                        
                        if event_response.status_code == 200:
                            event = event_response.json()
                            if event.get('status') == 'ABNORMAL':
                                # Get patient name
                                patient = self.patients.find_one(
                                    {"patient_id": patient_id},
                                    {"name": 1, "age": 1, "medical_history.conditions": 1, "_id": 0}
                                )
                                
                                abnormal_patients.append({
                                    'patient_id': patient_id,
                                    'name': patient['name'] if patient else 'Unknown',
                                    'age': patient['age'] if patient else 'N/A',
                                    'conditions': patient['medical_history']['conditions'] if patient else [],
                                    'heart_rate': event.get('heart_rate'),
                                    'bp': event.get('blood_pressure'),
                                    'temperature': event.get('temperature'),
                                    'timestamp': event.get('timestamp')
                                })
                                break
                    except:
                        continue
            
            print(f"    ABNORMAL READINGS DETECTED:")
            print(f"   {'─' * 60}")
            print(f"   Found {len(abnormal_patients)} patients with abnormal readings\n")
            
            for i, patient in enumerate(abnormal_patients[:10], 1):
                bp = patient['bp']
                conditions = ', '.join(patient['conditions']) if patient['conditions'] else 'None'
                print(f"   {i}. {patient['patient_id']} - {patient['name']} (Age: {patient['age']})")
                print(f"      ⚠️  HR: {patient['heart_rate']} bpm | "
                      f"BP: {bp['systolic']}/{bp['diastolic']} mmHg | "
                      f"Temp: {patient['temperature']}°C")
                print(f"      Conditions: {conditions}")
                print(f"      Time: {patient['timestamp'][:19]}")
                print()
            
            if len(abnormal_patients) > 10:
                print(f"   ... and {len(abnormal_patients) - 10} more patients with abnormal readings")
            
            print(f"   ✓ Analysis complete")
            print(f"   {'─' * 60}")
            return abnormal_patients
            
        except requests.exceptions.ConnectionError:
            print(f"   ✗ Cannot connect to Riak KV")
            return []
        except Exception as e:
            print(f"   ✗ Error - {e}")
            return []
    
    def q5_generate_daily_statistics(self):
        """Q5: Generate daily hospital-wide health statistics"""
        print("\n    GENERATING DAILY STATISTICS:")
        print(f"   {'─' * 60}")
        
        try:
            total_patients = self.patients.count_documents({})
            
            age_pipeline = [
                {"$group": {
                    "_id": None,
                    "avg_age": {"$avg": "$age"},
                    "min_age": {"$min": "$age"},
                    "max_age": {"$max": "$age"}
                }}
            ]
            age_stats = list(self.patients.aggregate(age_pipeline))
            
            gender_pipeline = [
                {"$group": {"_id": "$gender", "count": {"$sum": 1}}}
            ]
            gender_stats = list(self.patients.aggregate(gender_pipeline))
            
            risk_pipeline = [
                {"$group": {
                    "_id": None,
                    "avg_risk": {"$avg": "$risk_score"},
                    "high_risk_count": {"$sum": {"$cond": [{"$gte": ["$risk_score", 7]}, 1, 0]}},
                    "medium_risk_count": {"$sum": {"$cond": [{"$and": [
                        {"$gte": ["$risk_score", 4]},
                        {"$lt": ["$risk_score", 7]}
                    ]}, 1, 0]}},
                    "low_risk_count": {"$sum": {"$cond": [{"$lt": ["$risk_score", 4]}, 1, 0]}}
                }}
            ]
            risk_stats = list(self.patients.aggregate(risk_pipeline))
            
            conditions_pipeline = [
                {"$unwind": "$medical_history.conditions"},
                {"$group": {"_id": "$medical_history.conditions", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            condition_stats = list(self.patients.aggregate(conditions_pipeline))
            
            city_pipeline = [
                {"$group": {"_id": "$contact.city", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            city_stats = list(self.patients.aggregate(city_pipeline))
            
            blood_pipeline = [
                {"$group": {"_id": "$medical_history.blood_type", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            blood_stats = list(self.patients.aggregate(blood_pipeline))
            
            print(f"\n   {'═'*60}")
            print(f"   DAILY HOSPITAL STATISTICS - {datetime.now().strftime('%Y-%m-%d')}")
            print(f"   {'═'*60}\n")
            
            print(f"   👥 PATIENT DEMOGRAPHICS")
            print(f"   {'─' * 60}")
            print(f"   Total Patients        : {total_patients}")
            print(f"   Average Age           : {age_stats[0]['avg_age']:.1f} years")
            print(f"   Age Range             : {age_stats[0]['min_age']} - {age_stats[0]['max_age']} years")
            
            print(f"\n   ⚧ GENDER DISTRIBUTION")
            print(f"   {'─' * 60}")
            for g in gender_stats:
                gender = "Male" if g['_id'] == 'M' else "Female"
                percentage = (g['count']/total_patients*100)
                bar = "█" * int(percentage / 2)
                print(f"   {gender:8s}  : {g['count']:3d} ({percentage:5.1f}%) {bar}")
            
            print(f"\n    RISK ASSESSMENT")
            print(f"   {'─' * 60}")
            print(f"   Average Risk Score    : {risk_stats[0]['avg_risk']:.2f}/10")
            high_risk = risk_stats[0]['high_risk_count']
            medium_risk = risk_stats[0]['medium_risk_count']
            low_risk = risk_stats[0]['low_risk_count']
            print(f"   High Risk (7-10)      : {high_risk} ({high_risk/total_patients*100:.1f}%)")
            print(f"   Medium Risk (4-6)     : {medium_risk} ({medium_risk/total_patients*100:.1f}%)")
            print(f"   Low Risk (1-3)        : {low_risk} ({low_risk/total_patients*100:.1f}%)")
            
            print(f"\n    TOP MEDICAL CONDITIONS")
            print(f"   {'─' * 60}")
            for i, cond in enumerate(condition_stats[:7], 1):
                percentage = (cond['count']/total_patients*100)
                print(f"   {i}. {cond['_id']:20s} : {cond['count']:3d} ({percentage:.1f}%)")
            
            print(f"\n    GEOGRAPHICAL DISTRIBUTION")
            print(f"   {'─' * 60}")
            for city in city_stats:
                percentage = (city['count']/total_patients*100)
                bar = "█" * int(percentage / 2)
                print(f"   {city['_id']:15s} : {city['count']:3d} ({percentage:5.1f}%) {bar}")
            
            print(f"\n    BLOOD TYPE DISTRIBUTION")
            print(f"   {'─' * 60}")
            for blood in blood_stats:
                percentage = (blood['count']/total_patients*100)
                print(f"   {blood['_id']:6s} : {blood['count']:3d} ({percentage:.1f}%)")
            
            print(f"\n   {'═'*60}")
            print(f"   ✓ Statistics generated successfully")
            print(f"   {'═'*60}\n")
            
            return {
                'total_patients': total_patients,
                'age_stats': age_stats[0],
                'gender_stats': gender_stats,
                'risk_stats': risk_stats[0],
                'condition_stats': condition_stats,
                'city_stats': city_stats,
                'blood_stats': blood_stats
            }
            
        except Exception as e:
            print(f"   ✗ Error - {e}")
            return {}
    
    def q6_maintain_risk_score(self, patient_id, new_risk_score):
        """Q6: Maintain current risk score per patient"""
        print(f"\n   UPDATING RISK SCORE:")
        print(f"   {'─' * 60}")
        
        try:
            # Get BEFORE state
            patient = self.patients.find_one({"patient_id": patient_id})
            if not patient:
                print(f"   ✗ Patient {patient_id} not found")
                return False
            
            old_score = patient.get('risk_score', 0)
            
            # Determine risk level
            def get_risk_level(score):
                if score >= 7:
                    return "HIGH RISK ⚠️"
                elif score >= 4:
                    return "MEDIUM RISK ⚡"
                else:
                    return "LOW RISK ✓"
            
            print(f"   Patient: {patient['name']}")
            print(f"   Current Risk Score: {old_score}/10 - {get_risk_level(old_score)}")
            print(f"   New Risk Score    : {new_risk_score}/10 - {get_risk_level(new_risk_score)}")
            
            if new_risk_score > old_score:
                print(f"   Change: ⬆️  INCREASED by {new_risk_score - old_score} points")
            elif new_risk_score < old_score:
                print(f"   Change: ⬇️  DECREASED by {old_score - new_risk_score} points")
            else:
                print(f"   Change: ➡️  No change")
            
            # Perform update
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
                print(f"\n   ✓ Risk score updated successfully")
                
                # Show full patient state after update
                print(f"\n    UPDATED PATIENT STATE:")
                self.print_patient_details(patient_id, "AFTER RISK SCORE UPDATE")
                
                return True
            else:
                print(f"\n   ⚠ No update performed")
                return False
                
        except Exception as e:
            print(f"\n   ✗ Error - {e}")
            return False
    
    def demo_all_queries(self):
        """Run complete demonstration of all query patterns"""
        print("\n" + "="*70)
        print(" " * 15 + "HealthInsight CRUD Operations Demo")
        print(" " * 10 + "Demonstrating all mandatory query patterns (Q1-Q6)")
        print("="*70)
        
        # Get a real patient ID from database
        sample_patient = self.patients.find_one({}, {"patient_id": 1, "_id": 0})
        if not sample_patient:
            print("\n✗ No patients found in database!")
            print("   Run: python src/generators/generate_data.py")
            return
        
        test_patient_id = sample_patient['patient_id']
        print(f"\n🎯 Using test patient: {test_patient_id}")
        
        # Q1: Insert monitoring event
        print("\n" + "="*70)
        print("[Q1] Insert Patient Monitoring Event")
        print("="*70)
        event = {
            "heart_rate": 78,
            "blood_pressure": {"systolic": 120, "diastolic": 80},
            "oxygen_saturation": 98,
            "temperature": 36.8,
            "status": "NORMAL"
        }
        self.q1_insert_monitoring_event(test_patient_id, event)
        
        # Q2: Retrieve last readings
        print("\n" + "="*70)
        print("[Q2] Retrieve Last 20 Readings")
        print("="*70)
        readings = self.q2_retrieve_last_readings(test_patient_id, n=10)
        
        # Q3: Update patient profile
        print("\n" + "="*70)
        print("[Q3] Update Patient Profile")
        print("="*70)
        updates = {
            "contact.phone": "0666777888",
            "notes": "Updated contact information "
        }
        self.q3_update_patient_profile(test_patient_id, updates)
        
        # Q4: Detect abnormal readings
        print("\n" + "="*70)
        print("[Q4] Detect Abnormal Readings")
        print("="*70)
        abnormal = self.q4_detect_abnormal_readings(time_window_minutes=1440)
        
        # Q5: Generate daily statistics
        print("\n" + "="*70)
        print("[Q5] Generate Daily Statistics")
        print("="*70)
        stats = self.q5_generate_daily_statistics()
        
        # Q6: Update risk score
        print("\n" + "="*70)
        print("[Q6] Maintain Risk Score")
        print("="*70)
        self.q6_maintain_risk_score(test_patient_id, 7)
        
    
    
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