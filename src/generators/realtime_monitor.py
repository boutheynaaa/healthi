"""
Real-Time Monitoring Simulator
Continuously generates patient monitoring events to simulate real hospital activity
"""

import random
import time
from datetime import datetime
import requests

# Configuration
RIAK_URL = "http://localhost:8098"
NUM_PATIENTS = 50
UPDATE_INTERVAL = 5  # seconds between updates

# Medical data ranges
HEART_RATE_NORMAL = (60, 100)
HEART_RATE_ABNORMAL = [(40, 59), (101, 150)]
BP_SYSTOLIC_NORMAL = (90, 120)
BP_SYSTOLIC_ABNORMAL = [(70, 89), (121, 180)]
BP_DIASTOLIC_NORMAL = (60, 80)
BP_DIASTOLIC_ABNORMAL = [(40, 59), (81, 110)]

def generate_monitoring_event(patient_id):
    """Generate a single monitoring event"""
    # 25% chance of abnormal readings for more activity
    is_abnormal = random.random() < 0.25
    
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
        "timestamp": datetime.now().isoformat(),
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

def send_event_to_riak(event):
    """Send event to Riak KV"""
    event_key = f"{event['patient_id']}_{int(datetime.now().timestamp())}"
    
    try:
        response = requests.put(
            f"{RIAK_URL}/buckets/monitoring/keys/{event_key}",
            json=event,
            headers={"Content-Type": "application/json"},
            timeout=1
        )
        return response.status_code in [200, 204]
    except Exception as e:
        print(f"Error sending event: {e}")
        return False

def main():
    """Main simulation loop"""
    print("=" * 70)
    print("🏥 REAL-TIME MONITORING SIMULATOR")
    print("=" * 70)
    print(f"Monitoring {NUM_PATIENTS} patients")
    print(f"Update interval: {UPDATE_INTERVAL} seconds")
    print("Dashboard will show live updates!")
    print()
    print("Press Ctrl+C to stop")
    print("=" * 70)
    print()
    
    event_count = 0
    abnormal_count = 0
    
    try:
        while True:
            # Select random patients to generate events for (simulate real activity)
            # Not all patients are monitored every cycle
            num_active = random.randint(15, 30)
            active_patients = random.sample(range(1, NUM_PATIENTS + 1), num_active)
            
            cycle_abnormal = 0
            
            for patient_id in active_patients:
                event = generate_monitoring_event(patient_id)
                
                if send_event_to_riak(event):
                    event_count += 1
                    if event['status'] == 'ABNORMAL':
                        abnormal_count += 1
                        cycle_abnormal += 1
                        print(f"⚠️  {event['patient_id']} - ABNORMAL: "
                              f"HR={event['heart_rate']} BP={event['blood_pressure']['systolic']}/{event['blood_pressure']['diastolic']}")
            
            print(f"✓ Cycle complete: {num_active} events | "
                  f"{cycle_abnormal} abnormal | "
                  f"Total: {event_count} events ({abnormal_count} abnormal)")
            print(f"   Next update in {UPDATE_INTERVAL} seconds...")
            print()
            
            time.sleep(UPDATE_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("Simulator stopped")
        print(f"Total events generated: {event_count}")
        print(f"Total abnormal events: {abnormal_count} ({abnormal_count/event_count*100:.1f}%)")
        print("=" * 70)

if __name__ == "__main__":
    main()