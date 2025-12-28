"""
HealthInsight Population Health Analytics
Batch processing with Apache Spark for Q5 daily statistics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

def create_spark_session():
    """Create Spark session"""
    spark = SparkSession.builder \
        .appName("HealthInsight-PopulationHealth") \
        .master("local[*]") \
        .config("spark.mongodb.read.connection.uri", "mongodb://admin:password123@mongodb:27017/healthinsight.patients") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:password123@mongodb:27017/healthinsight.analytics") \
        .getOrCreate()
    
    return spark

def analyze_patient_demographics(spark, input_path):
    """Analyze patient demographics"""
    print("="*60)
    print("POPULATION HEALTH ANALYTICS - BATCH PROCESSING")
    print("="*60)
    
    # Read patient data
    df = spark.read.json(input_path)
    
    print(f"\n📊 Total Patients: {df.count()}")
    
    # Age statistics
    print("\n1. AGE DISTRIBUTION")
    print("-" * 50)
    age_stats = df.select(
        avg("age").alias("avg_age"),
        min("age").alias("min_age"),
        max("age").alias("max_age")
    ).collect()[0]
    
    print(f"Average Age: {age_stats['avg_age']:.1f} years")
    print(f"Age Range: {age_stats['min_age']} - {age_stats['max_age']} years")
    
    # Age groups
    age_groups = df.withColumn(
        "age_group",
        when(col("age") < 30, "18-29")
        .when(col("age") < 50, "30-49")
        .when(col("age") < 65, "50-64")
        .otherwise("65+")
    ).groupBy("age_group").count().orderBy("age_group")
    
    print("\nAge Groups:")
    age_groups.show()
    
    # Gender distribution
    print("\n2. GENDER DISTRIBUTION")
    print("-" * 50)
    gender_dist = df.groupBy("gender").count()
    gender_dist.show()
    
    # Risk score analysis
    print("\n3. RISK SCORE ANALYSIS")
    print("-" * 50)
    risk_stats = df.select(
        avg("risk_score").alias("avg_risk"),
        count(when(col("risk_score") >= 7, True)).alias("high_risk_count"),
        count(when(col("risk_score") < 4, True)).alias("low_risk_count")
    ).collect()[0]
    
    print(f"Average Risk Score: {risk_stats['avg_risk']:.2f}/10")
    print(f"High Risk Patients (≥7): {risk_stats['high_risk_count']}")
    print(f"Low Risk Patients (<4): {risk_stats['low_risk_count']}")
    
    # City distribution
    print("\n4. GEOGRAPHICAL DISTRIBUTION")
    print("-" * 50)
    city_dist = df.groupBy("contact.city").count().orderBy(col("count").desc())
    city_dist.show()
    
    # Blood type distribution
    print("\n5. BLOOD TYPE DISTRIBUTION")
    print("-" * 50)
    blood_type = df.groupBy("medical_history.blood_type").count().orderBy(col("count").desc())
    blood_type.show()
    
    # Save results
    results = {
        "age_stats": age_stats.asDict(),
        "total_patients": df.count(),
        "timestamp": "2025-12-28"
    }
    
    print("\n" + "="*60)
    print("✓ Analytics completed successfully")
    print("="*60)
    
    return results

def main():
    """Main execution"""
    spark = create_spark_session()
    
    try:
        # Input path - adjust based on where data is mounted in container
        input_path = "/opt/data/raw/patients.json"
        
        results = analyze_patient_demographics(spark, input_path)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()