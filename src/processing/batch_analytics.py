from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum, max, min, when, 
    explode, year, month, dayofmonth, hour,
    lit, round as spark_round
)
from pyspark.sql.types import *
import json
from datetime import datetime

def create_spark_session():
    """Create Spark session with MongoDB connector"""
    spark = SparkSession.builder \
        .appName("HealthInsight-BatchAnalytics") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    return spark

def analyze_patient_demographics(df):
    """Analyze patient demographics"""
    print("\n" + "="*70)
    print("PATIENT DEMOGRAPHICS ANALYSIS")
    print("="*70)
    
    total_patients = df.count()
    print(f"\n✓ Total Patients: {total_patients}")
    
    # Age statistics
    age_stats = df.select(
        spark_round(avg("age"), 1).alias("avg_age"),
        min("age").alias("min_age"),
        max("age").alias("max_age")
    ).collect()[0]
    
    print(f"\nAge Statistics:")
    print(f"   Average Age: {age_stats['avg_age']} years")
    print(f"   Age Range: {age_stats['min_age']} - {age_stats['max_age']} years")
    
    # Age groups
    age_groups = df.withColumn(
        "age_group",
        when(col("age") < 30, "18-29")
        .when(col("age") < 50, "30-49")
        .when(col("age") < 65, "50-64")
        .otherwise("65+")
    ).groupBy("age_group").count().orderBy("age_group")
    
    print("\n Age Distribution:")
    age_groups.show()
    
    # Gender distribution
    gender_dist = df.groupBy("gender").count()
    print("\n⚧ Gender Distribution:")
    gender_dist.show()
    
    return {
        "total_patients": total_patients,
        "avg_age": float(age_stats['avg_age']),
        "age_range": f"{age_stats['min_age']}-{age_stats['max_age']}",
        "age_groups": age_groups.collect(),
        "gender_distribution": gender_dist.collect()
    }

def analyze_risk_scores(df):
    """Analyze patient risk scores"""
    print("\n" + "="*70)
    print(" RISK SCORE ANALYSIS")
    print("="*70)
    
    risk_stats = df.select(
        spark_round(avg("risk_score"), 2).alias("avg_risk"),
        count(when(col("risk_score") >= 7, True)).alias("high_risk"),
        count(when((col("risk_score") >= 4) & (col("risk_score") < 7), True)).alias("medium_risk"),
        count(when(col("risk_score") < 4, True)).alias("low_risk")
    ).collect()[0]
    
    total = df.count()
    
    print(f"\n Risk Score Summary:")
    print(f"   Average Risk: {risk_stats['avg_risk']}/10")
    print(f"   High Risk (7-10): {risk_stats['high_risk']} ({risk_stats['high_risk']/total*100:.1f}%)")
    print(f"   Medium Risk (4-6): {risk_stats['medium_risk']} ({risk_stats['medium_risk']/total*100:.1f}%)")
    print(f"   Low Risk (1-3): {risk_stats['low_risk']} ({risk_stats['low_risk']/total*100:.1f}%)")
    
    # Risk by age group
    risk_by_age = df.withColumn(
        "age_group",
        when(col("age") < 30, "18-29")
        .when(col("age") < 50, "30-49")
        .when(col("age") < 65, "50-64")
        .otherwise("65+")
    ).groupBy("age_group").agg(
        spark_round(avg("risk_score"), 2).alias("avg_risk"),
        count("*").alias("count")
    ).orderBy("age_group")
    
    print("\n Risk Score by Age Group:")
    risk_by_age.show()
    
    return {
        "avg_risk": float(risk_stats['avg_risk']),
        "high_risk_count": risk_stats['high_risk'],
        "medium_risk_count": risk_stats['medium_risk'],
        "low_risk_count": risk_stats['low_risk'],
        "risk_by_age": risk_by_age.collect()
    }

def analyze_medical_conditions(df):
    """Analyze medical conditions"""
    print("\n" + "="*70)
    print(" MEDICAL CONDITIONS ANALYSIS")
    print("="*70)
    
    # Extract and count conditions
    conditions_df = df.select(explode("medical_history.conditions").alias("condition")) \
        .filter(col("condition") != "None") \
        .filter(col("condition") != "") \
        .groupBy("condition") \
        .count() \
        .orderBy(col("count").desc())
    
    print("\n Top Medical Conditions:")
    conditions_df.show(10)
    
    # Patients with multiple conditions
    multi_condition = df.withColumn("num_conditions", 
                                    when(col("medical_history.conditions").isNull(), 0)
                                    .otherwise(len(col("medical_history.conditions")))) \
        .groupBy("num_conditions") \
        .count() \
        .orderBy("num_conditions")
    
    print("\n Patients by Number of Conditions:")
    multi_condition.show()
    
    return {
        "top_conditions": conditions_df.limit(10).collect(),
        "multi_condition_stats": multi_condition.collect()
    }

def analyze_geographical_distribution(df):
    """Analyze geographical distribution"""
    print("\n" + "="*70)
    print(" GEOGRAPHICAL DISTRIBUTION")
    print("="*70)
    
    city_dist = df.groupBy("contact.city").agg(
        count("*").alias("patient_count"),
        spark_round(avg("risk_score"), 2).alias("avg_risk")
    ).orderBy(col("patient_count").desc())
    
    print("\n Patients by City:")
    city_dist.show()
    
    return {
        "city_distribution": city_dist.collect()
    }

def analyze_blood_types(df):
    """Analyze blood type distribution"""
    print("\n" + "="*70)
    print(" BLOOD TYPE ANALYSIS")
    print("="*70)
    
    blood_dist = df.groupBy("medical_history.blood_type").count() \
        .orderBy(col("count").desc())
    
    print("\n💉 Blood Type Distribution:")
    blood_dist.show()
    
    # Blood type by risk score
    blood_risk = df.groupBy("medical_history.blood_type").agg(
        spark_round(avg("risk_score"), 2).alias("avg_risk"),
        count("*").alias("count")
    ).orderBy(col("avg_risk").desc())
    
    print("\n Average Risk by Blood Type:")
    blood_risk.show()
    
    return {
        "blood_type_distribution": blood_dist.collect(),
        "blood_type_risk": blood_risk.collect()
    }

def save_results_to_hdfs(spark, results, output_path):
    """Save analysis results to HDFS"""
    print("\n" + "="*70)
    print(" SAVING RESULTS TO HDFS")
    print("="*70)
    
    # Convert results to JSON
    results_json = json.dumps(results, indent=2, default=str)
    
    # Save to HDFS
    results_rdd = spark.sparkContext.parallelize([results_json])
    results_df = spark.read.json(results_rdd)
    
    results_df.write.mode("overwrite").json(output_path)
    
    print(f"\n✓ Results saved to: {output_path}")

def main():
    """Main execution function"""
    print("\n" + "="*70)
    print(" HEALTHINSIGHT - POPULATION HEALTH ANALYTICS")
    print("="*70)
    print(f" Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    spark = create_spark_session()
    
    try:
        # Read patient data from HDFS
        input_path = "hdfs://hadoop-master:9000/data/raw/patients.json"
        output_path = "hdfs://hadoop-master:9000/analytics/results/batch_results.json"
        
        print(f"\n Reading data from: {input_path}")
        df = spark.read.json(input_path)
        
        print(f"✓ Loaded {df.count()} patient records")
        
        # Run all analyses
        demographics = analyze_patient_demographics(df)
        risk_analysis = analyze_risk_scores(df)
        conditions = analyze_medical_conditions(df)
        geography = analyze_geographical_distribution(df)
        blood_types = analyze_blood_types(df)
        
        # Compile results
        results = {
            "timestamp": datetime.now().isoformat(),
            "demographics": demographics,
            "risk_analysis": risk_analysis,
            "medical_conditions": conditions,
            "geographical_distribution": geography,
            "blood_types": blood_types
        }
        
        # Save results
        save_results_to_hdfs(spark, results, output_path)
        
        print("\n" + "="*70)
        print(" BATCH ANALYTICS COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f" Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()