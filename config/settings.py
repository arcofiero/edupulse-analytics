import os
from dotenv import load_dotenv

load_dotenv()

# Confluent Cloud
CONFLUENT_BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
CONFLUENT_API_KEY = os.getenv("CONFLUENT_API_KEY")
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_API_SECRET")

# Schema Registry
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

# AWS S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "edupulse-delta-lake")

# Delta Lake paths
LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", ".local/lakehouse")
BRONZE_PATH = os.getenv("BRONZE_PATH", f"s3://{S3_BUCKET}/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", f"s3://{S3_BUCKET}/silver")
GOLD_PATH = os.getenv("GOLD_PATH", f"s3://{S3_BUCKET}/gold")
DLQ_PATH = os.getenv("DLQ_PATH", f"s3://{S3_BUCKET}/dlq")

# Kafka topics
TOPIC_STUDENT_EVENTS = os.getenv("TOPIC_STUDENT_EVENTS", "student-events")
TOPIC_OFFLINE_EVENTS = os.getenv("TOPIC_OFFLINE_EVENTS", "offline-events")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "dead-letter-queue")

# Simulator
SIMULATOR_MODE = os.getenv("SIMULATOR_MODE", "live")
NUM_STUDENTS = int(os.getenv("NUM_STUDENTS", 500))
ACADEMIC_YEAR = int(os.getenv("ACADEMIC_YEAR", 2025))
SEMESTER_START_DATE = os.getenv("SEMESTER_START_DATE", "2025-09-01")

# Kafka producer base config
def get_kafka_producer_config() -> dict:
    return {
        "bootstrap.servers": CONFLUENT_BOOTSTRAP_SERVERS,
        "sasl.mechanisms": "PLAIN",
        "security.protocol": "SASL_SSL",
        "sasl.username": CONFLUENT_API_KEY,
        "sasl.password": CONFLUENT_API_SECRET,
    }

# Schema Registry config
def get_schema_registry_config() -> dict:
    return {
        "url": SCHEMA_REGISTRY_URL,
        "basic.auth.user.info": f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}",
    }
