import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "camera-frames"
KAFKA_GROUP_ID = "processing-server"

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
MONGO_DB = "crowd_counting"
MONGO_COLLECTION = "detections"

WEIGHTS_PATH = "/app/weights/yolov8n.pt"
