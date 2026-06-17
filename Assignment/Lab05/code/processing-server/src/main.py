import base64
import json

import cv2
import numpy as np
from kafka import KafkaConsumer

import config
from src.detector import PersonDetector
from src.db_client import MongoDBClient


def decode_frame(b64_string: str) -> np.ndarray:
    frame_bytes = base64.b64decode(b64_string)
    arr = np.frombuffer(frame_bytes, dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


def main():
    detector = PersonDetector(config.WEIGHTS_PATH)
    db = MongoDBClient(config.MONGO_URL, config.MONGO_DB, config.MONGO_COLLECTION)

    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=config.KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    print("Processing server started, waiting for frames...")

    try:
        for message in consumer:
            payload = message.value
            frame = decode_frame(payload["frame"])
            timestamp = payload["timestamp"]

            result = detector.detect(frame)
            db.save_detection(timestamp, result["count"], result["bboxes"])

            print(f"[ts={timestamp:.3f}] persons={result['count']}")

    except KeyboardInterrupt:
        print("Processing stopped.")
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()
