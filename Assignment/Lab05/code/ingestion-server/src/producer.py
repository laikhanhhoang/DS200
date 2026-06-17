import base64
import json
from kafka import KafkaProducer


class FrameProducer:
    def __init__(self, bootstrap_servers: str, topic: str = "camera-frames"):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send_frame(self, frame_bytes: bytes, timestamp: float):
        encoded = base64.b64encode(frame_bytes).decode("utf-8")
        self.producer.send(self.topic, value={"frame": encoded, "timestamp": timestamp})

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()
