import os
import time
import cv2
from src.producer import FrameProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VIDEO_PATH = os.getenv("VIDEO_PATH", "/app/data/people_walking_video.mp4")
TARGET_FPS = 5  # frames per second to send


def main():
    producer = FrameProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    cap = cv2.VideoCapture(VIDEO_PATH)

    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {VIDEO_PATH}")

    video_fps = cap.get(cv2.CAP_PROP_FPS) or 30
    frame_skip = max(1, round(video_fps / TARGET_FPS))
    frame_idx = 0

    print(f"Ingestion started: {VIDEO_PATH} | video_fps={video_fps:.1f} | sending every {frame_skip} frames")

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                # loop video
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                frame_idx = 0
                continue

            if frame_idx % frame_skip == 0:
                _, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
                producer.send_frame(buffer.tobytes(), time.time())

            frame_idx += 1
            time.sleep(1.0 / video_fps)

    except KeyboardInterrupt:
        print("Ingestion stopped.")
    finally:
        cap.release()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
