import numpy as np
from ultralytics import YOLO


class PersonDetector:
    PERSON_CLASS_ID = 0

    def __init__(self, weights_path: str):
        self.model = YOLO(weights_path)

    def detect(self, frame: np.ndarray) -> dict:
        results = self.model(frame, verbose=False)[0]
        bboxes = []

        for box in results.boxes:
            if int(box.cls[0]) != self.PERSON_CLASS_ID:
                continue
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            bboxes.append(
                {
                    "x1": round(x1, 2),
                    "y1": round(y1, 2),
                    "x2": round(x2, 2),
                    "y2": round(y2, 2),
                    "confidence": round(float(box.conf[0]), 4),
                }
            )

        return {"count": len(bboxes), "bboxes": bboxes}
