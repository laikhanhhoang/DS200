from pymongo import MongoClient


class MongoDBClient:
    def __init__(self, url: str, db_name: str, collection_name: str):
        self.client = MongoClient(url)
        self.collection = self.client[db_name][collection_name]

    def save_detection(self, timestamp: float, count: int, bboxes: list):
        self.collection.insert_one(
            {
                "timestamp": timestamp,
                "person_count": count,
                "bboxes": bboxes,
            }
        )

    def close(self):
        self.client.close()
