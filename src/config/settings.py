from src.config import MONGO_URI, MONGO_DB, MONGO_COLLECTION, TOPIC, KAFKA_BROKER

# MongoDB
MONGO = {
    "uri": MONGO_URI,
    "db": MONGO_DB,
    "collection": MONGO_COLLECTION,
}

# Kafka
KAFKA = {
    "broker": KAFKA_BROKER,
    "topic": TOPIC,
    "group_id": "crawl_group",
}