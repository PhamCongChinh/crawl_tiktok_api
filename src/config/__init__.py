import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
TOPIC = os.getenv("TOPIC")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

# topic gửi kết quả crawl
CRAWL_RESULT_TOPIC = os.getenv("CRAWL_RESULT_TOPIC", "crawl-results")  # có giá trị mặc định nếu thiếu