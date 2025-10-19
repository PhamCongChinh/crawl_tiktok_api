import argparse
import asyncio
import datetime
from pathlib import Path
import time
import scraper
import json
from loguru import logger as log
import scraper
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.admin import AdminClient
from src.config import MONGO_URI, MONGO_DB, MONGO_COLLECTION, TOPIC, KAFKA_BROKER

# admin = AdminClient({'bootstrap.servers': KAFKA_BROKER})
# topic = TOPIC

# from pymongo import MongoClient
# client = MongoClient(MONGO_URI)
# # Lấy database
# db = client[MONGO_DB]
# # Lấy collection
# collection = db[MONGO_COLLECTION]

# from bson import Int64, ObjectId
# def json_converter(obj):
#     if isinstance(obj, ObjectId):
#         return str(obj)  # Chuyển ObjectId thành string
#     raise TypeError(f"Type {obj.__class__.__name__} không thể serialize.")

# conf = {
#     'bootstrap.servers': '192.168.1.28:9092',
#     'group.id': 'crawl_group',
#     'auto.offset.reset': 'earliest'
# }

# conf1 = {
#     'bootstrap.servers': '192.168.1.28:9092',
# }

# KAFKA_TOPIC = TOPIC
# producer = Producer(conf1)

# def flatten_post_data_unclassified(data: dict) -> dict:
#     return {
#         # "id": raw.get("id", None),
#         "doc_type": 1,  # POST = 1, COMMENT = 2
#         "crawl_source": 2,
#         "crawl_source_code":"tt",
#         # "pub_time": Int64(int(data.get("createTime", 0))),
#         "pub_time": data.get("createTime", 0),
#         "crawl_time": int(datetime.now().timestamp()),
#         # "org_id": channel.get("org_id", None),
#         "subject_id": data.get("subject_id", None),
#         "title": data.get("title", None),
#         "description": data.get("description", None),
#         "content": data.get("desc", None),
#         "url": f"https://www.tiktok.com/@{data.get('author', {}).get('uniqueId', '')}/video/{data['id']}",
#         "media_urls": "[]",
#         "comments": data.get("stats", {}).get("commentCount", 0),
#         "shares": data.get("stats", {}).get("shareCount", 0),
#         "reactions": data.get("stats", {}).get("diggCount", 0),
#         "favors": int(data.get("stats", {}).get("collectCount", 0) or 0),
#         "views": data.get("stats", {}).get("playCount", 0),
#         "web_tags": "[]",#json.dumps(raw.get("diversificationLabels", [])),
#         "web_keywords": "[]",# json.dumps(raw.get("suggestedWords", [])),
#         "auth_id": data.get("author", {}).get("id", ""),
#         "auth_name": data.get("author", {}).get("nickname", ""),
#         "auth_type": 1,
#         "auth_url": f"https://www.tiktok.com/@{data.get('author', {}).get('uniqueId', '')}",
#         "source_id": data.get("id", None),
#         "source_type": 5,
#         "source_name": None,
#         "source_url": f"https://www.tiktok.com/@{data.get('author', {}).get('uniqueId', '')}/video/{data['id']}",
#         "reply_to": None,
#         "level": None,
#         "sentiment": 0,
#         "isPriority": False,
#         "crawl_bot": "tiktok_1",
#         "createdAt": datetime.now(),
#         "updatedAt": datetime.now()
#     }


# def send_crawl_result(type: str, task_id: str, keyword: str, platform: str, created_at: str, topic: str, assigned_bot: str, status: str, success: bool, bot_id: str):
    
#     try:
#         payload = {
#             "type": type,
#             "data" : {
#                 "task_id": task_id,
#                 "keyword": keyword,
#                 "platform": platform,
#                 "created_at": created_at,
#                 "topic": topic,
#                 "assigned_bot": assigned_bot,
#                 "status": status
#             },
#             "success": success,
#             "timestamp": datetime.now().isoformat(),
#             "bot_id": bot_id
#         }

#         json_data = json.dumps(payload)

#         producer.produce(
#             topic='crawl-results',
#             value=json_data.encode('utf-8')
#         )

#         producer.flush()
#         log.info("Sent crawl result to Kafka: %s", json_data)
#         print(json_data)

#     except Exception as e:
#         log.error("Failed to send crawl result to Kafka: %s", e)

# # ======================
# # Hàm consume
# # ======================
# async def kafka_consume(agent_name: str):
#     # conf = {
#     #     'bootstrap.servers': KAFKA_BROKER,
#     #     'group.id': 'crawl_group',
#     #     'auto.offset.reset': 'earliest'
#     # }
#     consumer = Consumer(conf)
#     consumer.subscribe([KAFKA_TOPIC])

#     log.info(f"🟢 [{agent_name}] Đang lắng nghe Kafka topic: {KAFKA_TOPIC} ...")

#     try:
#         while True:
#             msg = consumer.poll(5.0)  # chờ 1s để lấy message
#             if msg is None:
#                 continue
#             if msg.error():
#                 raise KafkaException(msg.error())

#             # Decode message
#             value = msg.value().decode("utf-8").strip()
#             log.info(f"📩 Nhận message: {value}")

#             try:
#                 data = json.loads(value)
#                 log.info(data)
#                 keyword = data.get("keyword", "")
#                 log.info(f"🔑 Keyword lấy được: {keyword}")

#             #     task_id = data.get("job_id", "")
#                 type = "tiktok_keyword"
#                 bot_id = agent_name
#                 send_crawl_result(
#                     type=type,
#                     task_id=data.get("task_id", ""),
#                     keyword=data.get("keyword", ""),
#                     platform=data.get("platform", ""),
#                     created_at=data.get("created_at", ""),
#                     topic=data.get("topic", ""),
#                     assigned_bot=data.get("assigned_bot", ""),
#                     status="RUNNING",
#                     success=True,
#                     bot_id=bot_id
#                 )

#                 original_data = data

#                 # search_data = await scraper.scrape_search(
#                 #     keyword=keyword,
#                 #     max_search=12
#                 # )
#                 search_data = []
#                 await asyncio.sleep(10)

#                 if len(search_data) > 0:
#                     for document in search_data:
#                         # Sử dụng update_one với upsert=True để insert hoặc update
#                         data = flatten_post_data_unclassified(document)
#                         result = collection.update_one(
#                             {"url": data["url"]},  # Tìm kiếm theo trường 'id'
#                             {
#                                 "$set": {
#                                     **data,
#                                     "updatedAt": datetime.now(),
#                                 }# Cập nhật hoặc chèn document mới
#                             },
#                             upsert=True  # Nếu không tìm thấy document với id này, sẽ chèn mới
#                         )

#                         if result.upserted_id:
#                             log.info(f"Đã thêm document mới với id: {result.upserted_id}")
#                         else:
#                             log.info(f"Đã cập nhật document với id: {data['url']}")

#                     send_crawl_result(
#                         type=type,
#                         task_id=original_data.get("task_id", ""),
#                         keyword=original_data.get("keyword", ""),
#                         platform=original_data.get("platform", ""),
#                         created_at=original_data.get("created_at", ""),
#                         topic=original_data.get("topic", ""),
#                         assigned_bot=original_data.get("assigned_bot", ""),
#                         status="DONE",
#                         success=True,
#                         bot_id=bot_id
#                     )
#                 else:
#                     send_crawl_result(
#                         type=type,
#                         task_id=original_data.get("task_id", ""),
#                         keyword=original_data.get("keyword", ""),
#                         platform=original_data.get("platform", ""),
#                         created_at=original_data.get("created_at", ""),
#                         topic=original_data.get("topic", ""),
#                         assigned_bot=original_data.get("assigned_bot", ""),
#                         status="DONE",
#                         success=True,
#                         bot_id=bot_id
#                     )
                
#             except json.JSONDecodeError:
#                 log.error("❌ Không thể parse message: Không phải chuỗi JSON hợp lệ")

#     except KeyboardInterrupt:
#         log.info("Dừng Kafka consumer.")
#     finally:
#         consumer.close()

from src.kafka.consumer import kafka_consume

if __name__ == "__main__":
    # meta = admin.list_topics(timeout=10)
    # log.info(len(meta.topics[topic].partitions))
    parser = argparse.ArgumentParser(description="Kafka consumer agent")
    parser.add_argument("--agent", required=True, help="Tên agent (ví dụ: tiktok_1, tiktok_2)")
    args = parser.parse_args()
    asyncio.run(kafka_consume(agent_name=args.agent))