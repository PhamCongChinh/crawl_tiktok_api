import asyncio
from datetime import datetime
import json
import httpx
from confluent_kafka import Consumer, KafkaException
from loguru import logger as log
from src.db.mongo import collection

from src.kafka.producer import send_crawl_result
from src.kafka.utils import flatten_post_data_unclassified
from src.config.settings import KAFKA
import src.scraper.scraper as scraper

async def kafka_consume(agent_name: str):
    consumer = Consumer({
        'bootstrap.servers': KAFKA['broker'],
        'group.id': KAFKA['group_id'],
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([KAFKA['topic']])

    log.info(f"🟢 [{agent_name}] Listening Kafka topic: {KAFKA['topic']}")

    try:
        while True:
            msg = consumer.poll(5.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            
            value = msg.value().decode("utf-8").strip()
            log.info(f"📩 Message received: {value}")

            try:
                data = json.loads(value)
                keyword = data.get("keyword", "")
                log.info(f"🔑 Keyword: {keyword}")

                send_crawl_result(
                    type="tiktok_keyword",
                    task_id=data.get("task_id", ""),
                    keyword=keyword,
                    platform=data.get("platform", ""),
                    created_at=data.get("created_at", ""),
                    topic=data.get("topic", ""),
                    assigned_bot=data.get("assigned_bot", ""),
                    status="RUNNING",
                    success=True,
                    bot_id=agent_name
                )

                # search_data = []
                search_data = await scraper.scrape_search(keyword=keyword, max_search=12)
                await asyncio.sleep(10)

                if len(search_data) > 0:
                    docs = []  # Gom tất cả doc vào đây
                    for document in search_data:
                        doc = flatten_post_data_unclassified(document)
                        docs.append(doc)  # gom vào danh sách

                        collection.update_one(
                            {"url": doc["url"]},
                            {"$set": {**doc, "updatedAt": datetime.now()}},
                            upsert=True
                        )
                        log.info(f"✅ Saved: {doc['url']}")

                    for d in docs:
                        d.pop("createdAt", None)
                        d.pop("updatedAt", None)

                    api_url = "http://103.97.125.64:4416/api/v1/posts/insert-unclassified-org-posts"
                    headers = {"Content-Type": "application/json"}

                    payload = {
                        "index": "not_classify_org_posts",
                        "data": docs,
                        "upsert": True
                    }

                    try:
                        async with httpx.AsyncClient(timeout=30.0) as client:
                            response = await client.post(api_url, json=payload, headers=headers)
                            if response.status_code == 200:
                                log.info(f"📤 Sent {len(docs)} docs to API successfully")
                            else:
                                log.error(f"❌ Failed to send ({response.status_code}): {response.text}")
                    except httpx.RequestError as e:
                        log.error(f"⚠️ Network error while sending docs: {e}")
                    except Exception as e:
                        log.error(f"❌ Unexpected error: {e}")

                send_crawl_result(
                    type="tiktok_keyword",
                    task_id=data.get("task_id", ""),
                    keyword=keyword,
                    platform=data.get("platform", ""),
                    created_at=data.get("created_at", ""),
                    topic=data.get("topic", ""),
                    assigned_bot=data.get("assigned_bot", ""),
                    status="DONE",
                    success=True,
                    bot_id=agent_name
                )
            except json.JSONDecodeError:
                log.error("❌ Invalid JSON")
    
    except KeyboardInterrupt:
        log.info("Consumer stopped.")
    finally:
        consumer.close()