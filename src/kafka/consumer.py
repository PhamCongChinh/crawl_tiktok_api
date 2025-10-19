import asyncio
from datetime import datetime
import json
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
                    for document in search_data:
                        doc = flatten_post_data_unclassified(document)
                        result = collection.update_one(
                            {"url": doc["url"]},
                            {"$set": {**doc, "updatedAt": datetime.now()}},
                            upsert=True
                        )
                        log.info(f"✅ Saved: {doc['url']}")
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