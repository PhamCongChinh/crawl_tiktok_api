import asyncio
import json
from confluent_kafka import Consumer, KafkaException
from loguru import logger as log
from datetime import datetime

from src.config.settings import KAFKA

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
            except json.JSONDecodeError:
                log.error("❌ Invalid JSON")
    
    except KeyboardInterrupt:
        log.info("Consumer stopped.")
    finally:
        consumer.close()