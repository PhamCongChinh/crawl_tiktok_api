import json
from datetime import datetime
from confluent_kafka import Producer
from loguru import logger as log
from src.config.settings import KAFKA
from src.config import CRAWL_RESULT_TOPIC

producer = Producer({'bootstrap.servers': KAFKA['broker']})

def send_crawl_result(type: str, task_id: str, keyword: str, platform: str,
                      created_at: str, topic: str, assigned_bot: str,
                      status: str, success: bool, bot_id: str):
    try:
        payload = {
            "type": type,
            "data": {
                "task_id": task_id,
                "keyword": keyword,
                "platform": platform,
                "created_at": created_at,
                "topic": topic,
                "assigned_bot": assigned_bot,
                "status": status
            },
            "success": success,
            "timestamp": datetime.now().isoformat(),
            "bot_id": bot_id
        }

        json_data = json.dumps(payload)
        producer.produce(topic=CRAWL_RESULT_TOPIC, value=json_data.encode('utf-8'))
        producer.flush()
        log.info(f"✅ Sent crawl result to Kafka: {json_data}")
    except Exception as e:
        log.error(f"❌ Failed to send crawl result: {e}")
