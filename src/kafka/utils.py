from datetime import datetime
from bson import ObjectId

def json_converter(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Type {obj.__class__.__name__} không thể serialize.")

def flatten_post_data_unclassified(data: dict) -> dict:
    return {
        "doc_type": 1,
        "crawl_source": 2,
        "crawl_source_code": "tt",
        "pub_time": data.get("createTime", 0),
        "crawl_time": int(datetime.now().timestamp()),
        "subject_id": data.get("subject_id", None),
        "title": data.get("title", None),
        "description": data.get("description", None),
        "content": data.get("desc", None),
        "url": f"https://www.tiktok.com/@{data.get('author', {}).get('uniqueId', '')}/video/{data['id']}",
        "media_urls": "[]",
        "comments": data.get("stats", {}).get("commentCount", 0),
        "shares": data.get("stats", {}).get("shareCount", 0),
        "reactions": data.get("stats", {}).get("diggCount", 0),
        "favors": int(data.get("stats", {}).get("collectCount", 0) or 0),
        "views": data.get("stats", {}).get("playCount", 0),
        "web_tags": "[]",
        "web_keywords": "[]",
        "auth_id": data.get("author", {}).get("id", ""),
        "auth_name": data.get("author", {}).get("nickname", ""),
        "auth_type": 1,
        "auth_url": f"https://www.tiktok.com/@{data.get('author', {}).get('uniqueId', '')}",
        "source_id": data.get("id", None),
        "source_type": 5,
        "source_url": f"https://www.tiktok.com/@{data.get('author', {}).get('uniqueId', '')}/video/{data['id']}",
        "sentiment": 0,
        "isPriority": False,
        "crawl_bot": "tiktok_1",
        "createdAt": datetime.now(),
        "updatedAt": datetime.now()
    }