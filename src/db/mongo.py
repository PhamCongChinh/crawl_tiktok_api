from pymongo import MongoClient
from src.config.settings import MONGO

client = MongoClient(MONGO["uri"])
db = client[MONGO["db"]]
collection = db[MONGO["collection"]]