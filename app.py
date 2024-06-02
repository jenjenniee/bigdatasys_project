from flask import Flask, render_template
from pymongo import MongoClient
import json

app = Flask(__name__)

# MongoDB 연결 설정
client = MongoClient("mongodb://localhost:27017/")
db = client.delivery
collection = db.data


@app.route('/')
def index():
    # 1. Statistics on delivery volume by distance in residential areas
    distance_stats = collection.aggregate([
        {"$group": {"_id": "$Dist", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ])

    # 2. Statistics on delivery amounts by date (monthly, weekly) in residential areas
    date_stats = collection.aggregate([
        {"$group": {"_id": {"month": {"$substr": ["$Date", 0, 7]}}, "total_amount": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ])

    # 3. Statistics on delivery volume by weather in residential areas
    weather_stats = collection.aggregate([
        {"$group": {"_id": "$Weather", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ])

    # 데이터 변환
    distance_stats = list(distance_stats)
    date_stats = list(date_stats)
    weather_stats = list(weather_stats)

    return render_template('index.html', distance_stats=distance_stats, date_stats=date_stats,
                           weather_stats=weather_stats)


if __name__ == '__main__':
    app.run(debug=True)
