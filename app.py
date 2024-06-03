from flask import Flask, render_template
from pymongo import MongoClient
import json

app = Flask(__name__)

# MongoDB 연결 설정
client = MongoClient("mongodb://localhost:27017/")
db = client.delivery  # Database name: delivery
collection = db.data  # Collection name: data

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

    # 7. 배달음식 품목별 순위 통계
    all_stats = collection.aggregate([
        {"$group": {"_id": "$Cuisine", "total_orders": {"$sum": 1}}},
        {"$sort": {"total_orders": -1}}
    ])

    # 8. 2020년 7월에 가장 인기가 많았던 Cuisine의 내림차순 통계
    date_cuisine = collection.aggregate([
        {"$match": {"Date": {"$regex": "^2020-07"}}},
        {"$group": {"_id": "$Cuisine", "total_orders": {"$sum": 1}}},
        {"$sort": {"total_orders": -1}}
    ])

    # 9. 배달 평균 금액이 큰 동네 TOP3 통계
    lowest_price_neighborhoods = collection.aggregate([
        {"$group": {"_id": "$AREA.Neighborhood", "avg_price": {"$avg": "$Price"}}},
        {"$sort": {"avg_price": -1}},
        {"$limit": 3}
    ])




    

    # 데이터 변환
    distance_stats = list(distance_stats)
    date_stats = list(date_stats)
    weather_stats = list(weather_stats)
    all_stats = list(all_stats)
    date_cuisine = list(date_cuisine)
    lowest_price_neighborhoods = list(lowest_price_neighborhoods)

    return render_template('index.html', distance_stats=distance_stats, date_stats=date_stats,
                           weather_stats=weather_stats, all_stats=all_stats, date_cuisine=date_cuisine, lowest_price_neighborhoods=lowest_price_neighborhoods)


if __name__ == '__main__':
    app.run(debug=True)
