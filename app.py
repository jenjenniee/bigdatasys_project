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

    # 7. 전 지역, 전 기간의 주문된 배달음식 품목 순위
    all_stats = collection.aggregate([
        {"$group": {"_id": "$Cuisine", "total_orders": {"$sum": 1}}},
        {"$sort": {"total_orders": -1}}
    ])

    # 8. 요일별로 어떤 Cuisine이 많이 배달되었는지 통계
    date_cuisine = collection.aggregate([
        {"$group": {"_id": {"Cuisine": "$Cuisine", "OrderWeek": "$OrderWeek"}, "total_orders": {"$sum": 1}}},
        {"$group": {"_id": "$_id.OrderWeek", "top_cuisine": {"$push": {"Cuisine": "$_id.Cuisine", "total_orders": "$total_orders"}}}},
        {"$project": {"_id": 0, "OrderWeek": "$_id", "top_cuisine": {"$slice": ["$top_cuisine", 5]}}}
    ])

    # 9. 지역별로 매 달 배송 수가 많은 지역 top5 추출
    all_months = collection.aggregate([
        {"$group": {"_id": {"month": {"$month": {"$dateFromString": {"dateString": "$Date"}}}, "province": "$AREA.Province"}, "total_orders": {"$sum": 1}}},
        {"$sort": {"_id.month": 1, "total_orders": -1}},
        {"$group": {"_id": "$_id.month", "top_regions": {"$push": {"province": "$_id.province", "total_orders": "$total_orders"}}}},
        {"$project": {"_id": 0, "month": "$_id", "top_regions": {"$slice": ["$top_regions", 5]}}}
    ])

    # 데이터 변환
    distance_stats = list(distance_stats)
    date_stats = list(date_stats)
    weather_stats = list(weather_stats)
    all_stats = list(all_stats)
    date_cuisine = list(date_cuisine)
    all_months = list(all_months)

    return render_template('index.html', distance_stats=distance_stats, date_stats=date_stats,
                           weather_stats=weather_stats, all_stats=all_stats, date_cuisine=date_cuisine, all_months=all_months)


if __name__ == '__main__':
    app.run(debug=True)
