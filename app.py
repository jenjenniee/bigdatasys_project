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

    # 4.주거 지역별 배달 음식 품목 통계
    delivery_city_stats = collection.aggregate([
    {"$match": {
        "AREA.City": {
            "$ne": "null",
            "$ne": ""
        }
    }},
    
    {"$group": {
        "_id": {
            "City": "$AREA.City",
            "Cuisine": "$Cuisine"
        },
        "total_orders": {"$sum": 1}
    }},
    
    {"$sort": {"total_orders": -1}}
    ])
    
    # 5. 가구 유형별 배달량 및 금액 통계
    Ftypes_cuisine_stats = collection.aggregate([
    {
        "$group": {
            "_id": {
                "Cuisine": "$Cuisine",
                "FType": "$FType"
            },
            "total_orders": { "$sum": 1 },
            "average_price": { "$avg": "$Price" },
            "min_price": { "$min": "$Price" },
            "max_price": { "$max": "$Price" }
        }
    },
    {
        "$sort": { "total_orders": -1 }
    },
    {
        "$group": {
            "_id": "$_id.Cuisine",
            "ftypes": {
                "$push": {
                    "FType": "$_id.FType",
                    #"total_orders": "$total_orders",
                    "average_price": "$average_price",
                    "min_price": "$min_price",
                    "max_price": "$max_price"
                }
            }
        }
    },
    {
        "$project": {
            "_id": 1,
            "top_3_ftypes": { "$slice": ["$ftypes", 3] }  #상위 가구 3개까지 
        }
    }
])
    # 6. 연령대별 배달 음식 품목 통계
    age_stats = collection.aggregate([
     {
        "$group": {
            "_id": {
                "age_group": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lte": ["$Age", 10]}, "then": "0-10"},
                            {"case": {"$lte": ["$Age", 20]}, "then": "11-20"},
                            {"case": {"$lte": ["$Age", 30]}, "then": "21-30"},
                            {"case": {"$lte": ["$Age", 40]}, "then": "31-40"},
                            {"case": {"$lte": ["$Age", 50]}, "then": "41-50"},
                        ],
                        "default": "etc"
                    }
                },
                "Cuisine": "$Cuisine"
            },
            "total_orders": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id.age_group": 1, "total_orders": -1}
    }
])
    
    # 7. 배달음식 품목별 순위 통계
    all_stats = collection.aggregate([
        {"$group": {"_id": "$Cuisine", "total_orders": {"$sum": 1}}},
        {"$sort": {"total_orders": -1}}
    ])

    # 8. 2020년 7월 기준 각 "도"들의 Cuisine 별 배달량 통계
    date_cuisine = collection.aggregate([
    {"$match": {
        "Date": {"$regex": "^2020-07"},
        "AREA.Province": {"$ne": "null", "$ne": ""},
        "AREA.City": {"$ne": "null", "$ne": ""}
    }},
    {"$group": {
        "_id": {
            "Province": "$AREA.Province",
            "City": "$AREA.City",
            "Cuisine": "$Cuisine"
        },
        "total_orders": {"$sum": 1}
    }},
    {"$sort": {"_id.Province": 1, "_id.City": 1, "total_orders": -1}}
])

    # 9. 배달 평균 금액이 큰 동네 TOP3 통계
    highest_price_neighborhoods = collection.aggregate([
    {"$group": {
        "_id": {
            "Province": "$AREA.Province",
            "City": "$AREA.City",
            "Neighborhood": "$AREA.Neighborhood"
        },
        "avg_price": {"$avg": "$Price"}
    }},
    {"$sort": {"avg_price": -1}},
    {"$limit": 3}
])
    
    
    # aggregate([
    #     {"$group": {"_id": "$AREA.Neighborhood", "avg_price": {"$avg": "$Price"}}},
    #     {"$sort": {"avg_price": -1}},
    #     {"$limit": 3}
    # ])

    



    

    # 데이터 변환
    
    distance_stats = list(distance_stats)
    date_stats = list(date_stats)
    weather_stats = list(weather_stats)
    delivery_city_stats = list(delivery_city_stats)
    Ftypes_cuisine_stats = list(Ftypes_cuisine_stats)
    age_stats = list(age_stats)
    all_stats = list(all_stats)
    date_cuisine = list(date_cuisine)
    highest_price_neighborhoods = list(highest_price_neighborhoods) 

    return render_template('index.html', distance_stats=distance_stats, date_stats=date_stats,weather_stats=weather_stats, 
                           delivery_city_stats=delivery_city_stats, Ftypes_cuisine_stats=Ftypes_cuisine_stats,age_stats=age_stats,
                           all_stats=all_stats, date_cuisine=date_cuisine, highest_price_neighborhoods=highest_price_neighborhoods)


if __name__ == '__main__':
    app.run(debug=True)
