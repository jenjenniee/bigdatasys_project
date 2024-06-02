from flask import Flask, render_template, request, send_from_directory
from pymongo import MongoClient
import ast
import os

app = Flask(__name__)

# MongoDB 설정
client = MongoClient('mongodb://localhost:27017/')
db = client['delivery']
collection = db['deliveries']

# 쿼리 제목과 넘버 매핑 딕셔너리
query_title_to_number = {
    "Rank of delivery food items across all periods and regions": "query7",
    # 다른 쿼리들의 제목과 넘버 추가
}

@app.route('/')
def index():
    query_title = request.args.get('query_value')
    results = []

    if query_title:
        query_number = query_title_to_number.get(query_title)
        if query_number:
            # 해당 쿼리 넘버에 해당하는 이미지 반환
            return render_template('index.html', results=results, query_title=query_title, query_number=query_number)

    return render_template('index.html', results=results, query_title=query_title)

@app.route('/img/<query_number>')
def get_image(query_number):
    # 이미지가 있는 현재 디렉토리의 경로 설정
    image_directory = os.path.join(os.getcwd(), 'img')
    # 이미지를 해당 경로에서 가져와서 반환
    return send_from_directory(image_directory, f"{query_number}.png")

if __name__ == '__main__':
    
    app.run(debug=True)
