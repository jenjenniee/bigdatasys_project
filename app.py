from flask import Flask, render_template
from pymongo import MongoClient

app = Flask(__name__)

@app.route('/')
@app.route('/home', methods =['POST'])
def home():
    client = MongoClient('mongodb://localhost:27017/')

    # db접근
    db = client.practice
    # collection접근
    collection = db.sutudents

    # 임시쿼리 - collect에서 모든 데이터 가져오기
    students = list(collection.find())


    return render_template('index.html', students=students)

if __name__ == '__main__':
    app.run(debug=True)