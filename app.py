#!/user/bin/python
# -*- coding:UTF-8 -*-

from flask import Flask, render_template, request, make_response, jsonify
from flask_sqlalchemy import SQLAlchemy

from dbmodels import ticket

import pymysql

pymysql.install_as_MySQLdb()

app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = "mysql://root:Wzq20010915.@192.168.23.135:3306/flight"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/index")
def home():
    return index()


@app.route("/team")
def team():
    return render_template("team.html")


@app.route("/recommend", methods=["GET"])
def recommend():
    return render_template("recommend.html")


# ajax 不跳转页面显示推荐机票信息 按票价升序排列
@app.route('/recommendInfo', methods=['POST'])
def recommendInfo():
    args = request.values.to_dict()
    leaveValue = args['leave']
    arriveValue = args['arrive']
    dateValue = args['leaveDate']
    print(leaveValue + " " + arriveValue + " " + dateValue)
    if leaveValue == '西安':
        tickets = ticket.LeaveTicket.query. \
            filter(ticket.LeaveTicket.destination == arriveValue, ticket.LeaveTicket.date == dateValue).order_by(ticket.LeaveTicket.lowestPrice.asc()).all()
    elif arriveValue == '西安':
        tickets = ticket.ArriveTicket.query. \
            filter(ticket.ArriveTicket.departure == leaveValue, ticket.ArriveTicket.date == dateValue).order_by(ticket.ArriveTicket.lowestPrice.asc()).all()
    else:
        tickets = []
    response = []
    for singleTicket in tickets:
        info = dict(zip(['起点', '终点', '日期', '航空公司', '飞机机型', '起飞时间', '起飞机场', '飞行方式', '到达时间', '到达机场', '准点率', '最低价格'],
                        [singleTicket.departure, singleTicket.destination, singleTicket.date, singleTicket.company,
                         singleTicket.model, singleTicket.leaveTime, singleTicket.leavePort, singleTicket.way, singleTicket.arriveTime,
                         singleTicket.arrivePort, singleTicket.punctualRate, singleTicket.lowestPrice]))
        response.append(info)
    #数据以json对象数组形式传递
    response = make_response(jsonify(response))
    print(response)
    return response

#ajax传递数据，按准点率降序排列
@app.route("/recommendPunctualRate", methods=["POST"])
def recommendPunctualRate():
    args = request.values.to_dict()
    leaveValue = args['leave']
    arriveValue = args['arrive']
    dateValue = args['leaveDate']
    print(leaveValue + " " + arriveValue + " " + dateValue)  #改成小数排序，百分数直接排序会出错
    if leaveValue == '西安':
        tickets = ticket.LeaveTicket.query. \
            filter(ticket.LeaveTicket.destination == arriveValue, ticket.LeaveTicket.date == dateValue).order_by(0.01*ticket.LeaveTicket.punctualRate.desc()).all()
    elif arriveValue == '西安':
        tickets = ticket.ArriveTicket.query. \
            filter(ticket.ArriveTicket.departure == leaveValue, ticket.ArriveTicket.date == dateValue).order_by(0.01*ticket.ArriveTicket.punctualRate.desc()).all()
    else:
        tickets = []
    response = []
    for singleTicket in tickets:
        info = dict(zip(['起点', '终点', '日期', '航空公司', '飞机机型', '起飞时间', '起飞机场', '飞行方式', '到达时间', '到达机场', '准点率', '最低价格'],
                        [singleTicket.departure, singleTicket.destination, singleTicket.date, singleTicket.company,
                         singleTicket.model, singleTicket.leaveTime, singleTicket.leavePort, singleTicket.way, singleTicket.arriveTime,
                         singleTicket.arrivePort, singleTicket.punctualRate, singleTicket.lowestPrice]))
        response.append(info)
    #数据以json对象数组形式传递
    response = make_response(jsonify(response))
    print(response)
    return response

from arriveView import arrive_blue
from leaveView import leave_blue

app.register_blueprint(arrive_blue)
app.register_blueprint(leave_blue)

if __name__ == '__main__':
    db.create_all(app=app)
    app.run(port=6699)
