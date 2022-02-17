#!/user/bin/python
# -*- coding:UTF-8 -*-
from app import db
from sqlalchemy import Column, String, Integer


class LeaveTicket(db.Model):
    __tablename__ = 'leaveTicket'
    id = Column(Integer, primary_key=True)
    departure = Column(String(10))
    destination = Column(String(10))
    date = Column(String(10))
    company = Column(String(20))
    model = Column(String(10))
    leaveTime = Column(String(10))
    leavePort = Column(String(10))
    way = Column(String(10))
    arriveTime = Column(String(10))
    arrivePort = Column(String(10))
    punctualRate = Column(String(10))
    lowestPrice = Column(Integer)

    def __init__(self, id, departure, destination, date, company, model, leaveTime, leavePort, way, arriveTime,
                 arrivePort, punctualRate, lowestPrice):
        self.id = id
        self.departure = departure
        self.destination = destination
        self.date = date
        self.company = company
        self.model = model
        self.leaveTime = leaveTime
        self.leavePort = leavePort
        self.way = way
        self.arriveTime = arriveTime
        self.arrivePort = arrivePort
        self.punctualRate = punctualRate
        self.lowestPrice = lowestPrice


class ArriveTicket(db.Model):
    __tablename__ = 'arriveTicket'
    id = Column(Integer, primary_key=True)
    departure = Column(String(10))
    destination = Column(String(10))
    date = Column(String(10))
    company = Column(String(20))
    model = Column(String(10))
    leaveTime = Column(String(10))
    leavePort = Column(String(10))
    way = Column(String(10))
    arriveTime = Column(String(10))
    arrivePort = Column(String(10))
    punctualRate = Column(String(10))
    lowestPrice = Column(Integer)

    def __init__(self, id, departure, destination, date, company, model, leaveTime, leavePort, way, arriveTime,
                 arrivePort, punctualRate, lowestPrice):
        self.id = id
        self.departure = departure
        self.destination = destination
        self.date = date
        self.company = company
        self.model = model
        self.leaveTime = leaveTime
        self.leavePort = leavePort
        self.way = way
        self.arriveTime = arriveTime
        self.arrivePort = arrivePort
        self.punctualRate = punctualRate
        self.lowestPrice = lowestPrice
