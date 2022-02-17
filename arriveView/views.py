#!/user/bin/python
# -*- coding:UTF-8 -*-
from flask import  render_template
from arriveView import arrive_blue
from dbmodels import ticket


@arrive_blue.route("/info")
def arriveInfo():
    return render_template("arriveTicket.html", tickets = ticket.ArriveTicket.query.all())
