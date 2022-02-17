#!/user/bin/python
# -*- coding:UTF-8 -*-

from flask import render_template
from leaveView import leave_blue
from dbmodels import ticket


@leave_blue.route("/info")
def index():
    return render_template("leaveTicket.html", tickets=ticket.LeaveTicket.query.all())
