#!/user/bin/python
# -*- coding:UTF-8 -*-

from flask import Blueprint


leave_blue = Blueprint('leaveInfo', __name__, url_prefix='/leave')
from . import views