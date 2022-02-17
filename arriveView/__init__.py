#!/user/bin/python
# -*- coding:UTF-8 -*-
from flask import Blueprint

arrive_blue = Blueprint('arriveInfo', __name__, url_prefix='/arrive')
from . import views