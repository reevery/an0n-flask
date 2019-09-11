#!/usr/bin/python3
import sys
import os
import logging
from flask import Flask, render_template
# , Response, render_template, flash, redirect, jsonify
# from flask_wtf import FlaskForm
# from wtforms import HiddenField, SubmitField
# from urllib.parse import quote

logger = logging.getLogger(__name__)


app = Flask(__name__)
app.config['SECRET_KEY'] = 'you-will-never-guess'


@app.route("/")
def index():
    return render_template('index.html')


if __name__ == '__main__':
    logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'))

    app.run(host='0.0.0.0')
