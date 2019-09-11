#!/usr/bin/python3
import sys
import os
import logging
from flask import Flask, render_template, redirect, url_for
# , Response, render_template, flash, redirect, jsonify
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from wtforms import SubmitField
# from urllib.parse import quote
from werkzeug.utils import secure_filename
from documentapi import initial

app = Flask(__name__)
app.config['SECRET_KEY'] = 'an0nymous'


class UploadForm(FlaskForm):
    filename = FileField(validators=[FileRequired()])
    submit = SubmitField('Upload')


@app.route("/", methods=['GET', 'POST'])
def index():
    form = UploadForm()

    if form.validate_on_submit():
        filename = secure_filename(form.filename.data.filename)
        app.logger.info(filename)
        # TODO Add path
        form.filename.data.save(filename)
        # filename = secure_filename(f.filename)
        # f.save(os.path.join(
        #     app.instance_path, 'twbx', filename
        # ))
        return redirect(url_for('index'))

    return render_template('index.html', form=form)


if __name__ == '__main__':
    logging.basicConfig(level=os.getenv('LOG_LEVEL', 'DEBUG'))

    app.run(host='0.0.0.0')
