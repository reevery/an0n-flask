#!/usr/bin/python3
import os
from flask import Flask, render_template, redirect, url_for, send_file
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from wtforms import SubmitField
from werkzeug.utils import secure_filename
from documentapi import initial

app = Flask(__name__)
app.config['SECRET_KEY'] = 'an0nymous'


class UploadForm(FlaskForm):
    filename = FileField(validators=[FileRequired()])
    submit = SubmitField('Upload')


class FieldSelectForm(FlaskForm):
    submit = SubmitField('Run')


@app.route("/", methods=['GET', 'POST'])
def index():
    form = UploadForm()

    if form.validate_on_submit():
        filename = secure_filename(form.filename.data.filename)
        app.logger.info(filename)
        # TODO Add path
        form.filename.data.save(filename)
        return redirect(url_for('select', filename=filename))
    return render_template('index.html', form=form)


@app.route("/select/<filename>", methods=['GET', 'POST'])
def select(filename):
    data = initial(filename)
    app.logger.info('Select: %s', data)

    form = FieldSelectForm()
    if form.validate_on_submit():
        return redirect(url_for('finish', filename=filename))
    return render_template('select.html', form=form, data=data, filename=filename)


@app.route("/finish/<filename>", methods=['GET', 'POST'])
def finish(filename):
    data = dict()
    data['filename'] = initial(filename)
    app.logger.info('Finish: %s', data)

    return render_template('download.html', data=data, filename=filename)


@app.route("/download/<filename>", methods=['GET', 'POST'])
def download(filename):
    return send_file(os.path.join(app.root_path, filename),
                     attachment_filename=filename,
                     as_attachment=True)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
