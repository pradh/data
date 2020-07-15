import logging

from flask import Flask, request

from app import utils
from app.executor import executor

app = Flask(__name__)


@app.route('/update', methods=['POST'])
def scheduled_updates():
    task_info = request.get_json(force=True)
    return executor.execute_import_on_update(task_info['absolute_import_name'])


@app.route('/_ah/start')
def start():
    utils.setup_logging()
    return ''


def main():
    app.run(host='127.0.0.1', port=8080, debug=True)
