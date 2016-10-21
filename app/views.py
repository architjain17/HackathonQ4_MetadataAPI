from app import app
from flask import render_template
from flask import request
from flask import jsonify

import os
import json
import requests

from util import traverse_dict


def read_config():
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config/api_config.json')
    with open(config_file) as cf:
        config = json.load(cf)

    return config

config = read_config()
SERVER = config['server']
USERNAME = config['username']
PASSWORD = config['password']


@app.route('/')
@app.route('/index')
def index():
    return render_template('get_data_point.html')


@app.route('/get_by_id', methods=['POST', 'GET'])
def get_data_point_by_id():
    data_point_id = request.args.get('id')
    app_data = requests.get('http://{0}/metadata/:{1}'.format(SERVER, data_point_id),
                            auth=(USERNAME, PASSWORD)).json()

    return jsonify(app_data)


@app.route('/get_by_db', methods=['POST', 'GET'])
def get_data_point_by_db():
    database = request.args.get('database')
    app_data = requests.get('http://{0}/metadata/{1}'.format(SERVER, database),
                            auth=(USERNAME, PASSWORD)).json()

    return jsonify(app_data)


@app.route('/get_all', methods=['POST', 'GET'])
def get_data_point_all():
    app_data = requests.get('http://{0}/metadata'.format(SERVER),
                            auth=(USERNAME, PASSWORD)).json()

    return jsonify(app_data)


@app.route('/update', methods=['POST', 'GET'])
def update_data_point_by_id():
    data_point_id = request.args.get('update')

    if data_point_id:
        data = dict()
        human_name = request.args.get('human_name')
        description = request.args.get('description')
        precision = request.args.get('precision')
        shift = request.args.get('shift')

        data['human_name'] = human_name
        data['description'] = description
        data['precision'] = precision
        data['shift'] = shift

        app_data = requests.patch('http://{0}/metadata/:{1}'.format(SERVER, data_point_id),
                                  auth=(USERNAME, PASSWORD),
                                  data=data).json()
        return jsonify(app_data)

    data_point_id = request.args.get('id')
    app_data = requests.get('http://{0}/metadata/:{1}'.format(SERVER, data_point_id),
                            auth=(USERNAME, PASSWORD)).json()

    app_data = traverse_dict(app_data, final_dict=dict())
    return render_template('update_data_point.html', data_point=app_data, type='parsed_dict')
