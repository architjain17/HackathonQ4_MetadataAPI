from app import app
from flask import render_template
from flask import request
from flask import jsonify

import requests

from util import traverse_dict


@app.route('/')
@app.route('/index')
def index():
    return render_template('get_data_point.html')


@app.route('/get_by_id', methods=['POST', 'GET'])
def get_data_point_by_id():
    data_point_id = request.args.get('id')
    server = '127.0.0.1:5001'
    app_data = requests.get('http://{0}/metadata/:{1}'.format(server, data_point_id),
                            auth=('arjain', 'arjain')).json()

    return jsonify(app_data)


@app.route('/get_by_db', methods=['POST', 'GET'])
def get_data_point_by_db():
    database = request.args.get('database')
    server = '127.0.0.1:5001'
    app_data = requests.get('http://{0}/metadata/{1}'.format(server, database),
                            auth=('arjain', 'arjain')).json()

    return jsonify(app_data)


@app.route('/get_all', methods=['POST', 'GET'])
def get_data_point_all():
    server = '127.0.0.1:5001'
    app_data = requests.get('http://{0}/metadata'.format(server),
                            auth=('arjain', 'arjain')).json()

    return jsonify(app_data)


@app.route('/update', methods=['POST', 'GET'])
def update_data_point_by_id():
    server = '127.0.0.1:5001'
    data_point_id = request.args.get('update')

    if data_point_id:
        data = dict()
        human_name = request.args.get('human_name')
        desc = request.args.get('description')
        precision = request.args.get('precision')
        shift = request.args.get('shift')

        data['human_name'] = human_name
        data['desc'] = desc
        data['precision'] = precision
        data['shift'] = shift

        app_data = requests.patch('http://{0}/metadata/:{1}'.format(server, data_point_id),
                                  auth=('vsoni', 'vsoni'),
                                  data=data).json()
        return jsonify(app_data)

    data_point_id = request.args.get('id')
    app_data = requests.get('http://{0}/metadata/:{1}'.format(server, data_point_id),
                            auth=('arjain', 'arjain')).json()

    app_data = traverse_dict(app_data, final_dict=dict())
    return render_template('get_data_point.html', data_point=app_data, type='parsed_dict')
