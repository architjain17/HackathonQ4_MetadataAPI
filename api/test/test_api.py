import requests
import json

server = '127.0.0.1:5001'

# testing /get endpoint
app_data = requests.get('http://{0}/metadata'.format(server), auth=('arjain', 'arjain')).json()

print type(app_data)
print str(app_data)[:100]


# testing /get/Database endpoint
database = 'Dogfood'
app_data = requests.get('http://{0}/metadata/{1}'.format(server, database), auth=('arjain', 'arjain')).json()

print type(app_data)
print app_data


# testing /get/ID endpoint
id = '7871'
app_data = requests.get('http://{0}/metadata/:{1}'.format(server, id), auth=('arjain', 'arjain')).json()

print type(app_data)
print app_data

# testing /put/ID endpoint
id = '7871'
data = {'precision': 2}
app_data = requests.patch('http://{0}/metadata/:{1}'.format(server, id), auth=('arjain', 'arjain'), data=data).json()

print type(app_data)
print app_data


# testing /put/ID endpoint
id = '7871'
data = {'precision': 2, 'human_name': 'Store Target Net Cash'}
app_data = requests.patch('http://{0}/metadata/:{1}'.format(server, id), auth=('arjain', 'arjain'), data=data).json()

print type(app_data)
print app_data
