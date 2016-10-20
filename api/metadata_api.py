from flask import Flask
from flask_restful import Resource, reqparse, Api
from collections import OrderedDict

# local imports
from src.try_except import try_except
from src.authorization import requires_auth
from src.db_connector import engine
from src.response import prepare_data_response, prepare_error_response


app = Flask(__name__)
api = Api(app)

class MetadataAPI(Resource):
    @try_except
    @requires_auth
    def get(self, username):
        conn = engine.connect()
        with open('sql/get_metadata.sql') as f:
            results = conn.execute(f.read())

        output = [row.row for row in results]
        results.close()
        conn.close()

        return (prepare_data_response(output), 200)


class MetadataAPIDatabase(Resource):
    @try_except
    @requires_auth
    def get(self, database, username):
        database_mapping = {
            'SPAAutoUpdate': 2,
            'NCIAutoUpdate': 3,
            'NMACAutoUpdate': 8,
            'Dogfood': 9,
            'VWAutoUpdate': 21,
            'CBAAutoUpdate': 22
        }

        dwdestinationid = database_mapping.get(database)

        if not dwdestinationid:
            error = {
                "status": "404",
                "error": "Database does not exists"
                }
            return (prepare_error_response(error), 404)

        conn = engine.connect()
        with open('sql/get_metadata_using_db.sql') as f:
            results = conn.execute(f.read().replace('{dwdestinationid}', str(dwdestinationid)))

        output = [row.row for row in results]
        results.close()
        conn.close()

        return (prepare_data_response(output), 200)


class MetadataAPIID(Resource):
    @try_except
    @requires_auth
    def get(self, id, username):
        conn = engine.connect()
        with open('sql/get_metadata_using_id.sql') as f:
            results = conn.execute(f.read().replace('{id}', id))

        if results.rowcount == 0:
            error = {
                "status": "404",
                "error": "ID does not exists"
                }
            return (prepare_error_response(error), 404)

        elif results.rowcount > 1:
            error = {
                "status": "100",
                "error": "SOS! Metadata corrupted!"
                }
            return (prepare_error_response(error), 404)

        output = results.fetchone().row

        results.close()
        conn.close()

        return (prepare_data_response(output), 200)

    @try_except
    @requires_auth
    def patch(self, id, username):

        # parsing data passed with request
        parser = reqparse.RequestParser()

        parser.add_argument('human_name', type=str, location='form', required=False)
        parser.add_argument('description', type=str, location='form', required=False)
        parser.add_argument('precision', type=int, location='form', required=False)
        parser.add_argument('shift', type=int, location='form', required=False)

        args = parser.parse_args()

        args_dic = {
        'human_name': args['human_name'],
        'description': args['description'],
        'precision': args['precision'],
        'shift': args['shift']
        }

        if sum(map(bool, [args_dic[key] for key in args_dic])) < 1:
            return {
                "status": "400",
                "error": "Please pass atleast one field-data pair to get updated"
                }

        update_fields = []
        comments = []

        for key in args_dic:
            if args_dic[key]:
                update_fields.append(key + " = '" + str(args_dic[key]) + "'")
                comments.append(key)

        update_fields = ', '.join(update_fields)
        comments = 'Updating ' + ', '.join(comments)
        modified_by = username

        conn = engine.connect()
        try:
            with open('sql/update_metadata_using_id.sql') as f:
                results = conn.execute(f.read().replace('{update_fields}', update_fields).replace('{id}', id).replace('{comments}', comments).replace('{modified_by}', modified_by))
        except Exception as e:
            return {
                "status": "400",
                "error": "Error while updating the resource"
                }

        results.close()
        conn.close()

        kwargs = {'id': id}
        output = MetadataAPIID().get(**kwargs)

        return output

api.add_resource(MetadataAPI, '/metadata')
api.add_resource(MetadataAPIID, '/metadata/:<id>')
api.add_resource(MetadataAPIDatabase, '/metadata/<database>')

if __name__ == '__main__':
    app.run(port=5001, debug=True)
