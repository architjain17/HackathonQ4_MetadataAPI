from functools import wraps
from flask_restful import reqparse

# local imports
from db_connector import engine


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # parsing data passed with request
        parser = reqparse.RequestParser()
        parser.add_argument('Authorization', type=str, location='headers', required=False)

        args = parser.parse_args()
        username = validate_credentials(args['Authorization'])

        if username:
            kwargs['username'] = username
            return f(*args, **kwargs)
        else:
            return {
                "status": "401",
                "error": "Unauthorized"
                }

    return decorated


def validate_credentials(user_pass):
    conn = engine.connect()
    query = """
        SELECT username
        FROM hackathon_q4_metadata_api.users
        WHERE encode(convert_to(username || ':' || password, 'UTF-8'), 'base64') = replace('{user_pass}', 'Basic ', '')
    """.format(user_pass=user_pass)

    results = conn.execute(query)
    row = results.fetchone()
    results.close()
    conn.close()

    if row:
        return row.username
    else:
        return None
