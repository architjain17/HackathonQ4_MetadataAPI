from functools import wraps

# internal imports
from response import prepare_error_response


def try_except(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception, e:
            print e
            error = {
            "status": "500",
            "error": "Internal Server Error"
            }
            return (prepare_error_response(error), 500)
    return wrapped
