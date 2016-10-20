from flask import Flask

app = Flask(__name__)

# Importing 'views' here because 'views' imports the above defined 'app' variable.
# Thus avoiding circular import error.
from app import views
