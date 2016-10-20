# HackathonQ4_MetadataAPI

run.py : Starting point of the web app
1) In terminal, cd rootFolderofApp and execute ./run.py
2) In browser, http://localhost:5000/

Flow :
1) run.py imports the variable 'app' from app module (user defined)
   The 'app' variable contains the Flask app instance. run.py then calls method 'run' on that instance

2) The endpoints will go in 'view.py'
   User route decorators to create URL mapping for the endpoints

Other than endpoints, we can create separate files/folders to put our code.
