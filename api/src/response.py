
def prepare_data_response(data):
    data_response = {
      "meta": {
        "copyright": "Copyright 2016 Square Root",
        "authors": [
            "Archit Jain",
            "Hitesh Singh",
            "Vijayant Soni",
            "Abhishek Jain",
            "Mark Gorman"
        ]
      },
      "data": data
    }

    return data_response

def prepare_error_response(error):
    error_response = {
      "meta": {
        "copyright": "Copyright 2016 Square Root",
        "authors": [
            "Archit Jain",
            "Hitesh Singh",
            "Vijayant Soni",
            "Abhishek Jain",
            "Mark Gorman"
        ]
      },
      "errors": error
    }

    return error_response
