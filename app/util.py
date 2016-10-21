def traverse_dict(app_data, final_dict):
    for k, v in app_data.iteritems():
        if isinstance(v, dict):
            # Traverse nested dictionary
            traverse_dict(v, final_dict)
        else:
            if isinstance(v, list):
                # Unicode to standard string
                v = [str(item) for item in v]
            final_dict[k] = v
    return final_dict
