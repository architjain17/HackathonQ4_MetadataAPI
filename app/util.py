def traverse_dict(app_data, final_dict):
    for k, v in app_data.iteritems():
        if isinstance(v, dict):
            traverse_dict(v, final_dict)
        else:
            final_dict[k] = v
    return final_dict
