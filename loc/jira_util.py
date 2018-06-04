def get_field_name_to_id(jira):
    fields = jira.fields()
    name_to_id = {}
    for f in fields:
        name_to_id[f['name']] = f['id']
    return name_to_id
