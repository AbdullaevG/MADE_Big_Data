import json

from pyspark.sql.functions import col, struct


def create_inner(schema, update_dict, name):

    result = {}
    for field in schema["fields"]:

        if name:
            full_name = f"{name}.{field['name']}"
        else:
            full_name = field["name"]

        if isinstance(field["type"], dict):

            result[field["name"]] = create_inner(field["type"], update_dict, full_name)

        else:
            if full_name in update_dict:
                result[field["name"]] = update_dict[full_name]
                update_dict.pop(full_name)
            else:
                result[field["name"]] = col(full_name)
    return result


def parse_external(update_dict, selected_dict):

    for key, value in update_dict.items():
        if key.count(".") == 0:
            selected_dict[key] = value
        else:
            inner_keys = key.split(".")
            inner_key = inner_keys[0]
            if inner_key not in selected_dict:
                selected_dict[inner_key] = {}
            inner_dict = selected_dict[inner_key]
            for inner_key in inner_keys[1:-1]:
                if inner_key not in inner_dict:
                    inner_dict[inner_key] = {}
                inner_dict = inner_dict[inner_key]
            inner_dict[inner_keys[-1]] = value
    return selected_dict


def struct_dict(parsed):

    result = []
    for key, value in parsed.items():
        if isinstance(value, dict):
            result.append(struct(*struct_dict(value)).alias(key))
        else:
            result.append(value.alias(key))
    return result


def update_df(df, columns_dict):

    schema = json.loads(df.schema.json())
    result_dict = create_inner(schema, columns_dict, "")
    result_dict = parse_external(columns_dict, result_dict)
    new_columns = struct_dict(result_dict)
    return df.select(*new_columns)
