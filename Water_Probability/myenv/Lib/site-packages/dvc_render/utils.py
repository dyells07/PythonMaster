from flatten_dict import flatten  # type: ignore[import]


def list_dict_to_dict_list(list_dict):
    """Convert from list of dictionaries to dictionary of lists."""
    if not list_dict:
        return {}
    flat_list_dict = [flatten(d, reducer="dot") for d in list_dict]
    return {k: [x[k] for x in flat_list_dict] for k in flat_list_dict[0]}
