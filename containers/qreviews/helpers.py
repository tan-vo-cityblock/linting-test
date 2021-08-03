import collections
from typing import Any, Callable

from google.cloud import bigquery


def convert_snake_to_camel(column_name: str) -> str:
    words = column_name.split("_")
    return words[0] + ''.join(word.title() for word in words[1:])


def keymap_nested_dicts(ob: Any, func: Callable):
    if isinstance(ob, collections.Mapping):
        return {func(k): keymap_nested_dicts(v, func) for k, v in ob.items()}
    elif isinstance(ob, collections.MutableSequence):
        return [keymap_nested_dicts(x, func) for x in ob]
    else:
        return ob


def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]
