import functools
import sys
from datetime import date, datetime
from typing import Any, Dict, List

from pytz import utc


def rgetattr(obj: Any, attr: str, *args) -> Any:
    """Recursive implementation of python's `getattr` to handled dotted `attr` strings.
    See SO for more: https://stackoverflow.com/a/31174427
    """

    def _getattr(obj, attr):
        return getattr(obj, attr, *args)

    return functools.reduce(_getattr, [obj] + attr.split("."))


def datetime_to_iso_str(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def datetime_utc_from_date(date_str: str) -> datetime:
    try:
        naive_datetime = datetime.strptime(date_str, "%Y-%m-%d")
        return utc.localize(naive_datetime)
    except ValueError as err:
        raise ValueError(f"Date string {date_str} must be in format YYYY-MM-DD. {err}")


def date_utc_from_date(date_str) -> date:
    datetime_utc = datetime_utc_from_date(date_str)
    return datetime_utc.date()


def json_serialize(obj):
    """Method to be passed in to `default` param of
    `json.dumps(obj, default=json_serialize)`.
    Will be used as method to handle encoding of objects that `json` can't encode out
    of the box.

    See json.dumps docs for more on param `default` requirements.
    """
    if isinstance(obj, datetime):
        return datetime_to_iso_str(obj)
    raise TypeError(
        (
            f"Type {type(obj)} is not serializable. Update method to implement "
            "serializer for object."
        )
    )


def get_kb_size(obj: Any) -> int:
    bytes_per_kb = 1000
    return int(sys.getsizeof(obj) / bytes_per_kb)


def is_list_of_primitives(lst: List[Any]):
    primitive = (int, str, bool)
    if lst and isinstance(lst, list):
        return all(isinstance(elem, primitive) for elem in lst)
    else:
        return False


def is_list_of_dicts_with_key(lst: List[Any], key: str):
    if lst and isinstance(lst, list):
        return all((isinstance(elem, dict) and key in elem) for elem in lst)
    else:
        return False


def stringify_int_val_for_key(d: Dict[str, Any], target_key: str):
    if target_key in d:
        val = d.get(target_key)
        d[target_key] = str(val) if isinstance(val, int) else val
        return d
    else:
        return d


def all_are_none(*args) -> bool:
    return all(arg is None for arg in args)


def all_are_not_none(*args) -> bool:
    return all(arg is not None for arg in args)


def only_one_arg_set(*args) -> bool:
    return sum(arg is not None for arg in args) == 1
