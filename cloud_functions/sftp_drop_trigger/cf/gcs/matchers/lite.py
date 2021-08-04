from typing import Callable, Union

from cf.define.types import LiteGCFMatcher
from cf.gcs.events import GCSBlobPath


class LiteGCSBlobPathMatcher(LiteGCFMatcher[GCSBlobPath]):
    pass


def blob_path_matcher(
    matches: Callable[[GCSBlobPath], bool],
    explain: Union[str, Callable[[GCSBlobPath], str]],
):
    """syntatic sugar to be able to use one-off functions as matchers

    blob_path_matcher(
        lambda x: x.filename.startswith("here"),
        "starts with here"
    ).match(
        GCSBlobPath("something/else/here.txt")
    ) # True

    Args:
        matches (Callable[[GCSBlobPath], bool]): [description]
        explain (str): [description]

    Returns:
        [type]: [description]
    """
    explain_fn = (lambda e: explain) if isinstance(explain, str) else explain
    return LiteGCSBlobPathMatcher(matches, explain_fn)
