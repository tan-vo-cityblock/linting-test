from cf.define.types import GCFEffect, GCFMatcher
from cf.gcs.events import GCSBlobPath


class GCSBlobPathEffect(GCFEffect[GCSBlobPath]):
    def effect(self, matcher: GCFMatcher[GCSBlobPath], blob_path: GCSBlobPath) -> None:
        raise NotImplementedError
