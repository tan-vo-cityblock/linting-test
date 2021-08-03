from cf.define.types import AddableGCFMatcher, GCFMatcher
from cf.gcs.events import GCSBlobPath


class GCSBlobPathMatcher(GCFMatcher[GCSBlobPath], AddableGCFMatcher):
    def matches(self, blob_path: GCSBlobPath) -> bool:
        raise NotImplementedError

    def explain(self, blob_path: GCSBlobPath) -> str:
        raise NotImplementedError
