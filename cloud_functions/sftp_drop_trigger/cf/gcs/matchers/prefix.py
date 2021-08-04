from cf.gcs.effects import GCSBlobPathEffect
from cf.gcs.events import GCSBlobPath
from cf.gcs.types import GCSBlobPathMatcher


class GCSBlobPathFilenamePrefixMatcher(GCSBlobPathMatcher):
    def __init__(self, filename_prefix: str) -> None:
        super().__init__()
        self.filename_prefix = filename_prefix

    def matches(self, blob_path: GCSBlobPathEffect) -> bool:
        return blob_path.filename.startswith(self.filename_prefix)

    def explain(self, blob_path: GCSBlobPath) -> str:
        return f"filename starts with {self.filename_prefix}"


class GCSBlobPathFullpathPrefixMatcher(GCSBlobPathMatcher):
    def __init__(self, fullpath_prefix: str) -> None:
        super().__init__()
        self.fullpath_prefix = fullpath_prefix

    def matches(self, blob_path: GCSBlobPath) -> bool:
        return blob_path.fullpath.startswith(self.fullpath_prefix)

    def explain(self, blob_path: GCSBlobPath) -> str:
        return f"fullpath starts with {self.fullpath_prefix}"
