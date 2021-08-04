from cf.gcs.types import GCSBlobPathMatcher


class GCSBlobPathFilenameExtMatcher(GCSBlobPathMatcher):
    def __init__(self, ext: str) -> None:
        super().__init__()
        self.ext = ext

    def matches(self, blob_path: GCSBlobPathMatcher) -> bool:
        return blob_path.filename.endswith(self.ext)

    def explain(self, blob_path: GCSBlobPathMatcher) -> str:
        return f"filename has extension {self.ext}"
