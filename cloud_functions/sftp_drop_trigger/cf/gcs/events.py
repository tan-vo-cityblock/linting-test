from os import path


class GCSBlobPath:
    """helper class for working with blob paths from GCS events sent to GCF

    bp = GCSBlobPath("carefirst/drop/cat_claims_20393010.csv")
    bp.fullpath # "carefirst/drop/cat_claims_20393010.csv"
    bp.filename # "cat_claims_20393010.csv"
    bp.filename_noext # "cat_claims_20393010"
    bp.ext # "csv"
    """

    def __init__(self, fullpath: str) -> None:
        self.fullpath = fullpath

    @property
    def filename(self):
        return path.basename(self.fullpath)

    @property
    def ext(self):
        return "." + self.filename.split(".")[-1] if "." in self.filename else ""

    @property
    def filename_noext(self):
        return self.filename.split(".")[0]

    def __str__(self) -> str:
        return self.fullpath
