import unittest

from cf.gcs.events import GCSBlobPath
from cf.gcs.matchers.ext import GCSBlobPathFilenameExtMatcher
from cf.gcs.matchers.lite import blob_path_matcher
from cf.gcs.matchers.prefix import (
    GCSBlobPathFilenamePrefixMatcher,
    GCSBlobPathFullpathPrefixMatcher,
)


class TestMatchers(unittest.TestCase):
    def test_ext(self):
        assert GCSBlobPathFilenameExtMatcher("txt").matches(
            GCSBlobPath("ok/yes/good/something.txt")
        )
        assert not GCSBlobPathFilenameExtMatcher("txt").matches(
            GCSBlobPath("ok/yes/good/something.csv")
        )

    def test_filename(self):
        assert GCSBlobPathFilenamePrefixMatcher("something").matches(
            GCSBlobPath("some/yes/good/something_else.txt")
        )
        assert not GCSBlobPathFilenamePrefixMatcher("something").matches(
            GCSBlobPath("something/yes/good/nothing_else.txt")
        )

    def test_fullpath(self):
        assert GCSBlobPathFullpathPrefixMatcher("some").matches(
            GCSBlobPath("some/yes/good/something_else.txt")
        )
        assert not GCSBlobPathFilenamePrefixMatcher("something").matches(
            GCSBlobPath("nothing/yes/good/nothing_else.txt")
        )

    def test_lite(self):

        # raw explain strings are okay
        assert (
            blob_path_matcher(
                lambda bp: bp.filename.startswith("something"), "starts with something"
            ).explain(GCSBlobPath("a/b/c/ok.tx"))
            == "starts with something"
        )

        # fancy explains are okay
        assert (
            blob_path_matcher(
                lambda bp: bp.filename.startswith("something"),
                lambda bp: f"{bp.filename} is great",
            ).explain(GCSBlobPath("a/b/c/ok.txt"))
            == "ok.txt is great"
        )

        # can combine them
        matcher_a = blob_path_matcher(
            lambda bp: bp.filename.startswith("sometime"),
            lambda bp: f"{bp.filename} is in the right place",
        )
        matcher_b = blob_path_matcher(
            lambda bp: bp.fullpath.startswith("good_folder"),
            lambda bp: f"{bp.filename} is at the right time",
        )

        matcher_c = matcher_a + matcher_b

        assert matcher_c.matches(GCSBlobPath("good_folder/ok/sometime.txt"))
        assert (
            matcher_c.explain(GCSBlobPath("good_folder/ok/sometime.txt"))
            == "sometime.txt is in the right place and sometime.txt is at the right time"
        )
        assert not matcher_c.matches(GCSBlobPath("bad_folder/ok/sometime.txt"))

    def test_explain(self):
        assert (
            GCSBlobPathFullpathPrefixMatcher("some").explain(
                GCSBlobPath("some/yes/good/something_else.txt")
            )
            == f"fullpath starts with some"
        )
        assert (
            GCSBlobPathFilenamePrefixMatcher("something").explain(
                GCSBlobPath("nothing/yes/good/nothing_else.txt")
            )
            == f"filename starts with something"
        )

        assert (
            GCSBlobPathFilenameExtMatcher("txt").explain(
                GCSBlobPath("nothing/yes/good/nothing_else.txt")
            )
            == f"filename has extension txt"
        )


if __name__ == "__main__":
    unittest.main()
