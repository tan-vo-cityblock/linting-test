import unittest

from cf.gcs.events import GCSBlobPath
from cityblock.load_to_silver.matcher import LoadToSilverPatternMatcher


class TestLoadToSilverMatcher(unittest.TestCase):
    def test_matcher(self):
        matcher = LoadToSilverPatternMatcher("something/else", "hello_%s")
        assert matcher.matches(GCSBlobPath("something/else/hello_20210101.txt"))
        assert not matcher.matches(GCSBlobPath("something/else/ok_20210101.txt"))
