""" 
Objects for shorthand access to collections of typed matchers on
GCF events.
"""
from cf.gcs.types import GCSBlobPathMatcher

from .prefix import GCSBlobPathFilenamePrefixMatcher, GCSBlobPathFullpathPrefixMatcher


class GCSBlobPathPrefixMatcherCatalog:
    @staticmethod
    def filename(filename_prefix: str) -> GCSBlobPathMatcher:
        return GCSBlobPathFilenamePrefixMatcher(filename_prefix)

    @staticmethod
    def fullpath(fullpath_prefix: str) -> GCSBlobPathMatcher:
        return GCSBlobPathFullpathPrefixMatcher(fullpath_prefix)


prefix = GCSBlobPathPrefixMatcherCatalog()
