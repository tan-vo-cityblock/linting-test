from cf.gcs.matchers.prefix import GCSBlobPathFullpathPrefixMatcher
from cf.gcs.matchers.catalogs import GCSBlobPathPrefixMatcherCatalog
import logging
from typing import Union

from cf.gcs.events import GCSBlobPath
from cf.gcs.types import GCSBlobPathMatcher
from cityblock.load_to_silver.pattern import LoadToSilverPattern, compile


class LoadToSilverPatternMatcher(GCSBlobPathMatcher):
    partner_prefix_matcher: GCSBlobPathFullpathPrefixMatcher
    pattern: LoadToSilverPattern

    def __init__(
        self, partner_prefix: str, pattern: Union[LoadToSilverPattern, str]
    ) -> None:
        self.pattern = compile(pattern) if isinstance(pattern, str) else pattern
        self.partner_prefix_matcher = GCSBlobPathFullpathPrefixMatcher(partner_prefix)
        if "%s" not in self.pattern.pattern:
            logging.warn(
                f"""
                created LoadToSilverPatternMatcher with pattern {pattern}, 
                which does not have %s placeholder for date string. 
                If you don't need to handle variable datestrings,
                you probably don't need LoadToSilverPatternMatcher.
                """
            )

    def matches(self, blob_path: GCSBlobPath) -> bool:
        return self.partner_prefix_matcher.matches(blob_path) and self.pattern.match(
            blob_path.filename_noext
        )

    def explain(self, blob_path: GCSBlobPath) -> str:
        return f"fullpath starts with {self.partner_prefix_matcher.fullpath_prefix} and filename_noext matches pattern: {self.pattern.pattern}"
