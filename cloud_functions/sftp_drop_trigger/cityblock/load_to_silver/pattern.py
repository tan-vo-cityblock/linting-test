import logging
import re
from textwrap import dedent
from typing import Optional


class LoadToSilverPattern:
    pattern: str

    def __init__(self, pattern: str) -> None:
        if "%s" not in pattern:
            logging.warn(
                dedent(
                    """
                    LoadToSilver filenamePatterns use %%s to denote date. 
                    (If your pattern doesn't even have date, don't use LoadToSilverPattern().
                    Use regular-ol string matching.
                    """
                )
            )
        self.pattern = pattern

    @property
    def as_regex_pattern(self):
        # as of 2021-07-09, LoadToSilver only expects dates in the form of YYYYMMDD
        date_pattern = "(\\d\\d\\d\\d\\d\\d\\d\\d)"
        return self.pattern.replace("%s", date_pattern)

    @property
    def as_regex(self):
        return re.compile(self.as_regex_pattern)

    def match(self, other: str) -> bool:
        """mimic the filenamePattern behavior of LoadToSilver
        from cityblock import load_to_silver
        load_to_silver
            .compile("NCMT_MEDENCCLMILN_BCBS_CB_%s")
            .match("NCMT_MEDENCCLMILN_BCBS_CB_20210709") # True
        """

        return self.as_regex.match(other) is not None

    def get_date(self, other: str) -> Optional[str]:
        """extract date from string using LoadToSilver filename pattern
        load_to_silver
            .compile("NCMT_MEDENCCLMILN_BCBS_CB_%s")
            .get_date("NCMT_MEDENCCLMILN_BCBS_CB_20210709") # "20210709"
        """
        search_result = self.as_regex.search(other)
        if search_result:
            return search_result.groups()[0]
        else:
            return None

    def with_date(self, datestr: str):
        return self.pattern.replace("%s", datestr)


def compile(pattern: str):
    return LoadToSilverPattern(pattern)
