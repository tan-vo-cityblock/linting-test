import re
import unittest

from cityblock import load_to_silver

"""
as 2021-07-19, load to silver's patterns are not very complex
they assume 8 digits in a row and favor earlier substrings
they don't support other time units such as hour and minutes
and the direction is to favor observable preprocessing (e.g.
of filenames) and wrap LoadToSilver as a component of more 
sophisticated tooling rather than extend the configuration 
capabilities of LoadToSilver itself.
"""


class TestLoadToSilverPattern(unittest.TestCase):
    def test_get_working_regex(self):
        lts = load_to_silver.pattern.compile("hello_%s")
        assert lts.as_regex_pattern == r"hello_(\d\d\d\d\d\d\d\d)"
        try:
            re.compile(lts.as_regex_pattern)
        except:
            raise AssertionError

    def test_can_match(self):
        lts = load_to_silver.pattern.compile("hello_%s")
        assert lts.as_regex.match("hello_20210101") is not None
        assert lts.as_regex.match("hello_21210101") is not None
        assert lts.as_regex.match("no_21210101") is None

    def test_can_get_date(self):
        lts = load_to_silver.pattern.compile("hello_%s")
        assert lts.get_date("hello_20200820") == "20200820"
        assert lts.get_date("hello_202008208080") == "20200820"
        assert lts.get_date("hello_2020") is None
        assert lts.get_date("no_20200101") is None

    def test_create_with_date(self):
        lts = load_to_silver.pattern.compile("hello_%s")
        assert lts.with_date("20201030") == "hello_20201030"

        # 2021-07-19 LoadToSilver doesn't care if it's an actual logical date
        # so load_to_silver.pattern shouldn't either
        assert lts.with_date("2020103038434") == "hello_2020103038434"
