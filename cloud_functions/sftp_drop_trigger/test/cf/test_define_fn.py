import unittest
from typing import Dict

from mock import Mock

from cf.define import *
from cf.define.fn import GCPCloudFunction
from cf.define.types import GCFEffect, GCFEvent, GCFMatcher

s = "good_input"


class SomeGCFEvent(GCFEvent):
    def __init__(self, _s) -> None:
        self._s = _s

    @property
    def log_name(self):
        return self._s


class SomeGCF(GCPCloudFunction[SomeGCFEvent]):
    def to_event(self, data: Dict) -> SomeGCFEvent:
        return SomeGCFEvent(data["s"])


class TestGCF(unittest.TestCase):
    def fake_matcher(self):
        class SomeMatcher(GCFMatcher[SomeGCFEvent]):
            def matches(self, e: SomeGCFEvent) -> bool:
                assert isinstance(e, SomeGCFEvent)
                assert e._s is s
                return e._s.startswith("good")

            def explain(self, e: SomeGCFEvent) -> str:
                assert isinstance(e, SomeGCFEvent)
                return "starts good"

        matcher = SomeMatcher()
        matcher.matches = Mock(wraps=matcher.matches)
        matcher.explain = Mock(wraps=matcher.explain)
        return matcher

    def fake_effect(self):
        class SomeSideEffect(GCFEffect[SomeGCFEvent]):
            def effect(self, m: GCFMatcher, e: SomeGCFEvent) -> None:
                pass

        effect = SomeSideEffect()
        effect.effect = Mock(wraps=effect.effect)
        return effect

    def test_simple_match(self):

        matcher = self.fake_matcher()
        effect = self.fake_effect()
        fn = SomeGCF((match, matcher, effect))
        fn({"s": s})
        matcher.matches.assert_called_once()
        effect.effect.assert_called_once()

    def test_ignore(self):
        matcher = self.fake_matcher()
        effect = self.fake_effect()

        SomeGCF((ignore, matcher), (match, matcher, effect))({"s": s})

        # once because should exit before hitting second condition matcher
        matcher.matches.assert_called_once()
        # matcher condition effect should not happen
        effect.effect.assert_not_called()

    def test_multieffect(self):
        matcher = self.fake_matcher()
        effect_a = self.fake_effect()
        effect_b = self.fake_effect()

        SomeGCF((match, matcher, effect_a), (match, matcher, effect_b),)({"s": s})

        assert matcher.matches.call_count == 2
        effect_a.effect.assert_called_once()
        effect_b.effect.assert_called_once()
