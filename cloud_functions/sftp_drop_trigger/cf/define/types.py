from abc import ABC, abstractmethod, abstractproperty
from typing import Callable, Generic, Tuple, TypeVar

from .vocab import ignore, match, require


class GCFEvent(ABC):
    @abstractproperty
    def log_name(self):
        raise NotImplementedError


E = TypeVar("E", bound=GCFEvent)


class GCFMatcher(ABC, Generic[E]):
    @abstractmethod
    def matches(self, e: E) -> bool:
        raise NotImplementedError

    @abstractmethod
    def explain(self, e: E) -> str:
        raise NotImplementedError


class AddableGCFMatcher:
    def __add__(self, other):
        return LiteGCFMatcher(
            lambda e: self.matches(e) and other.matches(e),
            lambda e: f"{self.explain(e)} and {other.explain(e)}",
        )


class LiteGCFMatcher(Generic[E], GCFMatcher[E], AddableGCFMatcher):
    def __init__(
        self, matches: Callable[[E], bool], explain: Callable[[E], str]
    ) -> None:
        self.matches_impl = matches
        self.explain_impl = explain

    def matches(self, e: E) -> bool:
        return self.matches_impl(e)

    def explain(self, e: E) -> str:
        return self.explain_impl(e)


class GCFEffect(ABC, Generic[E]):
    @abstractmethod
    def effect(self, m: GCFMatcher[E], e: E) -> None:
        raise NotImplementedError

    def __call__(self, m: GCFMatcher[E], e: E) -> None:
        return self.effect(m, e)


IgnoreCondition = Tuple[ignore, GCFMatcher]
RequireCondition = Tuple[require, GCFMatcher]
MatchCondition = Tuple[match, GCFMatcher, GCFEffect]
