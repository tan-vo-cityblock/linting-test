import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Optional, TypeVar, Union

from cf.gcs.events import GCSBlobPath

from .types import GCFEvent, GCFMatcher, IgnoreCondition, MatchCondition
from .vocab import *
import logging

E = TypeVar("E", bound=GCFEvent)


class GCPCloudFunction(ABC, Generic[E]):
    def __init__(self, *conditions: Union[IgnoreCondition, MatchCondition]) -> None:
        self.conditions = conditions

    def on_ignore(self, matcher: GCFMatcher, event: E):
        logging.info(f"Ignoring {event.log_name} because {matcher.explain(event)}")

    def on_require_fail(self, matcher: GCFMatcher, event: E):
        logging.info(
            f"Requirement failed for {event.log_name}: {matcher.explain(event)} "
        )

    @abstractmethod
    def to_event(self, data: Dict) -> E:
        raise NotImplementedError

    def __call__(self, data: Dict, context: Optional[Dict] = None) -> Any:
        logging.info(f"INVOCATION: data={str(data)}")
        event = self.to_event(data)

        for condition in self.conditions:
            logging.debug("EVALUTATE CONDITION " + str(condition))
            command = condition[0]
            matcher = condition[1]
            if command is ignore:
                if matcher.matches(event):
                    return self.on_ignore(matcher, event)
                else:
                    logging.debug(
                        "Not ignored because condition not met: "
                        + matcher.explain(event)
                    )
            elif command is require:
                if not matcher.matches(event):
                    return self.on_require_fail(matcher, event)
                else:
                    logging.debug("Requirement passed: " + matcher.explain(event))
            elif command is match:
                effect = condition[2]
                if matcher.matches(event):
                    logging.debug("MATCHING CONDITION " + str(condition))
                    effect(matcher, event)
                else:
                    logging.debug(
                        "MATCHER FAILED, condition not met: " + matcher.explain(event)
                    )
                    # no return. ipass through (multiple effects possible)
        logging.warning("FUNCTION END: no matching statements found")


class GCPCloudFunctionWithGCSTrigger(GCPCloudFunction[GCSBlobPath]):
    def to_event(self, data: Dict) -> E:
        return GCSBlobPath(data["name"])


class GCPCloudFunctionCatalog:
    @property
    def on_gcs_blob(self):
        return GCPCloudFunctionWithGCSTrigger
