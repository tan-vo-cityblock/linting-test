from typing import Iterator, List, Tuple

from cf.define.types import MatchCondition
from cf.define.vocab import match
from cf.gcs.matchers.catalogs import prefix
from cityblock.load_to_silver.matcher import LoadToSilverPatternMatcher
from cityblock.load_to_silver.pattern import LoadToSilverPattern, compile

from .dag_trigger_effect import PartnerDagTriggerEffect

PartnerDagTriggerConditionSpec = Tuple[str, List[str], str]


class PartnerConditionSetCatalog:
    def partner(
        self, slug: str, specs: List[PartnerDagTriggerConditionSpec]
    ) -> Iterator[MatchCondition]:
        for spec in specs:
            dag_id, filename_str_patterns, ext = spec

            filename_patterns = list(map(compile, filename_str_patterns))
            for filename_pattern in filename_patterns:
                matcher = LoadToSilverPatternMatcher(f"{slug}/drop", filename_pattern)
                effect = PartnerDagTriggerEffect(
                    partner_slug=slug,
                    dag_id=dag_id,
                    precheck_patterns=filename_patterns,
                    ext=ext,
                )
                yield (match, matcher, effect)

    def healthyblue(
        self, *specs: PartnerDagTriggerConditionSpec
    ) -> Iterator[MatchCondition]:
        yield from self.partner("healthyblue", specs)


partner = PartnerConditionSetCatalog()
