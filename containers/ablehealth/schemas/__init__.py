from . import measure_results_desc
from . import risk_scores_desc
from . import risk_suspects_desc
from typing import Dict, List, Tuple

from google.cloud.bigquery.schema import SchemaField


class BigQuerySchema:
    def __init__(self, as_shorthand: List[Tuple[str, str, str]]) -> None:
        self.as_shorthand = as_shorthand

    @property
    def as_schema_fields(self) -> List[SchemaField]:
        for x in self.as_shorthand:
            if len(x) != 3:
                print(x)
        return [
            SchemaField(field_name, field_type, field_mode)
            for field_name, field_type, field_mode in
            self.as_shorthand]

    @property
    def as_dicts(self) -> List[Dict]:
        return [
            {
                "mode": schema_field.mode,
                "name": schema_field.name,
                "type": schema_field.field_type
            } for schema_field in
            self.as_schema_fields
        ]


measure_results = BigQuerySchema(measure_results_desc.fields)
risk_scores = BigQuerySchema(risk_scores_desc.fields)
risk_suspects = BigQuerySchema(risk_suspects_desc.fields)
