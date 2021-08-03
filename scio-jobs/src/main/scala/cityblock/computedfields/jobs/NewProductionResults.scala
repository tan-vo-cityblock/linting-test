package cityblock.computedfields.jobs

import cityblock.computedfields.common.{ComputedFieldJob, SelectDbtResults}

// Send new results for all slugs marked as production
object NewProductionResults extends ComputedFieldJob with SelectDbtResults {}
