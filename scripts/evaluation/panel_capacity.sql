
select 
  patientId,
  isPaused as isOnPausedPanel,
  numTends / numAttemptedTends as ratioTendsToAttempts,
  isPanelLargerThan30 as isOnPanelLargerThan30
from `cbh-katie-claiborne.abs_evaluation.eval_pc_panel_analytic`
where numAttemptedTends > 0
