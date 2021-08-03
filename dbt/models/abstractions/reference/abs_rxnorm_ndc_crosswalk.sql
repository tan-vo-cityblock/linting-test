{{
  config(
    materialized='table',
    udf = '''

CREATE TEMPORARY FUNCTION toNDC11(ndc STRING)
RETURNS STRING
LANGUAGE js AS """
  if (ndc == null){
    return null;
  }

  if (!ndc.includes("-") && ndc.length == 12) {
    return ndc.substring(1, 13);
  }

  ndcArray = ndc.split("-");
  if(ndcArray.length != 3){
    return ndc;
  }

  ndcArray[0] = ndcArray[0].padStart(5, "0");
  ndcArray[1] = ndcArray[1].padStart(4, "0");
  ndcArray[2] = ndcArray[2].padStart(2, "0");

  return ndcArray.join("");
""";

CREATE TEMPORARY FUNCTION toTrimmedTitleCase(str STRING)
RETURNS STRING
LANGUAGE js AS """
  if (str == null){
    return null;
  }

// DBT reduces "\\"" to "\" in regex so require 2x

return str.trim().replace(/^\\\\.\\\\s+/, "").replace(
    /\\\\w\\\\S*/g,
    function(txt) {
        return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
    });
""";

  '''
  )
}}


---- rxnorm input tables --------------------------------
-- RXNCONSO.RRF: Concepts, Concept Names, and their sources
-- RXNSAT.RRF, RXNSTY.RRF: Attributes
-- RXNREL.RRF: Relationships
-- RXNDOC.RRF, RXNSAB.RRF, RXNCUI.RRF: Data about RxNorm
-- RXNATOMARCHIVE.RRF: Archive Data
-- RXNCUICHANGES.RRF: Concept Changes Tracking Data
----------------------------------------------------------


-- For information on the RxNorm term types (TTY) see https://www.nlm.nih.gov/research/umls/rxnorm/docs/appendix5.html

---- output schema ----------------------------------------------------------------------
-- ndc	                              Standard 11-digit NDC code
-- sxdPckRxcui	                      RxCui of the overall SCD/SBD/GPCK/BPCK entity 
-- sxdPckStr	                        Descriptive string of sxdPckRxcui (e.g. Amifampridine 10 Mg Oral Tablet [Firdapse])
-- sxdPckTty	                        RxNorm term type of the RxCui (one of {SBD, SBD, GPCK, BPCK})
-- doseForm                           The dose form of the drug (e.g. Oral Tablet)
-- brandName	                        Brand name of drug (i.e. if SBD/BPCK). sxdPckStr will contain this in square brackets, potentially more than once: "|" separated if multiple distinct exist
-- components	                        Array to handle multiple components contained in both combo drugs and packs
-- components.scdcStr	                Descriptive string of SCDC RxCui
-- components.scdcRxcui	              SCDC RxCui (see link above)
-- components.sxdRxcui	              RxCui of components. I chose to *mirror these identically* across all components if the drug is a combo drug. If the drug is a pack these will vary
-- components.strength	              Dose strength of the active ingredient of the component drug
-- components.activeIngredient        Active ingredient of component ("IN" term type)
-- components.activeIngredientRxcui	  Active ingredient RxCui ("IN" term type)
-----------------------------------------------------------------------------------------


---- CUIs and the fields that are derived from them (see joins in the componentLevelTerms CTE)
-- sxdPckRxcui -> doseForm, brandName (SBD)
-- scdcRxcui -> strength, activeIngredient
-- components.sxdRxcui -> brandName (BPCK)


WITH rxnconso as (
  select * from {{ source('rxnorm', 'rxnconso_current') }} -- `bigquery-public-data.nlm_rxnorm.rxnconso_current`
),
rxnconsoSab as (
  select * from rxnconso where sab='RXNORM'
),
rxnrel as (
  select * from {{ source('rxnorm', 'rxnrel_current') }} -- `bigquery-public-data.nlm_rxnorm.rxnrel_current`
),
rxnsat as (
  select * from {{ source('rxnorm', 'rxnsat_current') }} -- `bigquery-public-data.nlm_rxnorm.rxnsat_current`
),
---- End of Rxnorm tables ----

-- SXD throughout stands for SBD *or* SCD
scdcSxd AS (
  select
      con1.STR scdcStr,
      rel.RXCUI2 scdcRxcui,
      rel.RXCUI1 sxdRxcui,
      con2.TTY sxdTty,
      con2.STR sxdRxStr,
      rel.RXCUI1 as pckComponentRxcui
  from rxnconsoSab con1
  inner join rxnrel rel on con1.RXCUI=rel.RXCUI2
  inner join rxnconsoSab con2 on rel.RXCUI1=con2.RXCUI
  and con1.TTY='SCDC' and con1.SUPPRESS='N' and con2.SUPPRESS='N' and rel.RELA='constitutes' and con2.TTY IN ('SBD','SCD')
),

-- Find packs containing drugs in previous step and associate with scdc rxcui
-- Note combo tablets will have a SINGLE pckRxcui and multiple SCD/SBD/SCDC RXCUIS e.g.  00074328813
scdcPck AS (
  select
      scdcStr,
      scdc.scdcRxcui,
      rel.RXCUI1 pckRxcui, -- RXCUI of the pack and NOT constituent components
      con.TTY sxdPckTty,
      con.STR sxdRxStr,
      sxdRxcui as pckComponentRxcui  -- RXCUIs of the components
  from scdcSxd scdc
  inner join rxnrel rel on scdc.sxdRxcui=rel.RXCUI2 and rel.RELA='contained_in'
  inner join rxnconsoSab con on con.RXCUI=rel.RXCUI1 and con.TTY IN ('BPCK','GPCK') and con.SUPPRESS='N'
),

-- Combine drugs and packs. Now sxdRxcui holds the rxcui for SBD/SCD drugs and the overall pack for BPCK/GPCK
rxcuiMain AS (
  -- Rename certain fields that now hold sxd and pck values
  select scdcStr, scdcRxcui, sxdRxcui as sxdPckRxcui, sxdTty as sxdPckTty, sxdRxStr, pckComponentRxcui from scdcSxd
  UNION DISTINCT
  select * from scdcPck
),

-- Fetch NDCs that haven't been suppressed
-- NDCs that are labeled obsolete by their original sources are marked as SUPPRESS="O".
-- All obsolete NDCs are NOT normalized or propagated to SAB='RXNORM' atoms
ndcRxcuiMain AS (
  select
    toNDC11(sat1.ATV) ndc,
    sat1.SUPPRESS ndcSuppress,
    scdc.*
  from rxcuiMain scdc
  inner join rxnsat sat1 on sat1.RXCUI=scdc.sxdPckRxcui
  and sat1.ATN='NDC' and sat1.SUPPRESS<>'Y' and sat1.sab IN ('RXNORM', 'MTHSPL')
  -- Omit VANDF. NDC 00069420030 listed as SBD in VANDF, SCD elsewhere
),

componentLevelTerms as (
-- Must select distinct without ndcSuppress to remove suppress flags that cause fanout
  select distinct
    ndcRxcuiMain.* except(ndcSuppress),
    con1.STR doseForm,
    sat1.atv strength,
    con3.STR ingrFromRelStr,
    rel4.RXCUI1 ingrFromRelRxcui,
    -- Ensure we only ever select one since brand name doesn't cause NDC fanout in next CTE
    string_agg(distinct con2.STR, ' | ' order by con2.STR) as brandFromRelStr,
    string_agg(distinct con5.STR, ' | ' order by con5.STR) as brandPckFromRelStr
--     array_agg(distinct con5.STR order by con5.STR)[SAFE_OFFSET(0)] brandPckFromRelStr --brandName for packs.
  from ndcRxcuiMain

  -- doseForm
  left join rxnrel rel1 on rel1.RXCUI2=ndcRxcuiMain.sxdPckRxcui and rel1.RELA='has_dose_form'
  left join rxnconsoSab con1 on con1.RXCUI=rel1.RXCUI1 and con1.tty = 'DF' and con1.sab='RXNORM'

  -- strength
  left join rxnsat sat1 on scdcRxcui=sat1.rxcui and sat1.atn = 'RXN_STRENGTH'

  -- activeIngredient
  left join rxnrel rel4 on rel4.RXCUI2=ndcRxcuiMain.scdcRxcui and rel4.RELA='has_ingredient'
  left join rxnconsoSab con3 on con3.RXCUI=rel4.RXCUI1 and con3.tty IN ('IN') and con3.sab='RXNORM'

  -- brandName non-packs (Could use pckComponentRxcui instead of sxdPckRxcui (as for packs below) since identical by previous assigment
  left join rxnrel rel2 on rel2.RXCUI2=ndcRxcuiMain.sxdPckRxcui and rel2.RELA='has_ingredient' and sxdPckTty IN ('SBD') -- brand drugs only.
  left join rxnconsoSab con2 on con2.RXCUI=rel2.RXCUI1 and con2.tty IN ('BN') and con2.sab='RXNORM'

  -- brandName packs
  left join rxnrel rel3 on rel3.RXCUI2=ndcRxcuiMain.pckComponentRxcui and rel3.RELA='has_ingredient' and sxdPckTty IN ('BPCK') -- brand pack drugs only
  left join rxnconsoSab con5 on con5.RXCUI=rel3.RXCUI1 and con5.tty IN ('BN') and con5.sab='RXNORM'

  group by 1,2,3,4,5,6,7,8,9,10,11
),

componentLevelTermsAgg as (
  select
    NDC as ndc,
    sxdPckRxcui,
    toTrimmedTitleCase(sxdRxStr) sxdPckStr,
    sxdPckTty,
    doseForm,
    string_agg(distinct coalesce(brandFromRelStr, brandPckFromRelStr), ' | '
                order by coalesce(brandFromRelStr, brandPckFromRelStr)) as brandName, -- in case branded packs contain multiple brands
    array_agg(struct(
        lower(scdcStr) as scdcStr,
        scdcRxcui as scdcRxcui,
        pckComponentRxcui as sxdRxcui,
        strength,
        lower(ingrFromRelStr) as activeIngredient,
        ingrFromRelRxcui as activeIngredientRxcui
        ) order by scdcRxcui ) as components
  from componentLevelTerms bdt
  group by 1,2,3,4,5
  order by 1 desc
),

vandfAdditions as (
  -- Adds consumables (e.g. FREESTYLE LITE (GLUCOSE) TEST STRIP), but only if NDCs aren't already captured
  select
    toNDC11(atv) as ndc,
    con1.rxcui,
    toTrimmedTitleCase(con1.str) as sxdPckStr,
    con2.str as brandName
  from rxnconso con1
  inner join rxnsat on con1.rxaui = rxnsat.rxaui and rxnsat.atn = 'NDC'
  left join componentLevelTermsAgg bdta  on bdta.ndc = toNDC11(atv)
  left join rxnrel rel on rel.rxcui2=con1.rxcui and rel.RELA='has_ingredient'
  left join rxnconso con2 on rel.rxcui1=con2.rxcui and con2.tty='BN'
  where con1.tty = 'CD' and con1.sab='VANDF'
  and bdta.ndc is null -- only add NDCs not already captured!
  group by 1,2,3,4
),

componentLevelTermsAggWithVandf as (
  select * from componentLevelTermsAgg
  union all
  select ndc, rxcui, sxdPckStr, brandName, STRING(NULL), STRING(NULL), [] as components from vandfAdditions
),

ndcDuplicates as (
  select ndc, count(*)
  from componentLevelTermsAggWithVandf
  group by 1
  having count(*) > 1
)

select bdt.*
from componentLevelTermsAggWithVandf bdt
left join ndcDuplicates
on bdt.ndc = ndcDuplicates.ndc
where ndcDuplicates.ndc is null
