{%- docs substance_assessment_scores -%}

This computed field is made up of the following:

- [DAST Assessments](https://data-docs.cityblock.com/dbt#!/model/model.cityblockdbt.cf_dast_assessment)
  - eligible score >= 3 (moderate level)
- [AUDIT Assessments](https://data-docs.cityblock.com/dbt#!/model/model.cityblockdbt.cf_audit_assessment)
  - eligible score >= 10 (harmful and severe categories)


Last updated: 2021-06-07

{%- enddocs -%}
