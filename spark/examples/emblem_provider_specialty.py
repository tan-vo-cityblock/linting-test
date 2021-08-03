# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.5.2
#   kernelspec:
#     display_name: env-spark
#     language: python
#     name: env-spark
# ---

from cbh_setup import CityblockSparkContext

cbh = CityblockSparkContext(app_name='test')
session = cbh.session

import os
personal_project = os.environ['CBH_SPARK_PROJECT_ID']
print(personal_project)

professional = session.read.format('bigquery').option('table', 'emblem-data.gold_claims.Professional_20200813').load()


billing_provider_specialties = professional.select('header.provider.billing.specialty')

distinct_specialties = billing_provider_specialties.distinct()


distinct_specialties.write.format('bigquery').option('table', f'{personal_project}.spark_test.null').save()


