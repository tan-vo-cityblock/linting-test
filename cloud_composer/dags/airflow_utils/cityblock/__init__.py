# why airflow_utils and airflow_utils.cityblock.airflow?
# airflow_utils.py was a single module predating modularization efforts
# airflow_utils.cityblock.airflow is an interm step towards
# import cityblock.airflow
# import airflow_utils should work as usual


from . import airflow
from . import bq
