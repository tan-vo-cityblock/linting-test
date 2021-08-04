from dags.healthyblue.claims import ClaimsDAG
import unittest


class TestHealthyBlue(unittest.TestCase):
    def test_claims_dag(self):

        claims_dag = ClaimsDAG(
            dag_id="fake_claims_dag",
            silver_configs=["somepartner/professional.txt", "somepartner/facility.txt"],
            silver_views=["professional", "facility"],
            gold_transformer="ClaimsTransformer",
            gold_table="gold_claims",
            dbt_run_cmd="echo nice",
        )

        def has_task(cond):
            return any(
                cond(task_id, task) for task_id, task in claims_dag.task_dict.items()
            )

        def in_downstream(downstream_task_id, task):
            if not hasattr(task, "downstream_task"):
                print(task)
            return any(t.task_id == downstream_task_id for t in task.downstream_list)

        def in_upstream(upstream_task_id, task):
            return any(t.task_id == upstream_task_id for t in task.upstream_list)

        assert has_task(lambda tid, t: hasattr(t, "secrets") and len(t.secrets) == 1)

        assert has_task(
            lambda tid, t: t.task_id == "load_to_silver"
            and in_downstream("silver_to_gold", t)
        )
        assert has_task(
            lambda tid, t: t.task_id == "silver_to_gold"
            and in_upstream("load_to_silver", t)
        )


if __name__ == "__main__":
    unittest.main()
