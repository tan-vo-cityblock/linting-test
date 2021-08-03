# from dataclasses import dataclass

# TODO: does bq have a new shard definition type?


# @dataclass
class ShardDef:
    project: str
    dataset: str
    table: str
    date: str

    def __init__(self, project: str, dataset: str, table: str, date: str) -> None:
        self.project = project
        self.dataset = dataset
        self.table = table
        self.date = date

    def __str__(self) -> str:
        return self.project + ":" + ".".join([self.dataset, self.table, self.date,])
