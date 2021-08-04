import json


def load_db_ref_json():
    with open('../db_mirror/util/db_ref.json') as f:
        db_reference = json.load(f)
    return db_reference


class TestDatabaseReference:
    def test_db_reference_structure(self):
        db_reference = load_db_ref_json()
        assert len(db_reference) == 1
        for sub_keys in db_reference.values():
            assert 'datasets_to_inspect' in sub_keys
            assert 'tables_to_skip' in sub_keys
