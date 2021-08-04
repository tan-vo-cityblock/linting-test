"""End-to-end test for response required predictions."""

import argparse
import pandas as pd
import pandas_gbq
import pytest

import prod_sms_response_required as uut


@pytest.fixture
def args():
    return argparse.Namespace(
    	prob__label__1_thresh=0.7,
    	model_path="models/response_not_essential_2019-02-28_15-05-44.bin",
    	output_project_id="test-project-id",
    	output_tbl_name="test-table-name")

@pytest.fixture
def sms_messages():
    return pd.read_json("tests/fake_sms_messages.json")

@pytest.fixture
def expected_output():
	return pd.read_json("tests/fake_expected_output.json")


def test_main(args, sms_messages, expected_output, monkeypatch):

    def mock_query_db(*args, **kwargs):
        return sms_messages

    def mock_to_gbq(data, output_table, **kwargs):
        assert data.shape[0] == expected_output.shape[0], "Expect same number of rows in input and output data!"

    monkeypatch.setattr(uut, "query_db", mock_query_db)
    monkeypatch.setattr(pandas_gbq, "to_gbq", mock_to_gbq)

    uut.main(args.prob__label__1_thresh, args.model_path, args.output_project_id, args.output_tbl_name)
