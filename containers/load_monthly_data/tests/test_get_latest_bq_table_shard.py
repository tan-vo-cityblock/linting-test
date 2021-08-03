import random

import pytest

from get_latest_bq_table_shard import LatestBQTableShard


class TestLatestBQTableShard:
    def test_get_prefix_from_table_name(self):
        assert LatestBQTableShard._get_prefix_from_table_name("Facility_20200813") == 'Facility'
        with pytest.raises(IndexError):
            LatestBQTableShard._get_prefix_from_table_name("Facility")
        with pytest.raises(IndexError):
            LatestBQTableShard._get_prefix_from_table_name("Facility_19990101")

    def test_get_shard_from_table_name(self):
        assert LatestBQTableShard._get_shard_from_table_name("Facility_20200813") == '20200813'
        assert LatestBQTableShard._get_shard_from_table_name(
            "connecticare-data.gold_claims.Facility_20200813") == '20200813'
        assert LatestBQTableShard._get_shard_from_table_name("Facility") is None
        assert LatestBQTableShard._get_shard_from_table_name(
            "connecticare-data.gold_claims.Facility") is None

    def test_determine_latest_shard(self):
        table_ids = [
            'Facility_20200913',
            'Professional_20200813',
            'Facility_20200813',
            'Provider_20200813',
            'Professional_20200713',
            'Facility_20200713',
            'Pharmacy_20200713',
            'Provider_20200713',
        ]
        # Shuffle ids but use the same random seed
        random.shuffle(table_ids, lambda: 0.5)

        assert LatestBQTableShard._determine_latest_shard(table_ids, ['Facility']) == '20200913'
        assert LatestBQTableShard._determine_latest_shard(table_ids, ['Professional', 'Facility',
                                                                      'Provider']) == '20200813'
        assert LatestBQTableShard._determine_latest_shard(table_ids, ['Professional', 'Facility',
                                                                      'Provider',
                                                                      'Pharmacy']) == '20200713'
        with pytest.raises(RuntimeError):
            assert LatestBQTableShard._determine_latest_shard(table_ids,
                                                              ['Professional', 'Facility',
                                                               'DoesNotExist'])
        with pytest.raises(RuntimeError):
            assert LatestBQTableShard._determine_latest_shard(table_ids, ['DoesNotExist'])
        with pytest.raises(RuntimeError):
            assert LatestBQTableShard._determine_latest_shard([], ['Professional', 'Facility',
                                                                   'Provider',
                                                                   'Pharmacy'])
