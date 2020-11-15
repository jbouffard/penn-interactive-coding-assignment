import pytest
from datetime import datetime

from nhldata.app import NHLApi


@pytest.fixture()
def start_date():
    return datetime.strptime("2020-08-04", "%Y-%m-%d")

@pytest.fixture()
def end_date():
    return datetime.strptime("2020-08-05", "%Y-%m-%d")


def test_successful_get_schedule(start_date, end_date):
    api = NHLApi()
    result = api.schedule(start_date, end_date)

    expected_element_count = 7
    actual_element_count = len(result)

    assert(expected_element_count == actual_element_count)

def test_failed_get_schedule(start_date, end_date):
    bad_url = "https://www.badurl/some/bad/data/"

    api = NHLApi(bad_url)

    result = api.schedule(start_date, end_date)

    assert result == {}

def test_successful_get_boxscore():
    api = NHLApi()
    game_id = 2019030042

    result = api.boxscore(game_id)

    expected_element_count = 3
    actual_element_count = len(result)

    assert(expected_element_count == actual_element_count)

def test_bad_failed_boxscore():
    bad_url = "https://www.badurl/some/bad/data/"
    bad_id = 12345

    api = NHLApi(bad_url)

    result = api.boxscore(bad_id)

    assert result == {}
