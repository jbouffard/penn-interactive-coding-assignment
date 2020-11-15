import pytest
import pandas as pd
from nhldata.parser import NhlParser
from nhldata.schema import NHL_SCHEMA
from nhldata.strategy import _parse_personal_info, _parse_goalie_stats, _parse_skater_stats, NHL_PARSING_STRATEGY


version = 'v1'

parser = NhlParser(NHL_PARSING_STRATEGY[version], NHL_SCHEMA[version])

output_cols = NHL_SCHEMA['v1']

actual_player_cols = output_cols[:28]

expected_player_cols = [
    'player_person_id',
    'player_jerseyNumber',
    'player_person_active',
    'player_person_alternateCaptain',
    'player_person_birthCity',
    'player_person_birthCountry',
    'player_person_birthDate',
    'player_person_birthStateProvince',
    'player_person_captain',
    'player_person_currentAge',
    'player_person_currentTeam_id',
    'player_person_currentTeam_link',
    'player_person_currentTeam_name',
    'player_person_firstName',
    'player_person_fullName',
    'player_person_height',
    'player_person_lastName',
    'player_person_link',
    'player_person_nationality',
    'player_person_primaryNumber',
    'player_person_primaryPosition_abbreviation',
    'player_person_primaryPosition_code',
    'player_person_primaryPosition_name',
    'player_person_primaryPosition_type',
    'player_person_rookie',
    'player_person_rosterStatus',
    'player_person_shootsCatches',
    'player_person_weight'
]

actual_goalie_cols = output_cols[28:45]

expected_goalie_cols = [
    'player_stats_goalieStats_assists',
    'player_stats_goalieStats_decision',
    'player_stats_goalieStats_evenSaves',
    'player_stats_goalieStats_evenShotsAgainst',
    'player_stats_goalieStats_evenStrengthSavePercentage',
    'player_stats_goalieStats_goals',
    'player_stats_goalieStats_pim',
    'player_stats_goalieStats_powerPlaySavePercentage',
    'player_stats_goalieStats_powerPlaySaves',
    'player_stats_goalieStats_powerPlayShotsAgainst',
    'player_stats_goalieStats_savePercentage',
    'player_stats_goalieStats_saves',
    'player_stats_goalieStats_shortHandedSavePercentage',
    'player_stats_goalieStats_shortHandedSaves',
    'player_stats_goalieStats_shortHandedShotsAgainst',
    'player_stats_goalieStats_shots',
    'player_stats_goalieStats_timeOnIce'
]

actual_skater_cols = output_cols[45:]

expected_skater_cols = [
    'player_stats_skaterStats_assists',
    'player_stats_skaterStats_blocked',
    'player_stats_skaterStats_evenTimeOnIce',
    'player_stats_skaterStats_faceOffPct',
    'player_stats_skaterStats_faceOffWins',
    'player_stats_skaterStats_faceoffTaken',
    'player_stats_skaterStats_giveaways',
    'player_stats_skaterStats_goals',
    'player_stats_skaterStats_hits',
    'player_stats_skaterStats_penaltyMinutes',
    'player_stats_skaterStats_plusMinus',
    'player_stats_skaterStats_powerPlayAssists',
    'player_stats_skaterStats_powerPlayGoals',
    'player_stats_skaterStats_powerPlayTimeOnIce',
    'player_stats_skaterStats_shortHandedAssists',
    'player_stats_skaterStats_shortHandedGoals',
    'player_stats_skaterStats_shortHandedTimeOnIce',
    'player_stats_skaterStats_shots',
    'player_stats_skaterStats_takeaways',
    'player_stats_skaterStats_timeOnIce',
    'side'
]


def test_parse_personal_info():
    raw_personal_info = {
        'id': 8475683,
        'fullName': 'Sergei Bobrovsky',
        'link': '/api/v1/people/8475683',
        'firstName': 'Sergei',
        'lastName': 'Bobrovsky',
        'primaryNumber': '72',
        'birthDate': '1988-09-20',
        'currentAge': 32,
        'birthCity': 'Novokuznetsk',
        'birthCountry': 'RUS',
        'nationality': 'RUS',
        'height': '6\' 2"',
        'weight': 187,
        'active': True,
        'alternateCaptain': False,
        'captain': False,
        'rookie': False,
        'shootsCatches': 'L',
        'rosterStatus': 'Y',
        'currentTeam': {'id': 13, 'name': 'Florida Panthers', 'link': '/api/v1/teams/13'},
        'primaryPosition': {'code': 'G', 'name': 'Goalie', 'type': 'Goalie', 'abbreviation': 'G'}
    }

    formatted_personal_info = [(
        raw_personal_info['id'],
        72,
        raw_personal_info['active'],
        raw_personal_info['alternateCaptain'],
        raw_personal_info['birthCity'],
        raw_personal_info['birthCountry'],
        raw_personal_info['birthDate'],
        None,
        raw_personal_info['captain'],
        raw_personal_info['currentAge'],
        raw_personal_info['currentTeam']['id'],
        raw_personal_info['currentTeam']['link'],
        raw_personal_info['currentTeam']['name'],
        raw_personal_info['firstName'],
        raw_personal_info['fullName'],
        raw_personal_info['height'],
        raw_personal_info['lastName'],
        raw_personal_info['link'],
        raw_personal_info['nationality'],
        raw_personal_info['primaryNumber'],
        raw_personal_info['primaryPosition']['abbreviation'],
        raw_personal_info['primaryPosition']['code'],
        raw_personal_info['primaryPosition']['name'],
        raw_personal_info['primaryPosition']['type'],
        raw_personal_info['rookie'],
        raw_personal_info['rosterStatus'],
        raw_personal_info['shootsCatches'],
        raw_personal_info['weight']
    )]

    expected_output = pd.DataFrame(formatted_personal_info, columns=expected_player_cols)

    actual_output = _parse_personal_info(raw_personal_info, 72, actual_player_cols)

    pd.testing.assert_frame_equal(expected_output, actual_output)

def test_parse_goalie_stats():
    raw_goalie_stats = {
        'timeOnIce': '57:29',
        'assists': 0,
        'goals': 0,
        'pim': 0,
        'shots': 34,
        'saves': 30,
        'powerPlaySaves': 3,
        'shortHandedSaves': 0,
        'evenSaves': 27,
        'shortHandedShotsAgainst': 0,
        'evenShotsAgainst': 29,
        'powerPlayShotsAgainst': 5,
        'decision': 'L',
        'savePercentage': 88.23529411764706,
        'powerPlaySavePercentage': 60.0,
        'evenStrengthSavePercentage': 93.10344827586206
    }

    formatted_goalie_stats = [(
        raw_goalie_stats['assists'],
        raw_goalie_stats['decision'],
        raw_goalie_stats['evenSaves'],
        raw_goalie_stats['evenShotsAgainst'],
        raw_goalie_stats['evenStrengthSavePercentage'],
        raw_goalie_stats['goals'],
        raw_goalie_stats['pim'],
        raw_goalie_stats.get('powerPlaySavePercentage'),
        raw_goalie_stats['powerPlaySaves'],
        raw_goalie_stats['powerPlayShotsAgainst'],
        raw_goalie_stats['savePercentage'],
        raw_goalie_stats['saves'],
        raw_goalie_stats.get('shortHandedSavePercentage'),
        raw_goalie_stats['shortHandedSaves'],
        raw_goalie_stats['shortHandedShotsAgainst'],
        raw_goalie_stats['shots'],
        raw_goalie_stats['timeOnIce']
    )]

    expected_output = pd.DataFrame(formatted_goalie_stats, columns=expected_goalie_cols)

    actual_output = _parse_goalie_stats(raw_goalie_stats, actual_goalie_cols)

    pd.testing.assert_frame_equal(expected_output, actual_output)

def test_parse_skater_stats():
    raw_skater_stats = {
        'timeOnIce': '18:19',
        'assists': 0,
        'goals': 0,
        'shots': 0,
        'hits': 3,
        'powerPlayGoals': 0,
        'powerPlayAssists': 0,
        'penaltyMinutes': 0,
        'faceOffPct': 62.5,
        'faceOffWins': 5,
        'faceoffTaken': 8,
        'takeaways': 0,
        'giveaways': 2,
        'shortHandedGoals': 0,
        'shortHandedAssists': 0,
        'blocked': 0,
        'plusMinus': 0,
        'evenTimeOnIce': '12:30',
        'powerPlayTimeOnIce': '0:00',
        'shortHandedTimeOnIce': '5:49'
    }

    formatted_skater_stats = [(
        raw_skater_stats['assists'],
        raw_skater_stats['blocked'],
        raw_skater_stats['evenTimeOnIce'],
        raw_skater_stats['faceOffPct'],
        raw_skater_stats['faceOffWins'],
        raw_skater_stats['faceoffTaken'],
        raw_skater_stats['giveaways'],
        raw_skater_stats['goals'],
        raw_skater_stats['hits'],
        raw_skater_stats['penaltyMinutes'],
        raw_skater_stats['plusMinus'],
        raw_skater_stats['powerPlayAssists'],
        raw_skater_stats['powerPlayGoals'],
        raw_skater_stats['powerPlayTimeOnIce'],
        raw_skater_stats['shortHandedAssists'],
        raw_skater_stats['shortHandedGoals'],
        raw_skater_stats['shortHandedTimeOnIce'],
        raw_skater_stats['shots'],
        raw_skater_stats['takeaways'],
        raw_skater_stats['timeOnIce'],
        None
    )]

    expected_output = pd.DataFrame(formatted_skater_stats, columns=expected_skater_cols)

    actual_output = _parse_skater_stats(raw_skater_stats, actual_skater_cols)

    pd.testing.assert_frame_equal(expected_output, actual_output)
