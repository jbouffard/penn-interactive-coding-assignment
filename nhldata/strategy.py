import logging
from collections import namedtuple

import pandas as pd


NhlParsingStrategy = namedtuple('NhlParsingStrategy', 'parse_game_id, parse_player_info')


def parse_game_id(dates: list) -> list:
    ids = []

    for date in dates:
        for game in date['games']:
            ids.append(game['gamePk'])

    return ids

def _parse_personal_info(personal_info: dict, jersey_number: int, output_cols: list) -> pd.DataFrame:
    current_team_info = personal_info['currentTeam']
    current_pos_info = personal_info['primaryPosition']

    player_id = personal_info['id']
    name = personal_info['fullName']

    logging.info("Parsing player information for: {}, id: {}".format(name, player_id))

    reorganized_cols = [(
        player_id,
        jersey_number,
        personal_info['active'],
        personal_info['alternateCaptain'],
        personal_info['birthCity'],
        personal_info['birthCountry'],
        personal_info['birthDate'],
        personal_info.get('birthStateProvince'),
        personal_info['captain'],
        personal_info['currentAge'],
        current_team_info['id'],
        current_team_info['link'],
        current_team_info['name'],
        personal_info['firstName'],
        name,
        personal_info['height'],
        personal_info['lastName'],
        personal_info['link'],
        personal_info['nationality'],
        personal_info['primaryNumber'],
        current_pos_info['abbreviation'],
        current_pos_info['code'],
        current_pos_info['name'],
        current_pos_info['type'],
        personal_info['rookie'],
        personal_info['rosterStatus'],
        personal_info['shootsCatches'],
        personal_info['weight']
    )]

    return pd.DataFrame(reorganized_cols, columns=output_cols)

def _parse_goalie_stats(stats: dict, output_cols: list) -> pd.DataFrame:
    if stats:
        reorganized_cols = [(
            stats['assists'],
            stats['decision'],
            stats['evenSaves'],
            stats['evenShotsAgainst'],
            stats['evenStrengthSavePercentage'],
            stats['goals'],
            stats['pim'],
            stats.get('powerPlaySavePercentage'),
            stats['powerPlaySaves'],
            stats['powerPlayShotsAgainst'],
            stats['savePercentage'],
            stats['saves'],
            stats.get('shortHandedSavePercentage'),
            stats['shortHandedSaves'],
            stats['shortHandedShotsAgainst'],
            stats['shots'],
            stats['timeOnIce']
        )]
    else:
        reorganized_cols = []

    return pd.DataFrame(reorganized_cols, columns=output_cols)

def _parse_skater_stats(stats: dict, output_cols: list) -> pd.DataFrame:
    if stats:
        reorganized_cols = [(
            stats['assists'],
            stats['blocked'],
            stats['evenTimeOnIce'],
            stats.get('faceOffPct'),
            stats['faceOffWins'],
            stats['faceoffTaken'],
            stats['giveaways'],
            stats['goals'],
            stats['hits'],
            stats['penaltyMinutes'],
            stats['plusMinus'],
            stats['powerPlayAssists'],
            stats['powerPlayGoals'],
            stats['powerPlayTimeOnIce'],
            stats['shortHandedAssists'],
            stats['shortHandedGoals'],
            stats['shortHandedTimeOnIce'],
            stats['shots'],
            stats['takeaways'],
            stats['timeOnIce'],
            stats.get('side')
        )]
    else:
        reorganized_cols = []

    return pd.DataFrame(reorganized_cols, columns=output_cols)

def _parse_stats(stats: dict, goalie_cols: list, skater_cols: list) -> list:
    goalie_stats = stats.get('goalieStats')
    skater_stats = stats.get('skaterStats')

    goalie_df = _parse_goalie_stats(goalie_stats, goalie_cols)
    skater_df = _parse_skater_stats(skater_stats, skater_cols)

    return pd.concat([goalie_df, skater_df], ignore_index=True)


def parse_player_info(boxscore: dict, output_cols: list) -> list:
    players_info = []
    teams = boxscore['teams']
    player_cols = output_cols[:28]

    logging.info("Parsing Player Information")

    for target_team in ['home', 'away']:
        players = teams[target_team]['players']

        for player in players.values():
            personal_info = _parse_personal_info(player['person'], player['jerseyNumber'], player_cols)

            stats = _parse_stats(player['stats'], output_cols[28:45], output_cols[45:])

            player_data = pd.concat([personal_info, stats], ignore_index=True)

            player_df = pd.DataFrame(player_data, columns=output_cols)

            players_info.append(player_df)

    return players_info


NHL_PARSING_STRATEGY = {
    'v1': NhlParsingStrategy(parse_game_id, parse_player_info)
}
