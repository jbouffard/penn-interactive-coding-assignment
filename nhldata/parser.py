from nhldata.strategy import NhlParsingStrategy


class NhlParser():
    def __init__(self, parsing_strategy: NhlParsingStrategy, output_cols: list):
        self.output_cols = output_cols

        self._parse_game_id = parsing_strategy.parse_game_id
        self._parse_player_info = parsing_strategy.parse_player_info

    def parse_game_id(self, dates: list) -> list:
        return self._parse_game_id(dates)

    def parse_player_info(self, boxscore: dict, output_cols: list) -> list:
        return self._parse_player_info(boxscore, output_cols)
