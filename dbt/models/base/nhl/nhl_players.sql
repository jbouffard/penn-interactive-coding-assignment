select
nhl_player_id as id,
game_team_name team_name,
full_name,
coalesce(stats_assists, 0) + coalesce(stats_power_play_assists, 0) + coalesce(stats_shorthanded_assists, 0) + coalesce(goalie_stats_assists, 0) assists,
coalesce(stats_goals, 0) + coalesce(stats_power_play_goals, 0) + coalesce(stats_shorthanded_goals, 0) + coalesce(goalie_stats_goals, 0) goals,
coalesce(stats_assists, 0) + coalesce(stats_power_play_assists, 0) + coalesce(stats_shorthanded_assists, 0) + coalesce(goalie_stats_assists, 0) + coalesce(stats_goals, 0) + coalesce(stats_power_play_goals, 0) + coalesce(stats_shorthanded_goals, 0) + coalesce(goalie_stats_goals, 0) points,
stats_time_on_ice::interval + stats_event_time_on_ice::interval +  stats_shorthanded_time_on_ice::interval + goalie_stats_time_on_ice::interval time_on_ice
from {{ ref('player_game_stats') }}
