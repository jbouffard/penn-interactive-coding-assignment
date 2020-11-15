select
points.team_name,
full_name,
points.points
from {{ ref('nhl_players') }} as players, {{ ref('points') }}
where players.team_name = points.team_name and players.points = points.points and points.points >= 1
