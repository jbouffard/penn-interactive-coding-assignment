select team_name, max(points) as points
from {{ ref('nhl_players') }}
group by team_name
