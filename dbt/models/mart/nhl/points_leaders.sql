select 
* -- TODO replace with correct projection columns
from {{ ref('nhl_players') }}  -- or other tables
