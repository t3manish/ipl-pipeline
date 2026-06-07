-- Staging model: clean and rename raw data
SELECT
    winner                              AS team_name,
    total_wins,
    ROUND(total_wins * 100.0 / SUM(total_wins) OVER (), 2) AS win_percentage
FROM {{ source('ipl_raw', 'team_wins') }}
WHERE winner IS NOT NULL
