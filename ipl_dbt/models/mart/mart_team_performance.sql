-- Final mart model: team performance summary
SELECT
    team_name,
    total_wins,
    win_percentage,
    RANK() OVER (ORDER BY total_wins DESC) AS performance_rank,
    CASE
        WHEN win_percentage >= 15 THEN 'Elite'
        WHEN win_percentage >= 10 THEN 'Strong'
        WHEN win_percentage >= 5  THEN 'Average'
        ELSE 'Developing'
    END AS performance_tier
FROM {{ ref('stg_team_wins') }}
ORDER BY total_wins DESC
