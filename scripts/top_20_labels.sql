-- The top 20 ‘labels’ against their total number of ‘tracks’.
    SELECT
        label,
        COUNT(DISTINCT track_name) as total_tracks
    FROM
        spotify.spotify_master
    GROUP BY
        label
    ORDER BY
        total_tracks DESC
    LIMIT 20;