-- The top 25 popular ‘tracks’ released between 2020 and 2023.
    SELECT
        track_name
    FROM
        spotify.spotify_master
    WHERE
        EXTRACT(YEAR FROM release_date) BETWEEN 2020 AND 2023
    ORDER BY track_popularity DESC
    LIMIT 25;