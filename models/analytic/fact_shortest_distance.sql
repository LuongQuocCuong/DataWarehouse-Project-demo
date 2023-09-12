WITH RECURSIVE shortest_distance__source AS (
    SELECT 
        departure
        , destination
        , departure || ' -> ' || destination as route
        , distance
    FROM {{ref('shortest_distance')}}

    UNION ALL
    SELECT
        source.departure as departure
        , target.destination as destination
        , source.route || ' -> ' || target.destination as route
        , source.distance + target.distance as distance
    FROM {{ref('shortest_distance')}} as target
    INNER JOIN shortest_distance__source as source
    ON source.destination = target.departure
)

SELECT
    departure
    , destination
    , route
    , distance
FROM shortest_distance__source