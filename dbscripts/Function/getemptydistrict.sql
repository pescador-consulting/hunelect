CREATE FUNCTION gerry.getemptydistrictid(
	in_election_id integer)
    RETURNS SETOF gerry.district 
    LANGUAGE 'sql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$
SELECT *
FROM gerry.district d
WHERE d.election_id = in_election_id
    AND polygon is null
ORDER BY RANDOM()
LIMIT 1;
$BODY$;