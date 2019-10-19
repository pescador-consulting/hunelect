create function gerry.getdistrictid(in_taz int, in_maz int, in_sorsz int)
  returns setof gerry.district
as
$$
SELECT *
FROM gerry.district d
WHERE d.taz = in_taz
	AND d.maz = in_maz
	AND d.sorsz = in_sorsz;
$$
language sql;