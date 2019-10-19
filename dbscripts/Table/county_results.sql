CREATE TABLE gerry.county_results
(
  id serial NOT NULL,
	district_id int REFERENCES gerry.district(id),
	election_id int REFERENCES gerry.election(id) ON DELETE CASCADE,
  pname varchar(8000) null,
  orderrank int null,
  results int null,
  CONSTRAINT county_results_pkey PRIMARY KEY (id)
)