CREATE TABLE gerry.capital_results
(
  id serial NOT NULL,
  district_id int REFERENCES gerry.district(id),
  election_id int REFERENCES gerry.election(id) ON DELETE CASCADE,
  orderank int null,
  hname varchar(8000) null,
  pname varchar(8000) null,
  result varchar(8000) null,
  CONSTRAINT capital_results_pkey PRIMARY KEY (id)
)