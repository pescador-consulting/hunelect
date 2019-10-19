CREATE TABLE gerry.street
(
  id serial NOT NULL,
	district_id int REFERENCES gerry.district(id),
	election_id int REFERENCES gerry.election(id) ON DELETE CASCADE,
  sname varchar(8000) null,
  stype varchar(8000) null,
  CONSTRAINT street_pkey PRIMARY KEY (id)
)