CREATE TABLE gerry.district
(
  id serial NOT NULL,
  election_id int REFERENCES gerry.election(id) ON DELETE CASCADE,
	maz int not null,
	taz int not null,
	sorsz int not null,
  evk int not null,
	tip int not null,
  cimt character varying(255),
  cimk character varying(255),
	polygon json null,
  CONSTRAINT district_pkey PRIMARY KEY (id)
)