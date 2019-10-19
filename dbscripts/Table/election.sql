CREATE TABLE gerry.election
(
  id serial NOT NULL,
	e_year int not null,
	e_type character varying(255),
  CONSTRAINT election_pkey PRIMARY KEY (id)
)