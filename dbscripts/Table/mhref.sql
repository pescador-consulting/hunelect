CREATE TABLE gerry.mhref
(
  id serial NOT NULL,
	election_id int REFERENCES gerry.election(id) ON DELETE CASCADE,
  maz int not null,
  evk int null,
  list_url varchar(10485760) null,
  CONSTRAINT mhref_pkey PRIMARY KEY (id)
)