CREATE TABLE gerry.href
(
  id serial NOT NULL,
	mhref_id int REFERENCES gerry.mhref(id) ON DELETE CASCADE,
  district_id int REFERENCES gerry.district(id),
  national_election varchar(10485760) null,
  local_election varchar(10485760) null,
  participation varchar(10485760) null,
  details varchar(10485760) null,
  shape varchar(10485760) null,
  CONSTRAINT href_pkey PRIMARY KEY (id)
)