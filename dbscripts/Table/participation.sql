CREATE TABLE gerry.participation
(
  id serial NOT NULL,
  district_id int REFERENCES gerry.district(id),
  election_id int REFERENCES gerry.election(id) ON DELETE CASCADE,
  nszvsz int null,
  mj int null,
  nsz int null,
  ulbnszsz int null,
  ulbszsz int null,
  easzmsz int null,
  elszsz int null,
  eszsz int null,
  vszsz int null,
  sznlvsz int null,
  atvsz int NULL,
  knszvsz int null,
  szmvsz int null,
  akszbbsz int null,
  avnmigjszvsz int null,
  aszm int null,
  asznsz int null,
  CONSTRAINT participation_pkey PRIMARY KEY (id)
)