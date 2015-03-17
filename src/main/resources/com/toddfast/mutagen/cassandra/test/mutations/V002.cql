create columnfamily if not exists "Test1" (
	key varchar PRIMARY KEY,
	value1 varchar
);

alter columnfamily "Test1"
	add value2 varchar;
