CREATE TABLE person 
(
	pid text,
	pname text,
	birthyear text,
	deathyear text,
	primaryprofession text,
	knowfor text
);

CREATE TABLE title_basic
(
	tid text,
	type text,
	primarytitle text,
	originaltitle text,
	isadult int,
	startyear text,
	endyear text,
	mins text,
	genres text
);

CREATE TABLE title_aka
(
	tid text,
	ordering int,
	title text,
	region text,
	language text,
	type text,
	attributes text,
	isoriginal int
);


CREATE TABLE crew
(
	tid text,
	director text,
	writer text
);


CREATE TABLE episode 
(
	tid text,
	ptid text,
	season int,
	enumber int
);


CREATE TABLE princical
(
	tid text.
	ordering int,
	pid text,
	category text,
	job text,
	character text
);

CREATE TABLE rating
(
	tid text,
	rating float,
	numvotes int
);



