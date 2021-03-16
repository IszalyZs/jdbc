DROP TABLE if exists people;

CREATE TABLE people (
	id serial primary key,
	name varchar,
	age smallint,
	city varchar
);

INSERT INTO people(name, age, city) VALUES ('Bob', 20, 'Los Angeles');
INSERT INTO people(name, age, city) VALUES ('Charlie', 18, 'New York City');
INSERT INTO people(name, age, city) VALUES ('Alex', 25, 'Chicago');
INSERT INTO people(name, age, city) VALUES ('Caroline', 17, 'New York City');
INSERT INTO people(name, age, city) VALUES ('Alfred', 15, 'Houston');
INSERT INTO people(name, age, city) VALUES ('Charlotte', 21, 'Los Angeles');
INSERT INTO people(name, age, city) VALUES ('Susan', 40, 'New York City');
INSERT INTO people(name, age, city) VALUES ('Helen', 35, 'Philadelphia');
INSERT INTO people(name, age, city) VALUES ('Pedro', 13, 'San Diego');
INSERT INTO people(name, age, city) VALUES ('Harry', 15, 'New York City');