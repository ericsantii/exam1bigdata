Schemas

create table escuelaspr (region_educativa string, distrito_escolar string, ciudad string, id int, nombre string, nivel string, numero_serie_college_board int) row format delimited fields terminated by ',' stored as textfile;

create table studentspr (region_educativa string, distrito_escolar string, id int, nombre string, nivel string, sexo string,id_estudiante  int) row format delimited fields terminated by ',' stored as textfile;

Exercise 1
select E.region_educativa,E.ciudad, count(*)
from studentspr as S, escuelaspr as E 
where S.id=E.id
group by E.region_educativa,E.ciudad;

Exercise 2
select ciudad, nivel count(*)
from escuelaspr
group by ciudad,nivel;

Exercise 3
select count(*)
from studentspr as S,escuelaspr as E
where S.id=E.id and S.sexo = 'F'and E.ciudad = 'Ponce' and E.nivel = 'Superior';

Exercise 4
select E.region_educativa, E.distrito_escolar, E.ciudad, count(*)
from studentspr as S,escuelaspr as E
where S.id =E.id and S.sexo = 'M'
group by E.region_educativa, E.distrito_escolar, E.ciudad

