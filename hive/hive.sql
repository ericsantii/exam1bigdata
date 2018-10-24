Schemas

create table escuelaspr (region_educativa string, distrito_escolar string, ciudad string, id int, nombre string, nivel string, numero_serie_college_board int) row format delimited fields terminated by ',' stored as textfile;

create table studentspr (region_educativa string, distrito_escolar string, id int, nombre string, nivel string, sexo string,id_estudiante  int) row format delimited fields terminated by ',' stored as textfile;

Exercise 1
select region_educativa,ciudad, count(*)
from studentspr as S, escuelaspr as E 
where S.id=E.id
group by S.region_educativa,E.ciudad;

Exercise 2
select ciudad, nivel count(*)
from escuelaspr
group by ciudad,nivel;

Exercise 3
select count(*)
from studentspr as S,escuelaspr as E
where S.id=E.id and sexo = 'F'and ciudad = 'Ponce' and S.nivel = 'Superior'

Exercise 4
select region_educativa, distrito_escolar, ciudad, count(*)
from studentspr as S,escuelaspr as E
where S.sexo = 'M'
group by S.region_educativa, S.distrito_escolar, E.ciudad

