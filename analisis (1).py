import pandas as pd
import duckdb
import xml.etree.ElementTree as ET
import numpy as np
import matplotlib.pyplot as plt 
from   matplotlib import ticker   
import seaborn as sns           

# Sobre equipos del país y la liga asignada:

# Cual es el id de la liga de Switzerland? consulta necesaria para resolver las demás, obtenido este resultado
# daremos por hecho que el ID de la liga es este
id_liga_suiza = duckdb.sql('''
                           SELECT ID AS ID_liga
                           FROM tabla_liga.csv
                           WHERE Nombre_pais = 'Switzerland'
                           ''')
print(f'El ID de la Liga de Suiza es: \n{id_liga_suiza}')

# ¿Cuál es el equipo con mayor cantidad de partidos ganados?

# acá tomé año = temporada
# primero consigo una tabla con todos los partidos que jugó un equipo de local estos años
# y luego de esos partidos cuento cuantos ganó

ganados_local = duckdb.sql('''
                           SELECT ID_equipo_local, Resultado, COUNT(*) AS Ganados_local
                           FROM (
                                    SELECT ID_local AS ID_equipo_local, 
                                    CASE WHEN Goles_local > Goles_visitante THEN 'Ganado'
                                    WHEN Goles_visitante > Goles_local THEN 'Perdido'
                                    ELSE 'Empatado'
                                    END AS Resultado
                                    FROM tabla_partidos.csv
                                    WHERE ID_liga = 24558 AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                                )
                           GROUP BY ID_equipo_local, Resultado
                           HAVING Resultado = 'Ganado'
                           ORDER BY ID_equipo_local
                           ''')
# hago lo mismo para visitantes
ganados_visitante = duckdb.sql('''
                           SELECT ID_equipo_visitante, Resultado, COUNT(*) AS Ganados_visitante
                           FROM (
                                    SELECT ID_visitante AS ID_equipo_visitante, 
                                    CASE WHEN Goles_local > Goles_visitante THEN 'Perdido'
                                    WHEN Goles_visitante > Goles_local THEN 'Ganado'
                                    ELSE 'Empatado'
                                    END AS Resultado
                                    FROM tabla_partidos.csv
                                    WHERE ID_liga = 24558 AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                                )
                           GROUP BY ID_equipo_visitante, Resultado
                           HAVING Resultado = 'Ganado'
                           ORDER BY ID_equipo_visitante
                           ''')

# ahora sumo la cantidad total de partidos ganados por los equipos: locales + visitante, haciendo un join entre los id para que se sumen los del
# mismo equipo
ganados_totales = duckdb.sql('''
                       SELECT ID_equipo_local AS ID_equipo, Ganados_local + Ganados_visitante AS GANADOS
                       FROM ganados_local
                       INNER JOIN ganados_visitante 
                       ON ID_equipo_local = ID_equipo_visitante
                       ORDER BY GANADOS
                       ''')

# luego en esos partidos tengo que buscar el que mas ganó, así que necesito esta subquery
equipo_mas_ganador = duckdb.sql('''
                          SELECT Nombre
                          FROM ganados_totales AS gt
                          INNER JOIN tabla_equipos.csv AS eq
                          ON eq.ID_equipo = gt.ID_equipo
                          WHERE GANADOS = (SELECT MAX(GANADOS) AS MAXGANADOS
                                            FROM ganados_totales)
                          ''')
print(f'El equipo más ganador en todas las temporadas analizadas es:\n{equipo_mas_ganador} ')

# ¿Cuál es el equipo con mayor cantidad de partidos perdidos de cada año?

# hago lo mismo que para ganador pero esta vez cuento los perdidos agrupandolos por temporada, resultado e ID
# hago esto para local y para visitante

perdidos_local_por_año = duckdb.sql('''
                                    SELECT ID_local, Resultado, Temporada, COUNT(*) AS Perdidos_local
                                    FROM (
                                        SELECT ID_local, 
                                        CASE WHEN Goles_local > Goles_visitante THEN 'Ganado'
                                        WHEN Goles_visitante > Goles_local THEN 'Perdido'
                                        ELSE 'Empatado'
                                        END AS Resultado, Temporada
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(Temporada AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                                    )
                                    GROUP BY ID_local, Resultado, Temporada
                                    HAVING Resultado = 'Perdido'
                                    

                                    ''')

perdidos_visitante_por_año = duckdb.sql('''
                                    SELECT ID_visitante, Resultado, Temporada, COUNT(*) AS Perdidos_visitante
                                    FROM (
                                        SELECT ID_visitante, 
                                        CASE WHEN Goles_local > Goles_visitante THEN 'Perdido'
                                        WHEN Goles_visitante > Goles_local THEN 'Ganado'
                                        ELSE 'Empatado'
                                        END AS Resultado, Temporada
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(Temporada AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                                    )
                                    GROUP BY ID_visitante, Resultado, Temporada
                                    HAVING Resultado = 'Perdido'
                                    

                                    ''')
# Ahora hago la suma de los partidos perdidos como visitante y como local

perdidos_totales_por_año = duckdb.sql('''
                                      SELECT ID_local AS ID_equipo, perloaño.Temporada, Perdidos_local + Perdidos_visitante AS Perdidos_totales
                                      FROM perdidos_local_por_año AS perloaño
                                      INNER JOIN perdidos_visitante_por_año AS perviaño
                                      ON ID_local = ID_visitante AND perloaño.Temporada = perviaño.Temporada
                                      
                                      ''')

# ahora busco cual de todos estos cuales son los que mas perdieron por temporada
mayores_perdedores = duckdb.sql('''
                                SELECT Nombre, perdidos1.Temporada, perdidos1.Perdidos_totales
                                FROM perdidos_totales_por_año AS perdidos1
                                INNER JOIN tabla_equipos.csv AS eq
                                ON eq.ID_equipo = perdidos1.ID_equipo
                                WHERE perdidos1.Perdidos_totales >= ALL  (
                                    SELECT perdidos2.Perdidos_totales
                                    FROM perdidos_totales_por_año AS perdidos2
                                    WHERE perdidos2.Temporada = perdidos1.Temporada
                                )
                                ORDER BY Temporada
                                ''')
print(f'Los equipos más perdedores en todas las temporadas analizadas fueron:\n{mayores_perdedores} ')

# ¿Cuál es el equipo con mayor cantidad de partidos empatados en el último año?

# cuento de la misma forma que los perdidos, agrupando por temporada, resultado e id
# hago esto para local y para visitante
empatados_local = duckdb.sql('''
                                    SELECT ID_local, Resultado, COUNT(*) AS Empatados_local
                                    FROM (
                                        SELECT ID_local, 
                                        CASE WHEN Goles_local > Goles_visitante THEN 'Ganado'
                                        WHEN Goles_visitante > Goles_local THEN 'Perdido'
                                        ELSE 'Empatado'
                                        END AS Resultado, Temporada
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(Temporada AS TEXT) = '2015/2016'
                                    )
                                    GROUP BY ID_local, Resultado, Temporada
                                    HAVING Resultado = 'Empatado'
                                    ORDER BY ID_local

                                    ''')

empatados_visitante = duckdb.sql('''
                                 SELECT ID_visitante, Resultado, COUNT(*) AS Empatados_visitante
                                    FROM (
                                        SELECT ID_visitante, 
                                        CASE WHEN Goles_local > Goles_visitante THEN 'Ganado'
                                        WHEN Goles_visitante > Goles_local THEN 'Perdido'
                                        ELSE 'Empatado'
                                        END AS Resultado, Temporada
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(Temporada AS TEXT) = '2015/2016'
                                    )
                                    GROUP BY ID_visitante, Resultado, Temporada
                                    HAVING Resultado = 'Empatado'
                                    ORDER BY ID_visitante
                                 ''')

# ahora hago un join para obtener la suma de los empatados de local y visitante en base al id del equipo que estamos viendo
empatados_totales = duckdb.sql('''
                               SELECT ID_local AS ID_equipo, Empatados_local + Empatados_visitante AS Empatados
                               FROM empatados_local
                               INNER JOIN empatados_visitante
                               ON ID_local = ID_visitante
                               
                               ''')
#Ahora busco el maximo dentro de este conjunto

equipo_mas_empatador = duckdb.sql('''
                                  SELECT Nombre
                                  FROM empatados_totales AS et
                                  INNER JOIN tabla_equipos.csv AS eq
                                  ON eq.ID_equipo = et.ID_equipo
                                  WHERE Empatados = (
                                      SELECT MAX(Empatados) AS elmaximo
                                      FROM empatados_totales
                                  )
                                  ''')
print(f'El equipo con mayor cantidad de partidos empatados en la última temporada (2015/2016) fue:\n{equipo_mas_empatador} ')

#¿Cuál es el equipo con mayor cantidad de goles a favor?

#pais = Switzerland, id_liga = 24558
#rango temporadas = 2012 a 2016

#Hice la union de id_local|goles_local con id_visitante|goles_visitante para obtener una tabla que contiene todos los
#goles por partido de cada equipo(acotando los partidos que se jugaron en la liga dada y el rango de tiempo dado).
#Luego los agrupe por el equipo y sume todos lo valores. Finalmente agregue el nombre del equipo haciendo un join con la 
#tabla equipo y ordene los goles de mayor a menor.


tabla_goles_a_favor = duckdb.sql ('''
                               SELECT ID_partido, ID_local AS ID_equipo, Goles_local AS goles_a_favor
                               FROM tabla_partidos.csv
                               WHERE ID_liga = '24558' AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                               UNION
                               SELECT ID_partido, ID_visitante, Goles_visitante
                               FROM tabla_partidos.csv
                               WHERE ID_liga = '24558' AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                               ''')
                               
goles_a_favor_por_id_equipo = duckdb.sql ('''
                               SELECT ID_equipo, SUM(goles_a_favor) AS goles_a_favor_totales
                               FROM tabla_goles_a_favor
                               GROUP BY ID_equipo
                               ''')
                               
goles_por_equipo = duckdb.sql('''
                              SELECT goles.ID_equipo,Nombre, goles_a_favor_totales AS Goles_totales
                              FROM goles_a_favor_por_id_equipo AS goles
                              JOIN tabla_equipos.csv AS equipos
                              ON goles.ID_equipo = equipos.ID_equipo 
                              WHERE goles_a_favor_totales = (
                                  SELECT MAX(goles_a_favor_totales) AS max
                                  FROM goles_a_favor_por_id_equipo
                              )
                              ''')

print(f'El equipo con mayor cantidad de goles a favor fue:\n{goles_por_equipo}')

#¿Cuál es el equipo con mayor diferencia de goles?

#similar al anterior, genere la tabla con los goles totales en contra por equipo, luego hice un join para
#unir la tabla con goles a favor y goles en contra por id_equipo y finalmente calcule la dif ordenando de mayor a menor.

tabla_goles_en_contra = duckdb.sql ('''
                               SELECT ID_partido, ID_local AS ID_equipo, Goles_visitante AS goles_en_contra
                               FROM tabla_partidos.csv
                               WHERE ID_liga = '24558' AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                               UNION
                               SELECT ID_partido, ID_visitante, Goles_local
                               FROM tabla_partidos.csv 
                               WHERE ID_liga = '24558' AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                               ''')
                               
goles_en_contra_por_id_equipo = duckdb.sql ('''
                               SELECT ID_equipo, SUM(goles_en_contra) AS Goles_en_contra_totales
                               FROM tabla_goles_en_contra
                               GROUP BY ID_equipo
                               ''')

goles_por_equipo_aFavor_enContra= duckdb.sql('''
                                  SELECT a_favor.ID_equipo,Nombre,Goles_totales AS Goles_a_favor_totales, Goles_en_contra_totales
                                  FROM goles_por_equipo AS a_favor
                                  JOIN goles_en_contra_por_id_equipo AS en_contra
                                  ON a_favor.ID_equipo = en_contra.ID_equipo
                                  ''')

dif_goles = duckdb.sql('''
                       SELECT ID_equipo, Nombre, ABS(Goles_a_favor_totales - Goles_en_contra_totales) AS Diferencia_goles
                       FROM goles_por_equipo_aFavor_enContra
                       ORDER BY Diferencia_goles DESC
                       ''')

print(f'El equipo con mayor diferencia de gol fue:\n{dif_goles}')

# ¿Cuántos jugadores tuvo durante el período de tiempo seleccionado cada equipo en su plantel?

# Armo una tabla que contenga sólo a los jugadores asociados a una única temporada y liga (tempoliga)
planteles_temporada = duckdb.sql("""SELECT ID_plantel
                                    FROM tabla_plantel.csv
                                    WHERE Temporada='2012/2013' OR
                                    Temporada='2013/2014' OR
                                    Temporada='2014/2015' OR
                                    Temporada='2015/2016'
                                    """)
planteles_tempoliga = duckdb.sql("""SELECT planteles_temporada.ID_plantel,
                                    FROM planteles_temporada, tabla_plantel.csv AS p, tabla_equipos.csv AS eq
                                    WHERE planteles_temporada.ID_plantel = p.ID_plantel AND
                                          p.ID_equipo = eq.ID_equipo AND
                                          eq.ID_liga = 24558
                                    """)

jugadores_tempoliga = duckdb.sql("""SELECT planteles_tempoliga.ID_plantel, tabla_conformacion_plantel.ID_jugador
                                    FROM tabla_conformacion_plantel.csv, planteles_tempoliga
                                    WHERE planteles_tempoliga.ID_plantel = tabla_conformacion_plantel.ID_plantel;""")

plantel_cantidad = duckdb.sql("""SELECT ID_plantel, COUNT(ID_jugador) AS Cantidad_Jugadores
                              FROM jugadores_tempoliga
                              GROUP BY ID_plantel""")

tabla_respuesta = duckdb.sql("""SELECT p.ID_equipo, eq.Nombre, plantel_cantidad.ID_plantel, plantel_cantidad.Cantidad_Jugadores
                             FROM tabla_equipos.csv AS eq, tabla_plantel.csv AS p, plantel_cantidad
                             WHERE p.ID_equipo = eq.ID_equipo AND
                                   p.ID_plantel = plantel_cantidad.ID_plantel
                             ORDER BY plantel_cantidad.Cantidad_Jugadores
                             """)

print(f'Tabla con todos los jugadores que tuvieron los equipos en sus planteles:\n{tabla_respuesta}')

#Sobre jugadores del país y la liga asignada:

# ¿Cuál es el jugador con mayor cantidad de goles?
# basicamente hay que contar la cantidad de veces que aparece cada jugador

goleador = duckdb.sql('''
                      SELECT ID_Jugador, COUNT(*) AS Goles
                      FROM (
                          SELECT *
                          FROM tabla_goles.csv AS g
                          WHERE g.ID_Partido IN (
                              SELECT ID_Partido
                              FROM tabla_partidos.csv
                              WHERE ID_liga = 24558 
                              AND CAST(Temporada AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                          )
                      )
                      GROUP BY ID_Jugador
                      ''')

maximo_goleador = duckdb.sql('''
                             SELECT Nombre, Goles
                             FROM goleador
                             INNER JOIN tabla_jugador.csv AS tj
                             ON tj.ID = goleador.ID_Jugador
                             WHERE Goles = (
                                 SELECT MAX(Goles) AS maximo
                                 FROM goleador
                             )
                             ''')
print(f'El jugador con mayor cantidad de goles es: \n{maximo_goleador}')


# ¿Cuáles son los jugadores que más partidos ganó su equipo?
ganados_local_por_temp = duckdb.sql('''
                                    SELECT ID_local, Resultado, Temporada, COUNT(*) AS Ganados_local
                                    FROM (
                                        SELECT ID_local, 
                                        CASE WHEN Goles_local > Goles_visitante THEN 'Ganado'
                                        WHEN Goles_visitante > Goles_local THEN 'Perdido'
                                        ELSE 'Empatado'
                                        END AS Resultado, Temporada
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(Temporada AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                                    )
                                    GROUP BY ID_local, Resultado, Temporada
                                    HAVING Resultado = 'Ganado'
                                    ORDER BY ID_local
                                    ''')
ganados_vis_por_temp = duckdb.sql('''
                                    SELECT ID_visitante, Resultado, Temporada, COUNT(*) AS Ganados_visitante
                                    FROM (
                                        SELECT ID_visitante, 
                                        CASE WHEN Goles_local > Goles_visitante THEN 'Perdido'
                                        WHEN Goles_visitante > Goles_local THEN 'Ganado'
                                        ELSE 'Empatado'
                                        END AS Resultado, Temporada
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(Temporada AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                                    )
                                    GROUP BY ID_visitante, Resultado, Temporada
                                    HAVING Resultado = 'Ganado'
                                    ORDER BY ID_visitante

                                  ''')
ganados_totales_por_temp = duckdb.sql('''
                                      SELECT ID_local AS ID_equipo, l.Temporada, (Ganados_local + Ganados_visitante) AS Ganados
                                      FROM ganados_local_por_temp AS l
                                      INNER JOIN ganados_vis_por_temp AS v
                                      ON ID_local = ID_visitante AND l.Temporada = v.Temporada
                                      ORDER BY l.Temporada
                                      ''')
ganados_equipo_plantel = duckdb.sql('''
                                    SELECT g.ID_equipo, g.Temporada, Ganados, ID_plantel
                                    FROM ganados_totales_por_temp AS g
                                    INNER JOIN tabla_plantel.csv AS p
                                    ON p.Temporada = g.Temporada AND p.ID_equipo = g.ID_equipo
                                    ''')

jugadores = duckdb.sql('''
                                          SELECT ID_jugador, tcf.ID_plantel
                                          FROM tabla_conformacion_plantel.csv AS tcf
                                          INNER JOIN ganados_equipo_plantel AS gep
                                          ON gep.ID_plantel = tcf.ID_plantel
                                          ''')

jugadores_con_ganados = duckdb.sql('''
                                   SELECT ID_jugador, SUM(Ganados) AS Total_Ganados
                                   FROM jugadores AS j
                                   INNER JOIN ganados_equipo_plantel AS gep
                                   ON j.ID_plantel = gep.ID_plantel
                                   GROUP BY ID_jugador
                                   ''')
respuesta = duckdb.sql('''
                       SELECT ID_jugador,NOmbre, Total_Ganados
                       FROM jugadores_con_ganados
                       INNER JOIN tabla_jugador.csv 
                       ON ID = ID_jugador 
                       WHERE Total_Ganados = (SELECT MAX(Total_Ganados) AS maximo
                                              FROM jugadores_con_ganados)
                       ''')

print(f'Los jugadores que más partidos ganaron sus equipos son: \n{respuesta}')

# ¿Cuál es el jugador que estuvo en más equipos?

#tome los equipos de la liga y la temporada que necesito consultar. Obtuve las id de los planteles y eeuipos que necesito para la consulta.
#Con un join de los equipos y planteles con los id_jugadores obtuve la informacion de todos los equipos y planteles que formaron parte
#todos los jugadores. Ordene por jugador y conte la cantidad de equipos y al final agregue el nombre del jugador a la tabla.

equipo_temporada= duckdb.sql('''
                             SELECT DISTINCT ID_local, Temporada
                             FROM tabla_partidos.csv
                             WHERE ID_liga = 24558 AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                             ''')

plantel_consulta = duckdb.sql('''
                       SELECT ID_plantel, ID_equipo
                       FROM tabla_plantel.csv AS plantel
                       JOIN equipo_temporada
                       ON ID_local = ID_equipo AND equipo_temporada.Temporada = plantel.Temporada
                       ''')
                       
id_jugador_plantel = duckdb.sql('''
                             SELECT ID_equipo, c_plantel.ID_plantel, ID_jugador
                             FROM tabla_conformacion_plantel.csv AS c_plantel
                             JOIN plantel_consulta 
                             ON plantel_consulta.ID_plantel = c_plantel.ID_plantel
                             ''')
                             
id_jugador_por_cantidad_equipos = duckdb.sql('''
                                          SELECT ID_jugador, COUNT(ID_equipo) AS Cantidad_equipos
                                          FROM id_jugador_plantel
                                          GROUP BY ID_jugador
                                          ''')
                                          
jugador_por_cantidad_equipos = duckdb.sql('''
                                          SELECT ID_jugador, Nombre, Cantidad_equipos
                                          FROM id_jugador_por_cantidad_equipos
                                          JOIN tabla_jugador.csv
                                          ON ID_jugador = ID
                                          WHERE Cantidad_equipos = (SELECT MAX(Cantidad_equipos)
                                          FROM id_jugador_por_cantidad_equipos)
                                          ORDER BY Cantidad_equipos DESC
                                          ''')


print(f'El jugador que estuvo en más equipos es: \n{jugador_por_cantidad_equipos}')

# ¿Cuál es el jugador que menor variación de potencia ha tenido a lo largo de los años? (medida
# en valor absoluto)

# Interpretación 1 - "potencia" es más cercano en nombre: potential (Potencial)
# Interpretación 2 - "potencia" es más cercano en significado: overall_rating (Puntos_Generales)

# Interpretación .1 - "variacion" es la diferencia entre el máximo y el mínimo.
# Interpretación .2 - "variacion" es la suma de todo cambio medido sobre la variable.

"""
INTERPRETACIÓN 1.1
"potencia" = Potencial
"variacion" = delta(max, min)
"""

# Primer Paso: Aislo potencial del resto de atributos. Tomo sólo los valores no-nulos.
potengadores_raw = duckdb.sql("""SELECT jugadores_tempoliga.ID_jugador, Potencial
                                 FROM tabla_atributos_jugador.csv, jugadores_tempoliga
                                 WHERE jugadores_tempoliga.ID_jugador = tabla_atributos_jugador.ID_jugador AND
                                       Potencial IS NOT NULL
                          """)


# Segundo Paso: Elimino los jugadores con una única aparición, pues medir su variacion es imposible.
potengadores = duckdb.sql("""SELECT potengadores_raw.ID_jugador, Potencial
                             FROM potengadores_raw
                             WHERE 1 < (SELECT COUNT(ID_jugador)
                                        FROM potengadores_raw)
                             GROUP BY ID_jugador, Potencial""")


# Segundo Paso: Encuentro los máximos y mínimos por jugador, y guardo la tabla final.
tabla_respuesta_1_1 = duckdb.sql("""SELECT potengadores.ID_jugador, ROUND(max(Potencial) - min(Potencial)) AS Deltamaxmin
                                FROM potengadores
                                GROUP BY potengadores.ID_jugador
                                """)

p10_1_1 = duckdb.sql("""SELECT *
                        FROM tabla_respuesta_1_1
                        WHERE Deltamaxmin = (SELECT MIN(Deltamaxmin)
                                             FROM tabla_respuesta_1_1)
                     """)
print(f'Los jugadores que menor variacion de potencia tuvieron fueron: \n{p10_1_1}')

"""
Interpretacion 2.1
"potencia" = Puntos_Generales
"variacion" = delta(max, min)
"""

# Primer Paso: Aislo Puntos_Generales del resto de atributos. Tomo sólo los valores no-nulos.
potengadores2_raw = duckdb.sql("""SELECT jugadores_tempoliga.ID_jugador, Puntos_Generales
                                 FROM tabla_atributos_jugador.csv, jugadores_tempoliga
                                 WHERE jugadores_tempoliga.ID_jugador = tabla_atributos_jugador.ID_jugador AND
                                       Puntos_Generales IS NOT NULL
                          """)

# Segundo Paso: Elimino los jugadores con una única aparición, pues medir su variacion es imposible.
potengadores2 = duckdb.sql("""SELECT potengadores2_raw.ID_jugador, Puntos_Generales
                             FROM potengadores2_raw
                             WHERE 1 < (SELECT COUNT(ID_jugador)
                                        FROM potengadores2_raw)
                             GROUP BY ID_jugador, Puntos_Generales""")


# Segundo Paso: Encuentro los máximos y mínimos por jugador, y guardo la tabla final.
tabla_respuesta_2_1 = duckdb.sql("""SELECT potengadores2.ID_jugador, ROUND(max(Puntos_Generales) - min(Puntos_Generales)) AS Deltamaxmin
                                FROM potengadores2
                                GROUP BY potengadores2.ID_jugador
                                """)

p10_2_1 = duckdb.sql("""SELECT *
                        FROM tabla_respuesta_2_1
                        WHERE Deltamaxmin = (SELECT MIN(Deltamaxmin)
                                             FROM tabla_respuesta_2_1)
                     """)
print(f'Los jugadores que menor variacion de potencia tuvieron fueron: \n{p10_2_1}')

# CONSULTAS ADICIONALES
# cuales equipos descendieron y cuales acendieron?
# con la tabla partidos filtro los partidos suizos
# con esos partidos me fijo cuales eran los equipos que arrancaron en nuestro período de observación
# después miro quienes fueron descendicendo temporada a temporada
# para hacer esto agarro los equipos de una temporada y me fijo que no estén en la siguiente, esos son los que descendieron


equipos_iniciales_2012_2013 = duckdb.sql('''
                               SELECT ID_local AS ID_equipo, Temporada
                               FROM tabla_partidos.csv
                               WHERE ID_liga = 24558 
                               AND CAST(Temporada AS TEXT) = '2012/2013'
                               UNION
                               SELECT ID_visitante AS ID_equipo, Temporada
                               FROM tabla_partidos.csv
                               WHERE ID_liga = 24558 
                               AND CAST(Temporada AS TEXT) = '2012/2013'
                               ''')
descensos_temporada_2012_13 = duckdb.sql('''
                               SELECT ID_equipo,Temporada
                               FROM equipos_iniciales_2012_2013
                               WHERE ID_equipo NOT IN (SELECT ID_local AS ID_equipo
                                                       FROM tabla_partidos.csv
                                                       WHERE ID_liga = 24558 
                                                       AND CAST(Temporada AS TEXT) = '2013/2014'
                                                       UNION
                                                       SELECT ID_visitante AS ID_equipo
                                                       FROM tabla_partidos.csv
                                                       WHERE ID_liga = 24558 
                                                       AND CAST(Temporada AS TEXT) = '2013/2014')
                               
                               ''')
descensos_temporada_2013_14 = duckdb.sql('''
                                           SELECT ID_equipo,Temporada
                                           FROM ( SELECT ID_local AS ID_equipo, Temporada
                                                  FROM tabla_partidos.csv
                                                  WHERE ID_liga = 24558 
                                                  AND CAST(Temporada AS TEXT) = '2013/2014'
                                                  UNION
                                                  SELECT ID_visitante AS ID_equipo, Temporada
                                                  FROM tabla_partidos.csv
                                                  WHERE ID_liga = 24558 
                                                  AND CAST(Temporada AS TEXT) = '2013/2014')
                                                  
                                           WHERE ID_equipo NOT IN (   SELECT ID_local AS ID_equipo
                                                                      FROM tabla_partidos.csv
                                                                      WHERE ID_liga = 24558 
                                                                      AND CAST(Temporada AS TEXT) = '2014/2015'
                                                                      UNION
                                                                      SELECT ID_visitante AS ID_equipo
                                                                      FROM tabla_partidos.csv
                                                                      WHERE ID_liga = 24558 
                                                                      AND CAST(Temporada AS TEXT) = '2014/2015')
                               ''')
descensos_temporada_2014_15 = duckdb.sql('''
                               SELECT ID_equipo,Temporada
                               FROM (SELECT ID_local AS ID_equipo, Temporada
                                     FROM tabla_partidos.csv
                                     WHERE ID_liga = 24558 
                                     AND CAST(Temporada AS TEXT) = '2014/2015'
                                     UNION
                                     SELECT ID_visitante AS ID_equipo, Temporada
                                     FROM tabla_partidos.csv
                                     WHERE ID_liga = 24558 
                                     AND CAST(Temporada AS TEXT) = '2014/2015')
                                                  
                               WHERE ID_equipo NOT IN (SELECT ID_local AS ID_equipo
                                                       FROM tabla_partidos.csv
                                                       WHERE ID_liga = 24558 
                                                       AND CAST(Temporada AS TEXT) = '2015/2016'
                                                       UNION
                                                       SELECT ID_visitante AS ID_equipo
                                                       FROM tabla_partidos.csv
                                                       WHERE ID_liga = 24558 
                                                       AND CAST(Temporada AS TEXT) = '2015/2016')
                               ''')
# no sé como crear una columna en sql, la única forma que conozco y que está en las diapos es con un
# condicional. Nunca un id va a ser negativo, aunque sea no lo es en nuestros datos.
todos_los_descendidos = duckdb.sql('''
                                   SELECT ID_equipo, Temporada,
                                   CASE WHEN ID_equipo != -1 THEN 'Descendio' END AS Estado
                                   FROM (
                                        SELECT ID_equipo, Temporada
                                        FROM descensos_temporada_2012_13
                                        UNION
                                        SELECT ID_equipo, Temporada
                                        FROM descensos_temporada_2013_14
                                        UNION
                                        SELECT ID_equipo, Temporada
                                        FROM descensos_temporada_2014_15
                                   )
                                   ''')

ascensos_temporada_2012_13 = duckdb.sql('''
                               SELECT ID_equipo, Temporada
                               FROM (
                                   SELECT ID_local AS ID_equipo, Temporada
                                   FROM tabla_partidos.csv
                                   WHERE ID_liga = 24558 
                                   AND CAST(Temporada AS TEXT) = '2013/2014'
                                   UNION
                                   SELECT ID_visitante AS ID_equipo, Temporada
                                   FROM tabla_partidos.csv
                                   WHERE ID_liga = 24558 
                                   AND CAST(Temporada AS TEXT) = '2013/2014'
                               )
                               WHERE ID_equipo NOT IN (
                                    SELECT ID_equipo
                                    FROM equipos_iniciales_2012_2013)
                               
                               ''')
ascensos_temporada_2013_14 = duckdb.sql('''
                               SELECT ID_equipo, Temporada
                               FROM (
                                   SELECT ID_local AS ID_equipo, Temporada
                                   FROM tabla_partidos.csv
                                   WHERE ID_liga = 24558 
                                   AND CAST(Temporada AS TEXT) = '2014/2015'
                                   UNION
                                   SELECT ID_visitante AS ID_equipo, Temporada
                                   FROM tabla_partidos.csv
                                   WHERE ID_liga = 24558 
                                   AND CAST(Temporada AS TEXT) = '2014/2015'
                               )
                               WHERE ID_equipo NOT IN (
                                    SELECT ID_equipo
                                    FROM (
                                        SELECT ID_local AS ID_equipo
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(Temporada AS TEXT) = '2013/2014'
                                        UNION
                                        SELECT ID_visitante AS ID_equipo
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(Temporada AS TEXT) = '2013/2014'
 
                                    ))
                               ''')
ascensos_temporada_2014_15 = duckdb.sql('''
                               SELECT *
                               FROM (
                                   SELECT ID_local AS ID_equipo, Temporada
                                   FROM tabla_partidos.csv
                                   WHERE ID_liga = 24558 
                                   AND CAST(Temporada AS TEXT) = '2015/2016'
                                   UNION
                                   SELECT ID_visitante AS ID_equipo, Temporada
                                   FROM tabla_partidos.csv
                                   WHERE ID_liga = 24558 
                                   AND CAST(Temporada AS TEXT) = '2015/2016'
                               )
                               WHERE ID_equipo NOT IN (
                                    SELECT ID_equipo
                                    FROM (
                                        SELECT ID_local AS ID_equipo
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(Temporada AS TEXT) = '2014/2015'
                                        UNION
                                        SELECT ID_visitante AS ID_equipo
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(Temporada AS TEXT) = '2014/2015'
                                    ))
                               ''')
todos_los_ascendidos = duckdb.sql('''
                                   SELECT ID_equipo, Temporada,
                                   CASE WHEN ID_equipo != -1 THEN 'Ascendio' END AS Estado
                                   FROM (
                                        SELECT *
                                        FROM ascensos_temporada_2012_13
                                        UNION
                                        SELECT *
                                        FROM ascensos_temporada_2013_14
                                        UNION
                                        SELECT *
                                        FROM ascensos_temporada_2014_15
                                   )
                                   ''')
ascensos_y_descensos = duckdb.sql('''
                                  SELECT eq.ID_equipo, eq.Nombre, esta.Temporada, esta.Estado
                                  FROM 
                                  (SELECT *
                                  FROM todos_los_descendidos
                                  UNION
                                  SELECT *
                                  FROM todos_los_ascendidos) AS esta
                                  INNER JOIN tabla_equipos.csv AS eq
                                  ON esta.ID_equipo = eq.ID_equipo
                                  ORDER BY Temporada
                                  
                                  ''')
print(f'Cuáles equipos descendieron y cuales ascendieron en estas temporadas?\n{ascensos_y_descensos}')
# luego esta tabla tiene quienes descendieron y quienes ascendieron en cada una de las temporadas
# las temporadas de descenso corresponden al año en el que el equipo salió último en el campeonato
# las temporadas de ascenso corresponden al año en el que ese equipo volvió a jugar en la liga

# Cual es el equipo con mayor cantidad de goles de penales?

#Para esta consulta tuvimos que dividir el problema. Primero limitamos la tabla goles a los goles metidos en la liga y temporadas dadas.
#Luego buscamos en esa tabla solo los goles que eran por penal(Esto por alguna razon nos dio solo penales de la ultima temporada pero puede
#ser por algun problema con la tabla goles). Luego teniamos la informacion de quien metio el penal y en que temporada pero no habia forma de
#hacer un join con la tabla de conformacion plantel a partir del id jugador sin esperar tuplas espureas o algun error ya que los jugadores 
#pertenecen a varios planteles. Para solucionarlo primero acotamos la tabla conformacion plantel a la temporada dada y ahora si hicimos el join.
#finalmente contamos la cantidad de veces que nos aparecian los equipos ya que estos eran los goles y le agregamos el nombre del equipo con un join a la talba equipos.


tabla_partidos_acotados = duckdb.sql ('''
                               SELECT ID_partido,Temporada
                               FROM tabla_partidos.csv
                               WHERE ID_liga = '24558' AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                               UNION
                               SELECT ID_partido, Temporada
                               FROM tabla_partidos.csv
                               WHERE ID_liga = '24558' AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                               ''')

tabla_goles_con_partidos_acotados = duckdb.sql('''
                                               SELECT tg.ID_partido, tg.ID_jugador, Temporada, Tipo
                                               FROM tabla_goles.csv AS tg
                                               JOIN tabla_partidos_acotados AS tpa
                                               ON tg.ID_partido = tpa.ID_partido
                                               ''')

penales_acotados = duckdb.sql('''
                              SELECT ID_partido,ID_jugador, Temporada
                              FROM tabla_goles_con_partidos_acotados 
                              WHERE Tipo = 'penal por desempate'
                              ''')

#penales_acotados ya da 14 penales en solo 2015/2016

conformacion_plantel_por_temporada = duckdb.sql('''
                                                SELECT tp.ID_plantel, ID_jugador
                                                FROM tabla_plantel.csv AS tp
                                                JOIN tabla_conformacion_plantel.csv AS tcp
                                                ON tcp.ID_plantel = tp.ID_plantel
                                                WHERE Temporada = '2015/2016'
                                                ''')

plantel_penales = duckdb.sql('''
                             SELECT Temporada, cpt.ID_plantel
                             FROM penales_acotados 
                             JOIN conformacion_plantel_por_temporada AS cpt
                             ON penales_acotados.ID_jugador = cpt.ID_jugador
                             ''')

penales_id_equipos = duckdb.sql('''
                             SELECT ID_equipo, COUNT(ID_equipo) AS penales_a_favor
                             FROM plantel_penales AS pp
                             JOIN tabla_plantel.csv AS tp
                             ON pp.ID_plantel = tp.ID_plantel
                             GROUP BY ID_equipo
                             ''')
#como ya estamos en una temporada determinada, plantel y equipo son lo mismo.


penales_equipos = duckdb.sql('''
                             SELECT Nombre, penales_a_favor
                             FROM penales_id_equipos AS pie
                             JOIN tabla_equipos.csv AS te
                             ON pie.ID_equipo = te.ID_equipo
                             WHERE penales_a_favor = (
                                 SELECT MAX(penales_a_favor)
                                 FROM penales_id_equipos
                             )
                             ''')


print(f'El equipo con más goles convertidos de penal es: \n{penales_equipos}')
                           

# Visualizaciones

# Graficar la cantidad de goles a favor y en contra de cada equipo a lo largo de los años que elijan

#pais = Switzerland, id_liga = 24558
#rango años = 2012 a 2016

#Obtuve los goles a favor y en contra de todos los partidos que correspondian a la liga y la fechas dadas.
#Luego ordene por equipo y año para poder hacer la suma total de goles a favor y en contra. Finalmente agregue con un JOIN el nombre
#de los equipos.

#Para la visualizacion primero buscamos un conjunto de equipos que hayan jugado en todos los años dados(esto es porque hay algunos equipos que
#jugaron en los años dados pero no en todos). Estos equipos se encuentran en una lista(sus ids). Luego sobre este conjuto armamos los graficos de
#barras formados por la cantidad de goles a favor y en contra por año de cada equipo. 

tabla_goles_a_favor = duckdb.sql ('''
                               SELECT ID_partido, Fecha, ID_local AS ID_equipo, Goles_local AS goles_a_favor
                               FROM tabla_partidos.csv
                               WHERE ID_liga = '24558' AND (EXTRACT(YEAR FROM Fecha) = 2012 OR EXTRACT(YEAR FROM Fecha) = 2013 OR EXTRACT(YEAR FROM Fecha) = 2014 OR EXTRACT(YEAR FROM Fecha) = 2015 OR EXTRACT(YEAR FROM Fecha) = 2016) 
                               UNION
                               SELECT ID_partido, Fecha, ID_visitante, Goles_visitante
                               FROM tabla_partidos.csv
                               WHERE ID_liga = '24558' AND (EXTRACT(YEAR FROM Fecha) = 2012 OR EXTRACT(YEAR FROM Fecha) = 2013 OR EXTRACT(YEAR FROM Fecha) = 2014 OR EXTRACT(YEAR FROM Fecha) = 2015 OR EXTRACT(YEAR FROM Fecha) = 2016)
                               ''')

tabla_goles_en_contra = duckdb.sql ('''
                               SELECT ID_partido, Fecha, ID_local AS ID_equipo, Goles_visitante AS goles_en_contra
                               FROM tabla_partidos.csv
                               WHERE ID_liga = '24558' AND (EXTRACT(YEAR FROM Fecha) = 2012 OR EXTRACT(YEAR FROM Fecha) = 2013 OR EXTRACT(YEAR FROM Fecha) = 2014 OR EXTRACT(YEAR FROM Fecha) = 2015 OR EXTRACT(YEAR FROM Fecha) = 2016)
                               UNION
                               SELECT ID_partido, Fecha, ID_visitante, Goles_local
                               FROM tabla_partidos.csv 
                               WHERE ID_liga = '24558' AND (EXTRACT(YEAR FROM Fecha) = 2012 OR EXTRACT(YEAR FROM Fecha) = 2013 OR EXTRACT(YEAR FROM Fecha) = 2014 OR EXTRACT(YEAR FROM Fecha) = 2015 OR EXTRACT(YEAR FROM Fecha) = 2016)
                               ''')


tabla_goles_por_anios = duckdb.sql('''
                                    SELECT EXTRACT(YEAR FROM a_favor.Fecha) AS anio,a_favor.ID_equipo, SUM(goles_a_favor) AS total_goles_a_favor, SUM(goles_en_contra) AS total_goles_en_contra
                                    FROM tabla_goles_a_favor AS a_favor
                                    JOIN tabla_goles_en_contra AS en_contra
                                    ON a_favor.ID_partido = en_contra.ID_partido AND a_favor.ID_equipo = en_contra.ID_equipo
                                    GROUP BY a_favor.ID_equipo, anio
                                    ''')
                                    
equipos_goles = duckdb.sql('''
                           SELECT anio, goles_anios.ID_equipo, Nombre, total_goles_a_favor, total_goles_en_contra
                           FROM tabla_goles_por_anios AS goles_anios
                           JOIN tabla_equipos.csv AS equipo
                           ON goles_anios.ID_equipo = equipo.ID_equipo
                           ORDER BY goles_anios.ID_equipo, anio
                           ''')


df_equipo_goles = equipos_goles.df()


equipos_que_jugaron_todos_los_anios = [9931,9956,10179,10192,10190,10199]

for equipo in equipos_que_jugaron_todos_los_anios:
    df_recortado_por_equipo = df_equipo_goles.loc[df_equipo_goles.ID_equipo == equipo]
    
    nombre_equipo = df_recortado_por_equipo['Nombre'].iloc[0] 
    id_equipo = df_recortado_por_equipo['ID_equipo'].iloc[0]
        
    fig, ax = plt.subplots()

    plt.rcParams['font.family'] = 'sans-serif' 

    df_recortado_por_equipo.plot(x = 'anio', 
                                 y = ['total_goles_a_favor','total_goles_en_contra'], 
                                 kind = 'bar',
                                 label = ['Goles a favor','Goles en contra'],
                                 ax = ax)

    ax.set_title(f'Goles a favor y en contra por año de {nombre_equipo},id = {id_equipo}')
    ax.set_xlabel('Año')
    ax.set_ylabel('Cantidad goles')
    plt.legend()
    plt.show()
    
# Graficar el promedio de gol de los equipos a lo largo de los años que elijan.

goles_por_equipo_local = duckdb.sql('''
                               SELECT ID_local AS ID_equipo, Temporada, Goles_local
                               FROM tabla_partidos.csv
                               WHERE ID_liga = 24558 
                               AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                               ORDER BY ID_local
                               ''')
goles_por_equipo_visitante = duckdb.sql('''
                                        SELECT ID_visitante AS ID_equipo, Temporada, Goles_visitante
                                        FROM tabla_partidos.csv
                                        WHERE ID_liga = 24558 
                                        AND CAST(TEMPORADA AS TEXT) IN ('2012/2013','2013/2014','2014/2015','2015/2016')
                                        ORDER BY ID_visitante
                                        ''')

goles_por_equipo = duckdb.sql('''
                              SELECT ID_equipo, Temporada, Goles_local AS Goles
                              FROM goles_por_equipo_local
                              UNION ALL
                              SELECT ID_equipo, Temporada, Goles_visitante
                              FROM goles_por_equipo_visitante
                              
                              ''')

tabla_promedio_gol = duckdb.sql('''
                                SELECT g.ID_equipo, Nombre, Temporada, AVG(Goles) AS Promedio_gol
                                FROM goles_por_equipo AS g
                                INNER JOIN tabla_equipos.csv AS eq
                                ON g.ID_equipo = eq.ID_equipo
                                GROUP BY g.ID_equipo, Temporada, Nombre
                                ORDER BY Nombre, Temporada
                                ''')
promedios_por_temporada_todos = duckdb.sql('''
                                SELECT Temporada, AVG(Goles) AS Promedio_gol
                                FROM goles_por_equipo
                                GROUP BY Temporada
                                ORDER BY  Temporada
                                ''')

df_promiedos = promedios_por_temporada_todos.df()
df_promedios_equipos = tabla_promedio_gol.df()

# creo el gráfico juntando todos los equipos
fig, ax =plt.subplots()
ax.plot('Temporada', 'Promedio_gol', data = df_promiedos, marker='o')
ax.set_title('Promedio de Gol de todos los equipos a lo largo de las Temporadas')
ax.set_xlabel('Temporadas')
ax.set_ylabel('Promedio de Gol')
ax.set_yticks(np.arange(1.24,1.60,0.04))
plt.show()

# gráficos del promedio de gol de cada equipo
print(df_promedios_equipos.Nombre.unique())
for equipo in (df_promedios_equipos.Nombre.unique()):
    
    df_recortado_por_equipo = df_promedios_equipos.loc[df_promedios_equipos.Nombre == equipo]
    
    fig, ax = plt.subplots()
    ax.bar(data = df_recortado_por_equipo, x= 'Temporada', height='Promedio_gol',width=0.5 )
    ax.set_title(f'Promedio de Gol a lo largo de las temporadas de {equipo}')
    ax.set_xlabel('Temporada')
    ax.set_ylabel('Promedio de Gol')
    ax.set_yticks([])
    ax.bar_label(ax.containers[0],fontsize=8)
    
    plt.show()


#
# Graficar el número de goles convertidos por cada equipo en función de la suma de todos sus atributos.
# Primer paso: Obtengo Suma de Atributos por C/J, eliminando los nulos.
# ¿Habrá alguna mejor manera de hacer esto?
suma_atributos = duckdb.sql("""SELECT jugadores_tempoliga.ID_jugador,
                            (aj.Potencial + aj.Cruce + aj.Finalizacion + 
                            aj.Dribbling + aj.Precision_Patada_libre + 
                            aj.Control_Pelota + aj.Aceleracion + 
                            aj.Velocidad_Corriendo + aj.Agilidad + aj.Reacciones +
                            aj.Balance + aj.Poder_de_tiro + aj.Salto +
                            aj.Fuerza + aj.Agresion + aj.Intercepciones +
                            aj.Vision + aj.Penales + aj.Marcar
                            ) AS Suma_Atributos
                            FROM tabla_atributos_jugador.csv as aj, jugadores_tempoliga
                            WHERE jugadores_tempoliga.ID_jugador = aj.ID_jugador AND
                                  aj.Potencial IS NOT NULL AND
                                  aj.Cruce IS NOT NULL AND
                                  aj.Finalizacion IS NOT NULL AND
                                  aj.Dribbling IS NOT NULL AND
                                  aj.Precision_Patada_libre IS NOT NULL AND
                                  aj.Control_Pelota IS NOT NULL AND
                                  aj.Aceleracion IS NOT NULL AND
                                  aj.Velocidad_Corriendo IS NOT NULL AND
                                  aj.Agilidad IS NOT NULL AND
                                  aj.Reacciones IS NOT NULL AND
                                  aj.Balance IS NOT NULL AND
                                  aj.Poder_de_tiro IS NOT NULL AND
                                  aj.Salto IS NOT NULL AND
                                  aj.Fuerza IS NOT NULL AND
                                  aj.Agresion IS NOT NULL AND
                                  aj.Intercepciones IS NOT NULL AND
                                  aj.Vision IS NOT NULL AND
                                  aj.Penales IS NOT NULL AND
                                  aj.Marcar IS NOT NULL
                        """)
#
# Segundo paso: Vinculo a Planteles como la suma de atributos de todos los jugadores de un plantel.
suma_atriplanteles = duckdb.sql("""SELECT CfP.ID_plantel, SUM(SA.Suma_Atributos) AS Suma_Atributos_Plantel
                                   FROM suma_atributos as SA, tabla_conformacion_plantel.csv AS CfP
                                   WHERE SA.ID_jugador = CfP.ID_jugador
                                   GROUP BY CfP.ID_plantel
                                """)

# Tercer paso: Obtengo los partidos (y sus goles) que jugaron en la tempoliga asignada.
# Resulta que primero debo obtener los equipos de la liga, así que busco eso también.
equiliga = duckdb.sql("""SELECT ID_equipo
                         FROM tabla_equipos.csv AS equipos
                         WHERE equipos.ID_liga = 24558
                      """)

partidos_tempoliga = duckdb.sql("""SELECT ID_partido, ID_Local, ID_Visitante, Goles_Local, Goles_Visitante, Temporada
                                   FROM tabla_partidos.csv AS partidos, equiliga
                                   WHERE (partidos.ID_local = equiliga.ID_equipo OR
                                         partidos.ID_visitante = equiliga.ID_equipo) AND
                                         (partidos.Temporada='2012/2013' OR
                                         partidos.Temporada='2013/2014' OR
                                         partidos.Temporada='2014/2015' OR
                                         partidos.Temporada='2015/2016')
                                """)

# Cuarto paso: Añado a partidos_tempoliga "Atributos_Local" y "Atributos_Visitante"
 # Resulta que para esto necesito primero dividir los partidos del "punto de vista" de cada equipo
partical_tempoliga = duckdb.sql("""SELECT plantel.ID_plantel AS ID_plantel, ID_partido, Goles_Local AS Goles, Suma_Atributos_Plantel
                                   FROM partidos_tempoliga, suma_atriplanteles, tabla_plantel.csv AS plantel
                                   WHERE partidos_tempoliga.ID_local = plantel.ID_equipo AND
                                   suma_atriplanteles.ID_plantel = plantel.ID_plantel
                                """)

partitante_tempoliga = duckdb.sql("""SELECT plantel.ID_plantel AS ID_plantel, ID_partido, Goles_Local AS Goles, Suma_Atributos_Plantel
                                      FROM partidos_tempoliga, suma_atriplanteles, tabla_plantel.csv AS plantel
                                      WHERE partidos_tempoliga.ID_visitante = plantel.ID_equipo AND
                                      suma_atriplanteles.ID_plantel = plantel.ID_plantel
                                   """)

 # Y luego unir las tablas 
partidos_atributados = duckdb.sql("""SELECT * FROM partical_tempoliga
                                               UNION
                                               SELECT * FROM partitante_tempoliga
                                            """)
partidos_atributados = partidos_atributados.df()

# Quinto paso: Obtengo [NombreEquipo - Partido - Goles - Atributos]
partipos_atributados =  duckdb.sql("""SELECT equipos.Nombre AS Nombre, pta.ID_partido, pta.Goles, pta.Suma_Atributos_Plantel
                                      FROM partidos_atributados as pta, tabla_equipos.csv AS equipos, tabla_plantel.csv AS plantel
                                      WHERE pta.ID_plantel = plantel.ID_plantel AND
                                      plantel.ID_equipo = equipos.ID_equipo
                                   """)

# Sexto paso: Obtengo [Equipo - SumaGoles/CantidadPartidos - Atributos]
equipos_atributos = duckdb.sql("""SELECT Nombre, (SUM(Goles)/COUNT(ID_partido)) AS Ratio_Goles, AVG(Suma_Atributos_Plantel) AS Suma_Atributos
                                    FROM partipos_atributados
                                    GROUP BY Nombre
                                 """)



pta = partidos_atributados
print(f"table_keys = {pta.columns}")
print(f"table_shape = {pta.shape}")

figure, ax = plt.subplots()

pta["Goles"].value_counts().plot.bar(ax = ax)

ax.set_title("Goles por Plantel-Partido")
ax.set_xlabel("Cantidad de Goles")
ax.set_ylabel("Cantidad de Planteles-Partidos con esa cantidad de goles")
ax.set_yticks([])
ax.bar_label(ax.containers[0], fontsize=8)
ax.tick_params(axis="x", labelrotation=0)

plt.show()


# Plot 3 - Ploteando [NombreEquipo - Partido - Goles - Atributos] de formas interesantes

distinct_teams = duckdb.sql("""SELECT DISTINCT Nombre
                            FROM partipos_atributados""").df()["Nombre"]

for nombre in distinct_teams:
    # Ploteo Cuantos Goles Metieron En Cuantos Partidos
    datos_de_equipo = duckdb.sql(f"""SELECT *
                                 FROM partipos_atributados
                                 WHERE Nombre = '{nombre}'
                                """)

    figure, ax = plt.subplots()
    datos_de_equipo.df()["Goles"].value_counts().plot.bar(ax = ax)
    ax.set_title(f"Goles metidos por partido - {nombre}")
    ax.set_xlabel("Goles")
    ax.set_ylabel("Partidos")
    ax.set_yticks([])
    ax.bar_label(ax.containers[0], fontsize=8)
    ax.tick_params(axis="x", labelrotation=0)
    plt.show()
