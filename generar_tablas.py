import duckdb
import pandas as pd
import xml.etree.ElementTree as ET

# Función para convertir XML a diccionario
def xml_to_dict(root):
    # Parsear el string XML
    element = ET.fromstring(root)

    return xml_to_dict_recursive(element)

def xml_to_dict_recursive(element):
    result = {}
    if element.tag == "goal":
        result["values"] = []  # Inicializa la lista de valores
        for value in element.findall('value'):
            value_dict = xml_to_dict_recursive(value)  # Convierte cada valor a dict
            result["values"].append(value_dict)  # Agrega el dict a la lista
    else:
        for child in element:
            # Llamar recursivamente si hay más hijos
            result[child.tag] = xml_to_dict_recursive(child) if len(child) > 0 else child.text
    return result

#tabla paises 
pais = duckdb.sql('''SELECT name AS Nombre FROM enunciado_paises.csv''')
df_pais = pais.df()
df_pais.to_csv('tabla_pais.csv', index=False)

#tabla jugador
jugador = duckdb.sql('''SELECT player_api_id AS ID, player_name AS Nombre FROM enunciado_jugadores.csv''')
df_jugador = jugador.df()
df_jugador.to_csv('tabla_jugador.csv',index=False)

#tabla temporada, el id fue creado con pandas
temporada = duckdb.sql('''
                       SELECT DISTINCT season AS Temporada 
                       FROM enunciado_partidos.csv
                       ''')
df_temporada = temporada.df()         
df_temporada.to_csv('tabla_temporada.csv', index=False)

# tabla liga, su correcto ID fue obtenido a pertir del country_id del partido
liga = duckdb.sql('''
                  SELECT DISTINCT l.name AS Nombre_liga, p.name AS Nombre_pais, part.league_id AS ID
                  FROM enunciado_liga.csv AS l
                  INNER JOIN enunciado_paises.csv AS p ON l.country_id = p.id
                  INNER JOIN enunciado_partidos.csv AS part ON l.country_id = part.country_id
                  ''')

df_liga = liga.df()
df_liga.to_csv('tabla_liga.csv', index=False)

# Tabla Equipos
equipo = duckdb.sql('''
                    SELECT DISTINCT eq.team_api_id AS ID_equipo, eq.team_long_name AS Nombre, par.league_id AS ID_liga
                    FROM enunciado_equipos.csv AS eq
                    INNER JOIN enunciado_partidos.csv AS par 
                    ON eq.team_api_id = par.home_team_api_id
                    ''')

df_equipo = equipo.df()
#EXPLICACION: basicamente hice el join en base a la liga donde ese equipo jugó de local, podria pasar que
# según lo que estuve viendo no, porque me unió a todos los equipos
df_equipo.to_csv('tabla_equipos.csv', index=False)

tabla_goles = duckdb.sql('''
                      SELECT DISTINCT match_api_id AS ID_Partido, goal AS Goles
                      FROM enunciado_partidos.csv
                      ''')

df_goles_raw = tabla_goles.df().dropna(subset=['Goles'])



def dict_goles_to_listas(set_de_goles:dict):
    lista_de_goles = []
    for gol in set_de_goles["values"]:
        # if counter < 8: print(f"{counter} - From:\n{gol}")
        id_gol = int(gol["id"])
        if "player1" in gol.keys():
            id_jugador = str(gol["player1"])
        else:
            print(f"The player is missing here:\n{gol}")
            id_jugador = None

        if "stats" in gol.keys():
            stats = gol["stats"]
            if ("goals" in stats.keys()) and ("shoton" in stats.keys()):
                tipo = "normal"
            elif ("goals" in stats.keys()) and ("penalty" in stats.keys()):
                tipo = "penal en juego"
            elif ("penalties" in stats.keys()):
                tipo = "penal por desempate"
            elif ("owngoals" in stats.keys()):
                tipo = "contragol"
            else:
                tipo = "unknown"
                print(f"Los stats son {stats}")
        else:
            tipo = "-"
        

        if "subtype" in gol.keys():
            subtipo = gol["subtype"]
        else:
            subtipo = "-"
        
        lista_de_goles.append([id_gol, id_jugador, tipo, subtipo])
    
    return lista_de_goles


df_goles_raw["Goles"] = df_goles_raw["Goles"].apply(xml_to_dict)
df_goles_raw["Goles"] = df_goles_raw["Goles"].apply(dict_goles_to_listas)

# Busco transformar df_goles_raw en df_goles, que tiene, en cada linea:
# ID_Gol, ID_Partido, ID_Jugador, Tipo, Subtipo
# A partir de df_goles_raw, que tiene, en cada linea:
# ID_Partido, [Lista de datos x gol]
tabla_final = []

for i in range(1, len(df_goles_raw)):
    try:
        id_partido = df_goles_raw["ID_Partido"][i]
        for gol in df_goles_raw["Goles"][i]:
            tabla_final.append([gol[0], id_partido, gol[1], gol[2], gol[3]])
    except:
        pass
        #print(f"Fila {i} no existe.")

df_goles = pd.DataFrame(tabla_final)
df_goles = df_goles.rename(columns={0: 'ID_Gol', 1:'ID_Partido', 2:'ID_Jugador', 3: 'Tipo', 4:'Subtipo'})

df_goles.to_csv("tabla_goles.csv", index=False)

# Tabla Partidos
partidos = duckdb.sql('''
                      SELECT DISTINCT match_api_id AS ID_partido,league_id as ID_liga, season AS Temporada, date AS Fecha, home_team_api_id AS ID_local, away_team_api_id AS ID_visitante, home_team_goal AS Goles_local, away_team_goal AS Goles_visitante
                      FROM enunciado_partidos.csv
                      ''')
partidos = partidos.df()

partidos.to_csv('tabla_partidos.csv', index =False)

resultado = duckdb.sql('''
                      SELECT DISTINCT Goles_local, Goles_visitante,
                      CASE WHEN Goles_local > Goles_visitante THEN 'Ganado'
                      WHEN Goles_visitante > Goles_local THEN 'Perdido'
                      ELSE 'Empatado'
                      END AS Resultado,
                      FROM tabla_partidos.csv
                      ''')
resultado.df().to_csv('tabla_resultado.csv', index=False)

#Tabla Plantel
plantel = duckdb.sql('''
                     SELECT DISTINCT Temporada, ID_local AS ID_equipo
                     FROM tabla_partidos.csv 
                     ''')

df_plantel = plantel.df()
df_plantel['ID_plantel'] = range(0,len(df_plantel),1)
df_plantel.to_csv('tabla_plantel.csv', index=False)

# Conformación plantel
conf_plantel = duckdb.sql('''
                          SELECT DISTINCT ID_plantel, home_player_1 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, home_player_2 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, home_player_3 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, home_player_4 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, home_player_5 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, home_player_6 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, home_player_7 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, home_player_8 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, home_player_9 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, home_player_10 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, home_player_11 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND home_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_1 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_2 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_3 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_4 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_5 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_6 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_7 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_8 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_9 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_10 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          UNION
                          SELECT DISTINCT ID_plantel, away_player_11 AS ID_jugador
                          FROM enunciado_partidos.csv, tabla_plantel.csv
                          WHERE season = Temporada AND away_team_api_id = ID_equipo
                          ''')



# una vez que hago todos estos simplemente hago un join y listo
conformacion_plantel = conf_plantel.df()
conformacion_plantel = conformacion_plantel.dropna(subset=['ID_jugador']) # acá borré los nulls para poder aplicar la funcion para sacar ese id.0 que aparece en todos los jugadores de partidos
conformacion_plantel['ID_jugador'] = conformacion_plantel['ID_jugador'].apply(lambda x: int(x)) # saco esos .0
conformacion_plantel.to_csv('tabla_conformacion_plantel.csv',index=False)

# agarro la tabla plantel y digo: si en partidos coincide el season con id_temporada Y local con equipo SELECT id_plantel con id_home player

atributos_jugador = duckdb.sql("""SELECT
                               player_api_id AS ID_jugador,
                               date AS Fecha,
                               overall_rating AS Puntos_Generales,
                               potential AS Potencial,
                               preferred_foot AS Pie_Preferido,
                               crossing AS Cruce,
                               finishing AS Finalizacion,
                               dribbling AS Dribbling,
                               free_kick_accuracy AS Precision_Patada_libre,
                               ball_control AS Control_Pelota,
                               acceleration AS Aceleracion,
                               sprint_speed AS Velocidad_Corriendo,
                               agility AS Agilidad,
                               reactions AS Reacciones,
                               balance AS Balance,
                               shot_power AS Poder_de_Tiro,
                               jumping AS Salto,
                               strength AS Fuerza,
                               aggression AS Agresion,
                               interceptions AS Intercepciones,
                               vision AS Vision,
                               penalties AS Penales,
                               marking AS Marcar
                               FROM enunciado_jugadores_atributos.csv
                               """)

def decidir_temporada(fecha:str):
    fecha = str(fecha)
    fecha = fecha.split("-") #Año - Mes - Día

    match fecha[0]:
        case "2007":
            return "2007/2008"
        case "2008":
            if int(fecha[1]) <= 7:
                return "2007/2008"
            else:
                return "2008/2009"
        case "2009":
            if int(fecha[1]) <= 7:
                return "2008/2009"
            else:
                return "2009/2010"
        case "2010":
            if int(fecha[1]) <= 7:
                return "2009/2010"
            else:
                return "2010/2011"
        case "2011":
            if int(fecha[1]) <= 7:
                return "2010/2011"
            else:
                return "2011/2012"
        case "2012":
            if int(fecha[1]) <= 7:
                return "2011/2012"
            else:
                return "2012/2013"
        case "2013":
            if int(fecha[1]) <= 7:
                return "2012/2013"
            else:
                return "2013/2014"
        case "2014":
            if int(fecha[1]) <= 7:
                return "2013/2014"
            else:
                return "2014/2015"
        case "2015":
            if int(fecha[1]) <= 7:
                return "2014/2015"
            else:
                return "2015/2016"
        case "2016":
            return "2015/2016"




df_atributos_jugador = atributos_jugador.df()
df_atributos_jugador["Temporada"] = df_atributos_jugador["Fecha"].apply(decidir_temporada)
df_atributos_jugador.to_csv("tabla_atributos_jugador.csv", index=False)