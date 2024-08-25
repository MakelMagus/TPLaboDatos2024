import numpy as np
import csv


def cambia_ceros(M, e):
    
    for i in range(len(M)):
        for j in range(len(M[i])):
            if(M[i][j]== e):
                M[i][j] = 0
                
    return M

# miarray = np.array([[1,2,3,4],[5,6,3,3],[3,3,33,4],[3,3,3,1]])

# cambia_ceros(miarray, 3)
# print(miarray)

# a = np.array([[1,2,8],[8,7,9]])
# print(a)

def leer_parque(archivo, parque):
    
    lista_de_dicts = []
    
    
    with open(archivo, 'rt', encoding = 'utf-8') as df:
        # planilla = csv.reader(df)
        encabezado = next(df)
        encabezado = encabezado.split(',')
            
        for fila in df:
            datos_fila = fila.split(',')
            if parque in datos_fila:
                diccionario = {}
                for i in range(len(encabezado)):    
                    
                    diccionario[encabezado[i]] = datos_fila[i]
                
                lista_de_dicts.append(diccionario)
                    
    return lista_de_dicts

def especie(lista_de_arboles:list):
    conjunto_especies:set = set()
    
    for i in range(len(lista_de_arboles)):
        conjunto_especies.add(lista_de_arboles[i]['nombre_com'])
    
    return conjunto_especies

lista_de_pc = leer_parque('arbolado-en-espacios-verdes.csv', "CENTENARIO")
# print(especie(lista_de_generalpaz))
    
def contar_ejemplares(lista_arboles):
    diccionario = {}
    lista_con_nombres_repetidos = []
    for i in range(len(lista_arboles)):
        lista_con_nombres_repetidos.append(lista_arboles[i]['nombre_com'])
        
    for arbol in lista_con_nombres_repetidos:   #esta forma de iterar sobre listas pareciera que no contempla repetidos
        diccionario[arbol] = lista_con_nombres_repetidos.count(arbol)
        
    return diccionario

def obtener_alturas(lista_arboles, especie):
    alturas = []
    for i in range(len(lista_arboles)):
        if lista_arboles[i]['nombre_com'] == especie:
            altura = float(lista_arboles[i]['altura_tot'])
            alturas.append(altura)
    return alturas

# print(obtener_alturas(lista_de_generalpaz, 'Jacarandá'))

# altura_promedio_Jacaranda = sum(obtener_alturas(lista_de_generalpaz, 'Jacarandá'))/len(obtener_alturas(lista_de_generalpaz, 'Jacarandá'))
# print(altura_promedio_Jacaranda)
                          
def obtener_inclinaciones(lista_arboles, especie):
    inclinaciones = []
    for i in range(len(lista_arboles)):
        if lista_arboles[i]['nombre_com'] == especie:
            inclinacion = float(lista_arboles[i]['inclinacio'])
            inclinaciones.append(inclinacion)
    return inclinaciones

def especimen_mas_inclinado(lista_arboles):
    conjunto_de_especies = especie(lista_arboles)
    diccionario = {}
    
    for arbol in conjunto_de_especies:
        
        inclinacion_maxima_arbol = max(obtener_inclinaciones(lista_arboles, arbol))
        diccionario[inclinacion_maxima_arbol] = arbol
    
    lista_claves = []
    for claves in diccionario:
        lista_claves.append(claves)
        
    mayor_inclinacion = max(lista_claves)
    return f"El arbol con mayor inclinacion es el {diccionario[mayor_inclinacion]} y su inclinacion es de {mayor_inclinacion} grados"

print(especimen_mas_inclinado(lista_de_pc))
        
        
    
        
                
                
        