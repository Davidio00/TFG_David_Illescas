import random
import csv

#Método para determinar si dos medidas de humedad para determinar si es necesario riego o no
#Resultado = 0 --> es necesario el riego
#Resultado = 1 --> el estado es óptimo
#Resultado = 2 --> demasiada agua en los cultivos
def estado_cultivo(humedad_aire, vwc):
    respuesta = 0
    # Evaluar las condiciones de humedad
    if 50 <= humedad_aire <= 70 and 35 <= vwc <= 55:
        respuesta = 1
    elif 40 <= humedad_aire <= 50 and 35 <= vwc <= 55:
        respuesta = 1
    elif 15 <= vwc <= 35 and 50 <= humedad_aire <= 70:
        respuesta = 1
    elif humedad_aire < 40 and vwc < 15:
        respuesta = 0
    elif humedad_aire > 70 and vwc > 55:
        respuesta = 2
    elif humedad_aire < 40 and 35 <= vwc <= 55:
        respuesta = 1
    elif vwc <= 15 and 50 <= humedad_aire <= 70:
        respuesta = 1
    elif 50 <= humedad_aire <= 70 and vwc > 55:
        respuesta = 2
    elif 35 <= vwc <= 55 and humedad_aire > 70:
        respuesta = 2
    elif vwc < 15 and humedad_aire > 70:
        respuesta = 1
    elif vwc <= 15 and 40 <= humedad_aire <= 50:
        respuesta = 0
    elif humedad_aire < 40 and vwc > 55:
        respuesta = 1
    elif humedad_aire < 40 and 15 <= vwc <= 35:
        respuesta = 0
    elif vwc > 55 and humedad_aire < 50:
        respuesta = 2
    elif humedad_aire > 70 and vwc < 35:
        respuesta = 2
    elif 40 < humedad_aire < 50 and 15 < vwc < 35:
        respuesta = 0

        
    return respuesta

# Generar posiciones aleatorias y comprobar si están dentro del área
num_puntos = 100000
medidas= []

for _ in range(num_puntos):
    # Generar medidas aleatorias
    humedad_aire = random.randint(1, 100)
    vwc = random.randint(1, 100)
    
    # Comprobar el estado del cultivo
    estado = estado_cultivo(humedad_aire, vwc)
    # Se añaden las medidas y el estado
    medidas.append((humedad_aire, vwc, estado))

# Cuando se terminan de crear las medidas se almacenan en un fichero que servirá de entrenamiento
with open('medidas_entrenamiento.csv', 'w', newline='') as archivo_csv:
    writer = csv.writer(archivo_csv)
    writer.writerow(['Humedad_aire', 'VWC', 'Estado'])
    writer.writerows(medidas)

