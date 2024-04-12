import random
import csv

# Coordenadas de los cuatro puntos
punto1 = (40.687049, -4.546216)
punto2 = (40.687538, -4.523445)
punto3 = (40.700389, -4.523355)
punto4 = (40.696423, -4.543692)

#Se crea una lista con los puntos
poligono = [
    (40.687049, -4.546216),
    (40.687538, -4.523445),
    (40.700389, -4.523355),
    (40.696423, -4.543692)
]

#Método para determinar si un punto se encuentra dentro del poligono a partir de las intersecciones
def esta_dentro(lat, lon, poligono):
    
    # Inicializar el contador de intersecciones
    intersecciones = 0

    # Recorrer todos los bordes del polígono
    for i in range(len(poligono)):
        punto1 = poligono[i]
        punto2 = poligono[(i + 1) % len(poligono)]

        # Verificar si la línea imaginaria intersecta con el borde del polígono
        if ((punto1[1] > lon) != (punto2[1] > lon)) and (lat < (punto2[0] - punto1[0]) * (lon - punto1[1]) / (punto2[1] - punto1[1]) + punto1[0]):
            intersecciones += 1

    # Verificar si el número de intersecciones es impar
    if intersecciones % 2 == 1:
        return True
    else:
        return False

# Generar posiciones aleatorias y comprobar si están dentro del área
num_puntos = 100000
puntos= []

for _ in range(num_puntos):
    # Generar una latitud y longitud aleatoria pero con limite como máximo 5Km lejos del polígono
    lat = round(random.uniform(40.690000000, 40.700000000), 9)
    lon = round(random.uniform(-4.530000000, -4.550000000), 9)
    
    # Comprobar si el punto está dentro del área
    if esta_dentro(lat, lon, poligono):
        #Si el punto está dentro se añade a la lista de puntos con un 1
        puntos.append((lat, lon, 1))
    else:
        #Si el punto no está dentro se añade a la lista de puntos con un 0
        puntos.append((lat, lon, 0))

#Cuando se terminan de crear los puntos se almacenan en un fichero que servirá de entrenamiento
with open('puntos.csv', 'w', newline='') as archivo_csv:
    writer = csv.writer(archivo_csv)
    writer.writerow(['Latitud', 'Longitud', 'Dentro'])
    writer.writerows(puntos)
