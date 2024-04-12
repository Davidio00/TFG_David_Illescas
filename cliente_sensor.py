import pandas as pd
import pickle
from sklearn.tree import DecisionTreeClassifier
import joblib
import csv
import asyncio
import websockets
import json
import os


# Configura el socket para conectarse al servicio SPE
host = "localhost"
port = 9999


#Ficheros necesarios para el modelo
fichero_entrenamiento="./medidas_entrenamiento.csv"
fichero_modelo="./modelo.pkl"
fichero_salida="./resultados.csv"
fichero_evaluacion = "./medidas_evaluacion.csv"

#Diccionario para almacenar todas las medidas
medidas = {}

#Método utilizado para entrenar al modelo
def entrenar():
    
    # Cargar los datos desde el archivo CSV
    data = pd.read_csv(fichero_entrenamiento)

    # Dividir los datos en características (X) y resultados (y)
    X = data[['Humedad_aire', 'VWC']]
    y = data['Estado']

    # Crear una instancia del clasificador de árbol de decisión
    clf = DecisionTreeClassifier()
    
    # Entrenar el modelo utilizando los datos de entrenamiento
    clf.fit(X.values, y.values)
    
    # Cargar los datos de evaluación desde el archivo CSV
    data = pd.read_csv(fichero_entrenamiento)

    # Dividir los datos en características (X) y resultados (y)
    X_ev = data[['Humedad_aire', 'VWC']]
    y_ev = data['Estado']
    
    #Evaluamos la precisión del modelo entrenado antes
    precision = clf.score(X_ev.values, y_ev.values)
    print(precision)
    # Guardar el modelo en un archivo pkl
    with open(fichero_modelo, 'wb') as file:
        pickle.dump(clf, file)

# Función para cargar el modelo desde un archivo pkl y analizar si es necesario el riego o no
def set(med):
    
    for dispositivo, datos in med.items():
        
        for timestamp, valores in datos.items():
            if "hum" in valores and "vwc" in valores:
                #Se obtienen los valores relevantes para el algoritmo
                hum = valores["hum"]
                vwc = valores["vwc"]
                
                #Cargamos el modelo para utilizarlo
                modelo_entrenado = joblib.load(fichero_modelo)
                
                #Convertimos las medidas para que tengan el formato de entrada del modelo
                medidas = [[hum, vwc]]
                
                #Se usa el método predict para predecir si es necesario el riego o no 
                prediccion = modelo_entrenado.predict(medidas)
                
                #Todas las medidas se almacenan en un fichero con la predicción
                guardar(prediccion, hum, vwc)
        


#Método para almacenar el estado del riego
def guardar(estado, humedad_aire, vwc):
    with open(fichero_salida, "a") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([humedad_aire, vwc, estado])

#Método para obtener los datos de relevancia de todos los registros recibidos
def obtener_datos(mensaje):
    
    if mensaje['name'] == 'obs_volumetric_water_content_mineral_soil':
        vwc = mensaje['value']
        time = mensaje['time']
        entityName = mensaje['entityName']
        
        if entityName in medidas:
            if not time in medidas[entityName]:
                medidas[entityName][time] = {}
                medidas[entityName][time]["vwc"] = vwc
            else:
                if not "vwc" in medidas[entityName][time]:
                    if not "hum" in medidas[entityName][time]:
                        medidas[entityName][time] = {}
                        medidas[entityName][time]["vwc"] = vwc
                    else:
                        medidas[entityName][time]["vwc"] = vwc
        else:
            medidas[entityName] = {}
            medidas[entityName][time] = {}
            medidas[entityName][time]["vwc"] = vwc
        
    elif mensaje['name'] == 'obs_air_humidity':
        hum = mensaje['value']
        time = mensaje['time']
        entityName = mensaje['entityName']
    
        if entityName in medidas:
            if not time in medidas[entityName]:
                medidas[entityName][time] = {}
                medidas[entityName][time]["hum"] = hum
            else:
                if not "hum" in medidas[entityName][time]:
                    if not "vwc" in medidas[entityName][time]:
                        medidas[entityName][time] = {}
                        medidas[entityName][time]["hum"] = hum
                    else:
                        medidas[entityName][time]["hum"] = hum
        else:
            medidas[entityName] = {}
            medidas[entityName][time] = {}
            medidas[entityName][time]["hum"] = hum


#Método que tiene la lógica del cliente de conectarse al servidor y recibir los mensajes
async def enviar(mensaje):
    
    #Se crea la url con el host y el puerto indicados
    url = "ws://" + host + ":" + str(port)
        
    #Se conecta el cliente al servidor
    websocket = await websockets.connect(url, ping_interval=5000)
    
    #Se envía la información requerida al SPE
    await websocket.send(json.dumps(mensaje))
    
    mensaje = ""
    
    while True:
        try:
            #Se reciben los mensajes del SPE
            mensaje = await websocket.recv()
            obtener_datos(json.loads(mensaje))
        except Exception as e:
            # Capturar la excepción del cierre del websocket por parte del servidor
            print("Se produjo una excepción:", str(e))
            break
    print(medidas)
    #Se aplica el modelo a cada mensaje recibido
    set(medidas)



#Método principal del programa
if __name__ == '__main__':
    
    #Ruta al archivo del modelo
    file_path = fichero_modelo
    
    #Mensaje de ejemplo para solicitar datos de sensores
    # mensaje = {"modo": "offline",
    #         "tipo_analisis":"historic", 
    #         "name": "obs_wind_speed",
    #         "start_time": "2020-06-09",
    #         "end_time": "2023-11-27",
    #         "entityNames":"UPM_weatherD101690",
    #         "services":"environmentalObservations",
    #         "providers":"UPM",
    #         "types":"weatherStation"
    #         }
    mensaje = {
    "modo": "offline",
    "tipo_analisis":"latest",
    "start_time": "2020-06-09",
    #"end_time": "2023-11-27",
    #"entityNames":"afc_node_0205_24",
    #"services":"environmentalObservations",
    #"providers":"UPM",
    #"types":"weatherStation"
    }
    
    #Se comprueba si hay un modelo ya entrenado o no
    if os.path.exists(file_path):
        print("Modelo ya entrenado")
    else:
        #Si el modelo no esta entrenado se entrena antes de empezar con el funcionamiento
        entrenar()
        
    #Se ejecuta el cliente
    asyncio.run(enviar(mensaje))
