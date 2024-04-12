import pandas as pd
import pickle
from sklearn.tree import DecisionTreeClassifier
import joblib
import csv
import mapa
import asyncio
import websockets
import json
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
import time

#Para acceder a las variables de entorno
load_dotenv()

#Se especifica el servidor de Kafka de AFarCloud para poder generar alarmas
bootstrap_servers_AFC = os.getenv("bootstrap_servers_AFC")

# Configura el socket para conectarse al servicio SPE
host = os.getenv("host")
port = int(os.getenv("port"))

#Topic general de AFarCloud para generar alarmas
topic_online = os.getenv("topicOnline")

#Ficheros necesarios para el modelo
fichero_entrenamiento=os.getenv("fichero_entrenamiento")
fichero_modelo=os.getenv("fichero_modelo")
fichero_salida=os.getenv("fichero_salida")
fichero_evaluacion = os.getenv("fichero_evaluacion")

#Método utilizado para entrenar al modelo
def entrenar():
    
    # Cargar los datos desde el archivo CSV
    data = pd.read_csv(fichero_entrenamiento)

    # Dividir los datos en características (X) y resultados (y)
    X = data[['Latitud', 'Longitud']]
    y = data['Dentro']

    # Crear una instancia del clasificador de árbol de decisión
    clf = DecisionTreeClassifier()
    
    # Entrenar el modelo utilizando los datos de entrenamiento
    clf.fit(X.values, y.values)

    
    # Cargar los datos de evaluación desde el archivo CSV
    data = pd.read_csv(fichero_entrenamiento)

    # Dividir los datos en características (X) y resultados (y)
    X_ev = data[['Latitud', 'Longitud']]
    y_ev = data['Dentro']
    
    #Evaluamos la precisión del modelo entrenado antes
    precision = clf.score(X_ev.values, y_ev.values)
    print(precision)

    # Guardar el modelo en un archivo pkl
    with open(fichero_modelo, 'wb') as file:
        pickle.dump(clf, file)

# Función para cargar el modelo desde un archivo pkl y predecir si unas coordenadas están dentro o fuera
def set(mensaje):
    #Obtenemos del mensaje las coordenadas
    latitud = mensaje['latitude']
    longitud = mensaje['longitude']
    
    #Cargamos el modelo para utilizarlo
    modelo_entrenado = joblib.load(fichero_modelo)
    
    #Convertimos las coordenadas para que tengan el formato de entrada del modelo
    coordenadas = [[latitud, longitud]]
    
    #Se usa el método predict para predecir si están dentro o fuera las coordenadas 
    prediccion = modelo_entrenado.predict(coordenadas)
    
    #Con la predicción se comprueba si esta dentro o no (siendo 0 fuera)
    if prediccion[0]==0:
        
        #Si el animal esta fuera se llama al método para crear una alarma
        crear_alarma(mensaje)
    
    #Todas las coordenadas, nombres de entidad y predicciones se almacenan en un fichero
    guardar(prediccion, latitud, longitud, mensaje['entityName'])
    


#Método para almacenar todas las predicciones
def guardar(predic, latitud, longitud,entityName):
    with open(fichero_salida, "a") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([latitud, longitud, predic,entityName])

#Método que tiene la lógica del cliente de conectarse al servidor y recibir los mensajes
async def enviar(mensaje):
    
    #Se crea la url con el host y el puerto indicados
    url = "ws://" + host + ":" + str(port)
        
    #Se conecta el cliente al servidor
    websocket = await websockets.connect(url, ping_interval=5000)
    
    #Se envía la información requerida al SPE
    await websocket.send(json.dumps(mensaje))
    
    message = ""
    
    while True:
        #Se reciben los mensajes del SPE
        message = await websocket.recv()
        #Se aplica el modelo a cada mensaje recibido
        set(json.loads(message))

    
#Método para crear una alarma en caso de que un animal se salga del recinto
def crear_alarma(mensaje):
    #Se crea un productor del Kafka de AFarCloud
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers_AFC)
    
    #Mensaje JSON de alarma para enviar
    mensaje_alarma={
      'alarmCode':'COW_OUT_PADDOCK',
      'message':'One cow is out of the paddock',
      'resourceId':mensaje['entityName'],
      'alarmTime': time.time(),
      'priority': 'low',
      'status': '0'
    }
    
    #Se envía el mensaje por el topic correspondiente
    producer.send(topic_online, json.dumps(mensaje_alarma).encode('utf-8'))
    
    #Se cierra el productor
    producer.close()


#Método principal del programa
if __name__ == '__main__':
    
    #Ruta al archivo del modelo
    file_path = fichero_modelo
    
    #Mensaje de ejemplo para solicitar datos de collares
    mensaje = {"modo": "online",
               "name": "collar",
               "entityNames":"AH192",
               "services":"livestockManagement",
               "providers":"SENSO",
               "types":"collar"
               }
    
    #Se comprueba si hay un modelo ya entrenado o no
    if os.path.exists(file_path):
        print("Modelo ya entrenado")
    else:
        #Si el modelo no esta entrenado se entrena antes de empezar con el funcionamiento
        entrenar()
        
    #Se ejecuta el cliente
    asyncio.run(enviar(mensaje))















