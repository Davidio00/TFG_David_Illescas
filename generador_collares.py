import pandas as pd
from kafka import KafkaProducer
import json
import time

# Leer el archivo CSV y almacenar su contenido en un DataFrame
df = pd.read_csv('collars_time_date.csv')

#Dirección y puerto del servidor de Kafka al que se enviarán los collares
bootstrap_servers_SPE = "kafka1:9092"

#Topic general del Kafka AFarCloud al que se envían los collares
topic = "afar_telemetries_rest"

#Se crea un productor para enviar los datos al Kafka seleccionado
producer = KafkaProducer(bootstrap_servers=bootstrap_servers_SPE)

# Recorrer el DataFrame línea por línea, crear un JSON con los valores de cada línea y enviarlo con el Producer
for index, row in df.iterrows():
    #Se convierte cada fila del dataFrame en un directorio
    data = row.to_dict()
    
    #Los mensajes de collares según los estándares de AFarCloud deben tener un campo resourceID
    #Este campo se crea concatenando los datos relevantes de cada collar con ":"
    #Los mensajes de collares se crean directamente de cada fila del fichero pero este campo hay
    #que crearlo de forma manual y añadirlo al JSON.
    data["resourceId"] ="afar:" + data['provider'] + ":" + data['service'] + ":" + data['name'] + ":" + data['entityName']
    
    #Se codifica y formatean los datos para enviarlos
    mensaje_codificado = json.dumps(data).encode('utf-8')
    producer.send(topic, mensaje_codificado)
    time.sleep(1)
