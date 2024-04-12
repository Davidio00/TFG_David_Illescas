
from kafka import KafkaProducer
import json
import random
import time

#Dirección y puerto del servicio Kafka para enviar las alarmas generadas
bootstrap_servers_SPE = "kafka1:9092"

#Topic general al que se envían las alarmas para que el Alarm Report las pueda gestionar
topicAlarm = "afar_telemetries_rest"

#Se crea un producer para enviar datos al Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers_SPE)

#Se crean tres posibles alarmas a modo de ejemplo con distintos alarmCode
#Alarma de problema en una de las válvulas de riego 
mensaje1 = {
            "alarmCode": "GREENHOUSE_IRRIGATION_FLOW", 
            "message": "irrigation valve is open but no water flow", 
            "resourceId":"este_irrigation_valve_0",
            "sequenceNumber":"123131",
            "alarmTime":"1612951908",
            "source":"sensor 1",
            "priority":"low",
            "status":"0"
            }

#Alarma de problema con la puerta del almacen
mensaje2 = {
            "alarmCode": "WAREHOUSE_DOOR_PROBLEM", 
            "message": "the door can not be open", 
            "resourceId":"este_warehouse_door_0",
            "sequenceNumber":"158569",
            "alarmTime":"1612951908",
            "source":"sensor 67",
            "priority":"low",
            "status":"0"
            }

#Alarma de animal que se ha salido del recinto
mensaje3={
            'alarmCode':'COW_OUT_PADDOCK',
            'message':'One cow is out of the paddock',
            'resourceId':'AH192',
            'alarmTime': '1612951908',
            'priority': 'low',
            'status': '0'
            }

while True:
    #Se elige el mensaje a enviar de forma aleatoria
    numero = random.randint(0, 2)
    if numero == 1:
        mensaje = mensaje1
    if numero == 2:
        mensaje = mensaje2
    if numero == 0:
        mensaje = mensaje3
    
    #Se envía el mensaje a través del Kafka
    mensaje_codificado = json.dumps(mensaje).encode('utf-8')
    producer.send(topicAlarm, mensaje_codificado)
    
    #Se genera un tiempo de espera aleatorio para esperar a enviar la siguiente alarma
    #Esta espera se hace con el objetivo de emular un entorno real donde las alarmas
    #se generan de forma uniforme a lo largo del tiempo
    espera = random.randint(0, 200)
    time.sleep(espera)

