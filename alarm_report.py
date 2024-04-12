from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import mysql.connector
from dotenv import load_dotenv
import os

#Para acceder a las variables de entorno
load_dotenv()

#Variables de entorno 
#Variable topic del kafka de AFC
topicAfar = os.getenv("topicAfar")

#Variable de servidor Kafka SPE
bootstrap_servers_SPE = os.getenv("bootstrap_servers_SPE")

#Variable de servidor Kafka AFarCloud
bootstrap_servers_afar = os.getenv("bootstrap_servers_afar")

#Direccion de la base de datos
ip_db = os.getenv("ip_db")

#Puerto de la base de datos
puerto = int(os.getenv("puerto"))

#Topic para el SPE
topicAlarm = os.getenv("topicAlarm")

#Usuario para acceder a la base de datos
user = os.getenv("user")

#Nombre de la base de datos
database = os.getenv("database")

#Contraseña para acceder a la base de datos
password = os.getenv("password")

def escribir_db(mensaje):
    # Establece una conexión con la base de datos
    conn = mysql.connector.connect(
    host=ip_db,
    port=puerto,
    user=user,
    password=password,
    database=database
    )

    # Crea un cursor para ejecutar consultas SQL
    cursor = conn.cursor()
    
    #Comprobar si se trata de un sensor o un UAV
    if "alarmCode" in mensaje:
        #Extrae del mensaje recibido los datos relevantes para almacenar
        datos = obtener_datos(mensaje)
        # Crea una consulta SQL
        sql_query = "INSERT INTO alarms (alarmCode, message, resourceID, sequenceNumber, alarmTime, source, priority, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        # Crea una lista de valores
        values = (datos['alarmCode'], datos['message'], datos['resourceId'], datos['sequenceNumber'], datos['alarmTime'], datos['source'], datos['priority'], datos['status'])
    
    elif "alarm_code_id" in mensaje:
        #Para el caso de los UAV no hay campos opcionales por lo que no es necesario usar la función obtener_datos()
        # Crea una consulta SQL
        sql_query = "INSERT INTO alarms (alarmCode, message, resourceID, sequenceNumber, param[0], param[1], param[2]) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        # Crea una lista de valores
        values = (mensaje['alarm_code_id'], mensaje['alarm_type_id'], mensaje['vehicle_Id'], mensaje['sequence_number'],mensaje['param[0]'],mensaje['param[1]'],mensaje['param[2]'] )

    # Imprime la consulta SQL y los valores
    print("SQL Query:", sql_query)
    print("Values:", values)
    
    # Ejecuta una consulta SQL
    cursor.execute(sql_query, values)
    
    #Guardar los cambios
    conn.commit()

    # Cierra la conexión con la base de datos
    conn.close()
    
#Método para obtener del mensaje del cliente los datos para almacenar y rellenar en caso de que no estén todos
def obtener_datos(mensaje):
    datos={}
    if 'alarmCode' in mensaje:
        datos['alarmCode'] = mensaje['alarmCode']
    if 'message' in mensaje:
        datos['message'] = mensaje['message']
    if 'resourceId' in mensaje:
        datos['resourceId'] = mensaje['resourceId']
    if 'sequenceNumber' in mensaje:
        datos['sequenceNumber'] = mensaje['sequenceNumber']
    else:
        datos['sequenceNumber'] = None
    if 'alarmTime' in mensaje:
        datos['alarmTime'] = mensaje['alarmTime']
    else:
        datos['alarmTime'] = None
    if 'source' in mensaje:
        datos['source'] = mensaje['source']
    else:
        datos['source'] = None
    if 'priority' in mensaje:
        datos['priority'] = mensaje['priority']
    else:
        datos['priority'] = None
    if 'status' in mensaje:
        datos['status'] = mensaje['status']
    else:
        datos['status'] = 0
    return datos


if __name__ == '__main__':
    
    #Se crea el producer que se conectará al kafka del SPE para el análisis de las alarmas
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers_SPE)
    
    #Se crea el consumer que se conectará al kafka de AFarCloud
    consumer = KafkaConsumer(topicAfar, bootstrap_servers = bootstrap_servers_afar)
    
    for mensaje in consumer:
        #Decodificamos el mensaje y lo cargamos como un JSON
        mensaje_decodificado = json.loads(mensaje.value.decode('utf-8'))
        
        #Comprobamos que sea un mensaje de alarma comprobando su campo de codigo
        if 'alarmCode' in mensaje_decodificado:
            #Escribimos la alarma en la base de datos
            escribir_db(mensaje_decodificado)
            
            #Enviamos la alarma al SPE
            producer.send(topicAlarm, json.dumps(mensaje_decodificado).encode('utf-8'))
            
        elif 'alarm_code_id' in mensaje_decodificado:
            #Escribimos la alarma en la base de datos
            escribir_db(mensaje_decodificado)

            #Enviamos la alarma al SPE
            producer.send(topicAlarm, json.dumps(mensaje_decodificado).encode('utf-8'))
         
        
        
    #Cerramos la conexión al terminar para liberar recursos
    consumer.close()
    producer.close()
