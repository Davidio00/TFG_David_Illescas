
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import KafkaAdminClient
from kafka.admin import KafkaAdminClient
import socket
import csv
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import requests

#Para acceder a las variables de entorno
load_dotenv()
# Especifica los servidores de Kafka
bootstrap_servers_SPE = os.getenv("bootstrap_servers_SPE")
bootstrap_servers_AFC = os.getenv("bootstrap_servers_AFC")

# Configura el socket para recibir mensajes
host = os.getenv("HOST")
port = int(os.getenv("PUERTO"))


#Topic para el analisis online del kafka general
topic_online = os.getenv("topicOnline")

#Topic para el analisis online de alarmas
topic_alarms = os.getenv("topicAlarm")
#Creamos un admin para gestionar los topics
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers_SPE)


def main():
    
    # Crea el socket y espera por mensajes
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen(1)
        print(f"El servidor está escuchando en el puerto {port}...")
        #Creamos un pool de hilos para las distintas peticiones
        executor = ThreadPoolExecutor(max_workers=4)
        
        try:
            while True:
                print("Esperando conexiones")
                #Aceptamos una nueva conexión para cada cliente
                conn, addr = s.accept()
                topics = admin_client.list_topics()
                msg = ""
                # Recibe el mensaje del socket que contiene la información que quiere el cliente
                msg = conn.recv(1024).decode() 
                msg = json.loads(msg)
                topic = crear_topic(msg)
                #Comprobación de hilo tratando un topic online
                if topic in topics and msg['modo']=='online':
                  print("Topic ya existente")  
                else:
                  #Creamos un hilo para cada cliente
                  executor.submit(procesar, conn, addr,msg)
        except KeyboardInterrupt:
            executor.shutdown()
        
            
#Función principal que ejecuta cada hilo
def procesar(conn,addr,msg):

    #Creamos el producer para cada cliente
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers_SPE)

    print(f"Conexión establecida desde {addr[0]}:{addr[1]}")
  
    #Sacamos el modo de trabajo del json 
    modo = msg['modo']
    print("Modo recibido:  ", modo)
    topic = ""
    #Comprobamos el modo de trabajo
    if modo == "offline":
        
        #Creamos el topic
        topic = crear_topic(msg)
        print("Topic recibido: ", topic)
        
        #Comprobación de creación de topic
        if topic!="":
            
            #Se llama al método para obtener los datos del DataManager
            content = descargar_json(msg)
            
            #Se comprueba si la respuesta es válida
            if (content != "Error"):
                
                # Busca la posición de "resultados" en la respuesta HTTP
                pos_resultados = -1
                for i, row in enumerate(content):
                    if 'Results: ' in row:
                        pos_resultados = i
                        break
                    
                # Extrae los datos (CSV)
                parte_despues = content[pos_resultados + 2:]
                
                #Obtenemos el valor de las columnas
                fila_datos = content[pos_resultados + 1]
                
                #Creamos la estructura del json de salida
                json_salida = {}
                for valor in fila_datos:
                    json_salida[str(valor)] = ""
                
                time.sleep(5)    
                #Analizamos los datos del campo Resultado y lo almacenamos en la estructura json
                for fila in parte_despues:
                    
                    #Se crea un JSON para cada fila del CSV
                    for i, valor in enumerate(fila):
                        clave = fila_datos[i]
                        json_salida[str(clave)] = valor
                    
                    #Se comprueba la existencia del topic en el Kafka
                    if topic in admin_client.list_topics():
                        #Si el topic está activo se formatea el string para que sea un JSON
                        salida = json.dumps(json_salida)
                        #Se envían los datos por el topic
                        producer.send(topic, salida.encode('utf-8'))
                    else:
                        #En caso de que el topic ya no exista en el Kafka se acaba el envío
                        print("Topic ya borrado")
                        break
                #Si se completa todo el envío de datos offline se envía un mensaje vacío para finalizar la comunicación
                producer.send(topic, json.dumps(None).encode('utf-8'))
                #Se elimina el Productor para liberar recursos
                producer.close()
            else:
                #Si la respuesta no es válida se cierra el producer y acaba la ejecución
                producer.close()
    
    elif modo == "online":
        #Se crea el Topic para el caso online con el mismo método
        topic = crear_topic(msg)
        
        #Se crea una lista con los filtros a utilizar en los datos
        campos = crear_filtros(msg)
        
        #Se comprueba el tipo de analisis a realizar (collar o alarma)
        if 'tipo_analisis' in msg:
            tipo_analisis=msg['tipo_analisis']
        else:
            #Solo en los mensajes de alarma se indica, si no se tiene ese campo es un collar
            tipo_analisis='collar'
        
        #Si el mensaje solicita el analisis online de alarmas
        if tipo_analisis=='alarm':
            
            #Se crea un consumidor para las alarmas generadas por el AlarmReport
            consumer = KafkaConsumer(topic_alarms, bootstrap_servers = bootstrap_servers_AFC)
            
            #Para cada alarma que se reciba en el consumer se realiza el siguiente ciclo
            for mensaje in consumer:
                #Se decodifica el mensaje y se carga como un JSON
                mensaje_decodificado = json.loads(mensaje.value.decode('utf-8'))
                
                #Se comprueba si el Topic aún está activo
                if topic in admin_client.list_topics():
                    
                    #Si se requiere el análisis de un código en concreto
                    if 'alarmCode' in msg:
                        #Se crean los filtros solo con ese código de alarma
                        campos_entrada = [mensaje_decodificado['alarmCode']]
                        
                        #Verificamos si todos los campos del topic estan en cada mensaje
                        if verificar_valores(campos, campos_entrada):
                            #Si el mensaje cumple con los filtros se formatea carga y se envía
                            fila = json.dumps(mensaje_decodificado)
                            producer.send(topic, fila.encode('utf-8'))
                    else:
                        #Si no hay código en concreto para filtrar se envían todos los mensajes
                        fila = json.dumps(mensaje_decodificado)
                        producer.send(topic, fila.encode('utf-8'))
                else:
                    #Si no está el topic activo se sale del bucle y termina la ejecución
                    break
            #Se elimina el producer para liberar recursos
            producer.close()
            
        else:
            #Si no se solicitan alarmas se solicitan collares
            #Se crea un consumer del topic general del Kafka de AFarCloud para obtener los collares enviados
            consumer = KafkaConsumer(topic_online, bootstrap_servers = bootstrap_servers_AFC)
            
            #Para cada mensaje que obtiene el consumer se tiene el siguiente ciclo de trabajo
            for mensaje in consumer:
                
                #Se obtiene el mensaje y se decodifica
                mensaje_decodificado = json.loads(mensaje.value.decode('utf-8'))
                
                #Se obtienen las características del mensaje de collar a partir de su resourceID
                campos_entrada = mensaje_decodificado['resourceId'].split(":")
                
                #Se comprueba si el topic está activo
                if topic in admin_client.list_topics():
                    
                    #Se verifica si todos los campos del topic estan en cada mensaje
                    if verificar_valores(campos_entrada, campos):
                        #Si el mensaje cumple con los filtros se codifica y envía
                        fila = json.dumps(mensaje_decodificado)
                        producer.send(topic, fila.encode('utf-8'))
                else:
                    #Si el topic no está activo se sale del bucle y se termina el ciclo de trabajo
                    break
                
            #Se elimina el producer para liberar recursos
            producer.close()
            
        #Al terminar el ciclo de análisis online de cualquiera de los casos se cierra el consumer para liberar recursos
        consumer.close()

    conn.close()

#Método para descargar la información solicitada por el cliente del DataManager
def descargar_json(mensaje):
    
  #Comprobamos el tipo de analisis a realizar
    if mensaje['tipo_analisis'] == "historic":
      #Creamos la url acorde a la solicitud del cliente
        url = "http://torcos.etsist.upm.es:9224/getSensorTelemetry/" + mensaje['tipo_analisis'] + "/?csv=true"
        if "start_time" in mensaje:
            url += "&start_time=" + mensaje['start_time']
        if "end_time" in mensaje:
            url += "&end_time=" + mensaje['end_time']
        if "entityNames" in mensaje:
            url += "&entityNames=" + mensaje['entityNames']
        if "services" in mensaje:
            url += "&services=" + mensaje['services']
        if "types" in mensaje:
            url += "&types=" + mensaje['types']
        if "providers" in mensaje:
            url += "&providers=" + mensaje['providers']
        if "altitude" in mensaje:
            url += "&altitude=" + mensaje['altitude']
    elif mensaje['tipo_analisis'] == "latest":
        url = "http://torcos.etsist.upm.es:9224/getSensorTelemetry/" + mensaje['tipo_analisis'] + "/?limit=100&csv=true"
        
        if "entityNames" in mensaje:
            url += "&entityNames=" + mensaje['entityNames']
        if "services" in mensaje:
            url += "&services=" + mensaje['services']
        if "types" in mensaje:
            url += "&types=" + mensaje['types']
        if "providers" in mensaje:
            url += "&providers=" + mensaje['providers']
        if "altitude" in mensaje:
            url += "&altitude=" + mensaje['altitude']

    #Se manda la petición y se obtiene la respuesta    
    response = requests.get(url)

    if response.status_code == 200:
        #Se transforma la respuesta a texto
        contenido = response.text
        #Obtenermos la respuesta como un csv
        reader = csv.reader(contenido.strip().split('\n'))
        content = list(reader)
        return content
    else:
        #Si la respuesta no es válida se devuelve un error
        print(f'Error: {response.status_code}')
        return "Error"

#Método para crear el topic a partir del JSON del cliente
def crear_topic(mensaje):
    
        #Creamos el topic para enviar el mensaje identificativo con los datos del cliente
        topic = "afar_" + mensaje['modo']
        if mensaje['modo'] == 'offline':
          topic += "_" + mensaje['usuario']
        if 'tipo_analisis' in mensaje:
            topic += "_"+ mensaje['tipo_analisis']
        if 'start_time' in mensaje:
            topic += "_"+ mensaje['start_time']
        if 'end_time' in mensaje:
            topic += "_"+ mensaje['end_time']
        if 'services' in mensaje:
            topic += "_"+ mensaje['services']
        if 'providers' in mensaje:
            topic += "_"+ mensaje['providers']
        if 'types' in mensaje:
            topic += "_"+ mensaje['types']
        if 'entityNames' in mensaje:
            topic += "_"+ mensaje['entityNames']
        if 'alarmCode' in mensaje:
            topic += "_" + mensaje['alarmCode']
            
        return topic
    
#Método para crear los filtros que se aplican a los datos
def crear_filtros(mensaje):
    filtro = ["afar"]
    if "services" in mensaje:
        filtro.append(mensaje['services'])
    if "types" in mensaje:
        filtro.append(mensaje['types'])
    if "providers" in mensaje:
        filtro.append(mensaje['providers'])
    if "entityNames" in mensaje:
        filtro.append(mensaje['entityNames'])
    if "alarmCode" in mensaje:
        filtro.append(mensaje['alarmCode'])
    return filtro
    

#Funcion para verificar si los campos estan en la fila o no
def verificar_valores(diccionario, campos):
    todos_presentes = False
    todos_presentes = all(valor in diccionario for valor in campos)
    
    correcto = False
    if todos_presentes:
        correcto = True
    else:
        correcto = False
    return correcto

if __name__ == '__main__':
    main()
