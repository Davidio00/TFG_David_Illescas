import asyncio
import websockets
import time
import socket
import json
from kafka import KafkaConsumer
import time
from dotenv import load_dotenv
import os
from kafka.admin import KafkaAdminClient, NewTopic

#Variable usada para almacenar a los clientes
clientes_conectados = {}
#Para acceder a las variables de entorno
load_dotenv()

# Especifica el servidor de Kafka del SPE
bootstrap_servers = os.getenv("bootstrap_servers")

# Configura el socket para recibir mensajes
host = os.getenv("HOST")
port = int(os.getenv("PUERTO"))
portWeb = int(os.getenv("PORTWEB"))
hostWeb = os.getenv("HOSTWEB")

#Función que crea un socket entre el subscriber y el publisher
def conectar(host, port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host,port))
    except Exception as e:
        print("Error en la conexión--", str(e))
    return s

#Método para envíar al publisher la información del cliente
def send_info(filtrado, socket):
    
    try:
        socket.send(json.dumps(filtrado).encode())
    except Exception as e:
        print("Error en la conexión--", str(e))

#Método para crear el topic de cada cliente y obtener sus datos
def crear_topic(mensaje):

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

#Método para agregar a cada nuevo usuario con su topic
def agregar_usuario(clave, nombre_usuario):
    
    #Se comprueba si existe el topic
    if clave in clientes_conectados:
        #Si existe el topic se añade el nuevo usuario
        clientes_conectados[clave].append(nombre_usuario)
    else:
        #Si no existe el topic se crea y se añade al usuario
        clientes_conectados[clave] = [nombre_usuario]


#Método para eliminar a un usuario
def remover_usuario(clave, nombre_usuario):
    
    #Se comprueba si existe el topic
    if clave in clientes_conectados:
        #Si existe se elimina al usuario
        clientes_conectados[clave].remove(nombre_usuario)
        #Se comprueba si queda algún cliente conectado
        if len(clientes_conectados[clave]) == 0:
            #Si no queda ningún cliente se elimina el topic de la variable y de Kafka
            del clientes_conectados[clave]
            eliminar_topic(clave)


#Método para eliminar un topic sin clientes de Kafka para liberar recursos
def eliminar_topic(topic):
    # Crea una instancia de KafkaAdminClient
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    # Elimina el topic utilizando el método delete_topics de KafkaAdminClient
    admin_client.delete_topics(topics=[topic])
    # Cierra la conexión con KafkaAdminClient
    admin_client.close()


#Método que se ejecutará para recibir solicitudes de cada cliente y se ejecutará de forma independiente para cada cliente
async def handler(websocket, path):
    
    #Recibir mensaje del cliente con la información de filtrado
    mensaje = await websocket.recv()
    
    #Obtenemos el json para acceder a los datos
    mensaje = json.loads(mensaje)
    
    #Obtenemos el puerto del cliente para identificarlo
    puerto_cliente = websocket.remote_address[1]
    
    #Al mensaje del cliente le añadimos el puerto del cliente para que el producer lo pueda usar
    mensaje['usuario'] = puerto_cliente
    
    #Creamos el topic al que el publisher envia la información requerida
    topic = crear_topic(mensaje)
    
    #Guardamos al cliente en un diccionario con el puerto
    agregar_usuario(topic, puerto_cliente)
    
    #Nos conectamos al publisher
    s = conectar(host, port)
    
    #Le enviamos lo recibido al publisher
    send_info(mensaje, s)
    
    #Cerramos la conexión con el publisher
    s.close()
    
    #Creamos el consumer con el topic del algoritmo
    consumer = KafkaConsumer(topic, bootstrap_servers = bootstrap_servers)
    
    #Se ejecuta la siguiente rutina hasta que el cliente se desconecte o se termine la información en caso de análisis offline
    while True:
        #Intentamos consumir el ultimo mensaje
        mensaje = consumer.poll(0, 1)
        
        if len(mensaje) != 0:
            
            #Decodificamos el mensaje obtenido del kafka
            mensaje_decodificado = json.loads(next(iter(mensaje.values()))[0].value.decode('utf-8'))
            
            #Formateamos el mensaje para enviarlo al cliente
            mensaje_cliente = next(iter(mensaje.values()))[0].value.decode('utf-8')

            #Comprobamos si el mensaje es de final de comunicación (análisis offline)
            if mensaje_decodificado != None:
                try:
                    # Enviamos los mensajes al cliente
                    await websocket.send(mensaje_cliente)
                except websockets.exceptions.ConnectionClosed as e:
                    #Manejador de las desconexiones de clientes
                    print(f"Client {websocket.remote_address} disconnected: {e}")
                    break
            elif mensaje_decodificado == None:
                #Si es el mensaje de final se sale al bucle
                break
        else:
            #La función poll del consumer se ejecuta constantemente y es bloqueante para evitar este bloqueo
            #y que no afecte al resto de clientes cada hilo hace una pausa de 1 segundo. Esto permite que los
            #clientes se puedan intercalar sin bloquear el sistema
            await asyncio.sleep(1)
            
    #Cerramos el consumer
    consumer.close()
    #Para terminar cerramos el websocket para liberar los recursos
    await websocket.close()
    #Borramos el cliente
    remover_usuario(topic, puerto_cliente)


if __name__ == '__main__':
    #Lanzamos el servidor en el puerto y host indicados
    start_server = websockets.serve(handler, hostWeb, portWeb, ping_interval=5000)
    #Ejecutamos el servidor en un bucle
    asyncio.get_event_loop().run_until_complete(start_server)
    #Se ejecuta la tarea de forma perpetua
    asyncio.get_event_loop().run_forever()
