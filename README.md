RESUMEN

El propósito de este proyecto final de grado es la instalación de un sistema de alarmas y transmisión de información en una explotación agrícola automatizada. Se pretende la utilización de algoritmos de machine learning con los datos obtenidos. 
Este estudio se basa en el proyecto AFarCloud (Aggregate Farming in the Cloud), una iniciativa centrada en la agricultura de precisión, que ya cuenta con sensores, vehículos autónomos, dispositivos inteligentes y una plataforma informática para la gestión y almacenamiento de los datos recolectados.

Se cuenta con una plataforma que recibe de manera continua datos provenientes de diversos dispositivos. Este sistema, operativo por sí mismo, se encarga de la recolección, validación y almacenamiento de toda la información. 
No obstante, para que estos datos sean efectivos, es necesario procesarlos y analizarlos. El enfoque de este proyecto se centra en dirigir y depurar el flujo de datos generados para su utilización por distintos algoritmos. Se toman en consideración las alarmas generadas por anomalías, las cuales deben ser examinadas junto con las métricas correspondientes.

Un aspecto fundamental del proyecto reside en la capacidad de conexión de los algoritmos, tanto de manera remota como local. Esta conectividad posibilitará el análisis de datos y la generación de información relevante y detallada para el usuario. 
Además, la estructura global de la plataforma permitirá la incorporación de nuevos algoritmos altamente beneficiosos, gracias a la opción de conexión local.

Por consiguiente, el objetivo es desarrollar dos sistemas con la capacidad de interactuar con el resto de la plataforma AFarCloud para obtener métricas y alarmas, y así hacerlas accesibles a los algoritmos del cliente. 
Los datos disponibles deben permitir tanto un análisis en tiempo real de la información entrante a la plataforma como un análisis histórico de los datos previamente generados.

Es esencial destacar que se sigue el formato de mensajes ya en uso en AFarCloud y se mantiene la idea de desplegar los sistemas en contenedores Docker. Esto se realiza con el objetivo de garantizar que los sistemas sean portables, escalables y estén aislados, aspectos fundamentales en un proyecto de estas características.

Una vez que los sistemas son desarrollados, se procede a llevar a cabo diversas pruebas de funcionamiento en diferentes niveles. La primera etapa implica probar los dos sistemas en un entorno controlado, utilizando muestras pequeñas, para verificar la coherencia de los resultados con las entradas. 
Además, se verifica que el flujo de mensajes sea el adecuado a través de los sistemas implicados. En la segunda fase, se incrementa el número de entradas para simular un entorno más realista y evaluar la coherencia de los resultados. En la última etapa, se realizan pruebas con múltiples clientes y flujos de datos.

Durante todas las pruebas, se registran las salidas y se comparan con los resultados esperados con el objetivo de lograr un alto nivel de precisión con los algoritmos. Al mismo tiempo, se supervisa el flujo de mensajes y la comunicación entre los diversos componentes para garantizar el correcto funcionamiento del sistema.
Tras llevar a cabo las pruebas mencionadas, es posible obtener conclusiones sobre la integración de estos sistemas en los sectores agrícola y ganadero. Entre ellas, se destaca que la utilización de algoritmos facilita las tareas agrícolas al permitir una actuación precisa en áreas críticas de la granja, lo que conduce a una optimización de los recursos disponibles. Además, se puede concluir que la implementación de un sistema capaz de gestionar alarmas mejora considerablemente la detección temprana de problemas, evitando posibles pérdidas económicas derivadas de una detección tardía.

En consecuencia, se deduce que la adopción de granjas inteligentes, que fusionan el sector agropecuario con tecnologías avanzadas, podría ofrecer una ventaja significativa sobre las granjas tradicionales.


CÓDIGO DEL PROYECTO

1.1	CÓDIGO DISPONIBLE

Todo el código utilizado en el proyecto ha sido almacenado en un repositorio GitHub.
En el repositorio se encuentran los siguientes ficheros:
•	Alarm_report.py: este archivo encapsula la lógica de funcionamiento del sistema Alarm Report, que será el encargado de gestionar y almacenar las alarmas.
•	Mapa.py: programa diseñado para dibujar un área en un mapa y dibujar los collares en el de un color u otro si están dentro o no.
•	Publisher.py: este programa será el encargado de recibir del subscriber.py las características de los datos y filtrarlos de las distintas fuentes.
•	Subscriber.py: recibe de los clientes los mensajes con los datos que quieren y recibe esos datos a través del servidor Kafka.
•	Cliente_sensor.py: algoritmo de cliente que recibe la cantidad de agua en el suelo y la humedad en el aire, y determina si se necesita riego.
•	Cliente_localizacion.py: algoritmo de cliente que recibe la localización de un collar y determina si está en su recinto.
•	Generador_localizacion.py: programa utilizado para generar un fichero de entrenamiento para el algoritmo de localización.
•	Generador_sensor.py: programa utilizado para generar un fichero de entrenamiento para el algoritmo de sensores.
•	Generador_collares.py: este programa lee collares de un fichero CSV y los envía por un Kafka para simular el entorno real de la plataforma.
•	Generador_alarmas.py: programa que genera alarmas de distinto tipo y las envía por un Kafka para simular el entorno real de la plataforma.

Además de estos ficheros con el código, que contiene la lógica del proyecto, se encuentran también las imágenes de los contenedores usados. Estas imágenes contienen los servicios de Kafka, MySQL y contenedores con Python para ejecutar el código anterior.


1.2	REQUISITOS PARA EL FUNCIONAMIENTO

Para un correcto funcionamiento del proyecto se deben tener instalados los siguientes programas:
  •	Python 3.10+: para ejecutar los algoritmos de clientes fuera de Docker se debe tener instalado Python.
  •	Librerías de Python: 
      o	Kafka-python: crear productores y consumidores del servicio Kafka.
      o	KafkaAdminClient: administrar el servicio Kafka.
      o	WebSocket: creación de clientes y servidor WebSocket.
      o	Sklearn.tree: creación de modelos de aprendizaje.
      o	Pandas: gestión de grandes volúmenes de datos.
      o	Pickle: librería para almacenar los modelos en archivos para su posterior uso.
      o	Joblib: librería que sirve para cargar el modelo desde un archivo.
      o	CSV: gestión de archivos CSV.
      o	JSON: gestionar datos en formato JSON.
      o	ThreadPoolExecuter: librería para gestionar hilos de forma paralela.
      o	Dotenv: librería para acceder a las variables de entorno.
      o	Requests: para realizar peticiones HTTP.
      o	Socket: gestión, creación y uso de sockets.
      o	Folium: creación de ventanas con mapas.
      o	Mysql.connector: conexión y consulta a bases de datos MySQL.
      o	Asyncio: librería para gestionar funciones asíncronas, necesarias en servidores WebSocket.
  •	Docker Desktop: programa necesario para ejecutar los programas en contenedores y gestionar su funcionamiento.
  •	GitHub: para obtener o modificar el repositorio de código en línea.


1.3	EJECUCIÓN DEL CÓDIGO

Para ejecutar todo el proyecto, es necesario descargar las imágenes disponibles en GitHub. Una vez que se han descargado las imágenes correctamente, será necesario cargarlas con el comando de Docker adecuado. El comando es el siguiente:

docker load -i archivo.tar

Este comando debe ejecutarse, reemplazando el parámetro archivo.tar con el nombre del archivo correspondiente. Este proceso debe realizarse para todas las imágenes, lo que garantizará que se disponga de todos los recursos necesarios para la ejecución del proyecto.
A continuación, es necesario convertir estas imágenes de contenedores en contenedores activos. Para este paso, será necesario ejecutar el siguiente comando para cada imagen:

docker run -d -p 80:80 nombre-imagen

El anterior comando debe ser ejecutado para cada contenedor, sustituyendo "nombre-imagen" por el nombre de cada imagen disponible, siempre teniendo en cuenta ciertas consideraciones. En el parámetro "-p" se debe especificar el puerto del host al que se redirigirá el puerto del contenedor. Es importante tener en cuenta este detalle al asignar los puertos y reflejarlo en los archivos de variables de entorno.
Para el caso particular de los contenedores de Kafka, llamados imagen_kafka e imagen_zookeeper, se debe ejecutar un comando diferente, ya que estos dos contenedores están relacionados a través de Docker-compose, lo que permite relacionar dos contenedores para realizar tareas más complejas. Por lo tanto, los contenedores deben crearse a partir de las imágenes y el archivo llamado "Docker-compose.yml". Para esta tarea el comando necesario es:

docker-compose up -d

Una vez que se han ejecutado todos los comandos, los contenedores deben ser iniciados en un cierto orden. Primero, se ejecutará el contenedor que contiene el servicio Kafka, incluyendo tanto Zookeeper como Broker. Una vez que el servicio Kafka esté en funcionamiento, se ejecutarán los contenedores del Producer, el Consumer, el Alarm Report y la base de datos.
Después de ejecutar estos cuatro contenedores, los dos sistemas estarán operativos y listos para recibir datos del servicio Kafka de AFarCloud y atender solicitudes de clientes.
