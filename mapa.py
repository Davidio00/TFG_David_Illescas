import pandas as pd
import folium

def main():
    # Leer el archivo CSV con los datos de las vacas y sus resultados
    data = pd.read_csv('./resultadosL.csv')
    
    # Crear un mapa centrado en una ubicación específica
    mapa = folium.Map(location=[40.687049, -4.546216], zoom_start=12)

    # Definir los vertices del área de estudio
    punto1 = (40.687049, -4.546216)
    punto2 = (40.687538, -4.523445)
    punto3 = (40.700389, -4.523355)
    punto4 = (40.696423, -4.543692)

    # Definir el color de la línea
    color_linea = 'red'

    # Agregar puntos al mapa
    for index, row in data.iterrows():
        latitud = row['latitud']
        longitud = row['longitud']
        predic = row['predic']
        entityName = row['entityName']
        colore = 'blue'
        #En función de si está dentro o fuera se pone verde o rojo
        if int(predic[1]) == 0:
            colore = 'red'
        elif int(predic[1]) == 1:
            colore = 'green'
        #Se agrega el marcador
        folium.Marker(
            location=[latitud, longitud],
            icon=folium.Icon(color=colore),
            popup=entityName
        ).add_to(mapa)

    # Agregar los puntos adicionales al mapa
    folium.PolyLine(
        locations=[punto1, punto2, punto3, punto4, punto1],
        color=color_linea
    ).add_to(mapa)
    # Guardar el mapa como un archivo HTML
    mapa.save("./mapa_vacas_alarma.html")
    
if __name__ == '__main__':
    main()
