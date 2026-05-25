import os 
import shutil

usuario=os.environ.get( "USERNAME" )

path_destino = 'C:/Users/'+usuario+'/Documents/Project_snowflake_cli/data_sample/' # ruta destino
path_origen='C:/Users/'+usuario+'/Documents/Project_snwosql/Snwoflake_SME/data_sample/' # ruta orgien archivos 


# extensiones permitidas
extensiones = ('.csv', '.xml', '.json', '.txt')

# extensiones permitidas
extensiones = ('.csv', '.xml', '.json', '.txt')


def copiar_archivos(path_origen, path_destino):

    # crear carpeta destino si no existe
    os.makedirs(path_destino, exist_ok=True)

    # obtener archivos válidos
    archivos = [
        archivo for archivo in os.listdir(path_origen)
        if archivo.lower().endswith(extensiones)
    ]

    # eliminar archivos existentes
    for archivo in archivos:

        archivo_destino = os.path.join(path_destino, archivo)

        if os.path.exists(archivo_destino):
            os.remove(archivo_destino)
            print(f'Eliminado {archivo} de destino')

    # copiar archivos
    for archivo in archivos:

        ruta_origen = os.path.join(path_origen, archivo)
        ruta_destino = os.path.join(path_destino, archivo)

        shutil.copy(ruta_origen, ruta_destino)

        print(f'Copiado {archivo} -> {path_destino}')


# =========================
# CLIENTE A
# =========================

copiar_archivos(
    path_origen,
    path_destino
)

# =========================
# CLIENTE B
# =========================

path_origen_b = os.path.join(path_origen, 'Client B')
path_destino_b = os.path.join(path_destino, 'Client B')

copiar_archivos(
    path_origen_b,
    path_destino_b
)