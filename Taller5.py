import redis
import json
import os
import sys
from uuid import uuid4 # Usaremos un UUID para generar IDs únicos para cada libro

# --- 1. Configuración del Almacén en Memoria (KeyDB/Redis) ---
# Usaremos variables de entorno para la configuración, si no están, usa la local por defecto
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# Clave de Redis para el contador de IDs o un set/list para IDs
KEY_PREFIX = "libro:" # Prefijo para la clave de cada libro (Hash o String)
ALL_BOOKS_KEY = "libros:ids" # Usaremos un SET de Redis para almacenar todos los IDs de los libros.

# --- 2. Conexión y Cliente ---
def get_redis_client():
    """Establece la conexión a KeyDB/Redis y retorna el cliente."""
    try:
        # 1. Crear el cliente
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_timeout=5)

        # 2. Verificar la conexión inmediatamente (ping)
        client.ping()

        print(f"✅ Conexión a KeyDB/Redis exitosa en {REDIS_HOST}:{REDIS_PORT} (DB {REDIS_DB}).")
        return client

    # Capturamos el error de fallo de conexión
    except redis.exceptions.ConnectionError as e:
        print("\n❌ ERROR CRÍTICO DE CONEXIÓN A KEYDB/REDIS ❌")
        print("---------------------------------------------------------------------")
        print(f"Asegúrate de que el servidor KeyDB o Redis esté corriendo o que la configuración ({REDIS_HOST}:{REDIS_PORT}) sea correcta.")
        print("El programa se cerrará.")
        sys.exit(1) # Finaliza el programa si la conexión inicial falla
    except Exception as e:
        print(f"❌ Error inesperado durante la conexión: {e}")
        sys.exit(1)

# Cliente global de Redis
redis_client = get_redis_client()

# --- 3. Funciones de la Biblioteca (CRUD y Validaciones) ---

def agregar_libro():
    """Agrega un nuevo libro, serializándolo como JSON y guardándolo como un String de Redis."""
    print("\n--- AGREGAR NUEVO LIBRO ---")
    titulo = input("Título: ").strip()
    autor = input("Autor: ").strip()

    # Validación 1: Campos requeridos
    if not titulo or not autor:
        print("❌ Error: El título y el autor no pueden estar vacíos.")
        return

    try:
        anio = input("Año de Publicación (opcional): ")
        # Validación 2: Tipo de dato (garantiza que sea None o int)
        anio = int(anio) if anio.isdigit() else None
    except ValueError:
        print("⚠️ Advertencia: Año no válido. Se ignorará.")
        anio = None

    genero = input("Género: ").strip()

    # Usaremos un UUID como ID único
    libro_id = str(uuid4())

    nuevo_libro = {
        "id": libro_id, # Se incluye el ID dentro del objeto para facilitar la recuperación
        "titulo": titulo,
        "autor": autor,
        "anio_publicacion": anio,
        "genero": genero if genero else None,
        "leido": False
    }

    try:
        # Serializar el diccionario a una cadena JSON
        libro_json = json.dumps(nuevo_libro)
        key = f"{KEY_PREFIX}{libro_id}"

        # 1. Almacenar el libro como un String en Redis
        redis_client.set(key, libro_json)
        # 2. Agregar el ID a un SET para poder listar todos los IDs
        redis_client.sadd(ALL_BOOKS_KEY, libro_id)

        print(f"\n✅ Libro '{titulo}' de {autor} agregado exitosamente (ID: {libro_id[-5:]}).")
    except Exception as e:
        print(f"❌ Error al insertar el libro en KeyDB/Redis: {e}")

def buscar_libro_por_id_parcial(id_parcial):
    """Busca un libro en Redis cuyo ID termine en la cadena parcial (KeyDB no soporta búsquedas parciales como MongoDB)."""
    # KeyDB no tiene la capacidad de buscar por patrón de valor como MongoDB, así que:
    # 1. Recuperamos todos los IDs del SET
    all_ids_bytes = redis_client.smembers(ALL_BOOKS_KEY)
    all_ids = [id.decode('utf-8') for id in all_ids_bytes]

    # 2. Buscamos el ID que coincida con la parte final
    # Limitamos la búsqueda al primero que coincida para simular el comportamiento anterior
    libro_id_completo = next((_id for _id in all_ids if _id.endswith(id_parcial)), None)

    if not libro_id_completo:
        return None, None

    # 3. Recuperar el libro
    key = f"{KEY_PREFIX}{libro_id_completo}"
    libro_json = redis_client.get(key)
    
    if libro_json:
        return json.loads(libro_json), key
    
    return None, None

def listar_libros():
    """Muestra todos los libros, recuperando sus IDs del SET y luego cada String JSON."""
    
    # Obtener todos los IDs de los libros
    all_ids_bytes = redis_client.smembers(ALL_BOOKS_KEY)
    all_keys = [f"{KEY_PREFIX}{id.decode('utf-8')}" for id in all_ids_bytes]

    # Usar MGET para recuperar todos los libros en una sola llamada
    libros_json_list = redis_client.mget(all_keys)
    
    libros = []
    for libro_json in libros_json_list:
        if libro_json:
            libros.append(json.loads(libro_json.decode('utf-8')))

    # El orden en MGET no es garantizado y queremos la lista al revés, 
    # pero el orden es menos crucial en un sistema en memoria simple.
    # Lo mostramos como se recupera para simplicidad.
    libros.reverse() # Invertimos para el orden DESC

    # Validación 3: Búsquedas sin resultados
    if not libros:
        print("\n--- 📚 BIBLIOTECA VACÍA ---")
        print("Aún no tienes libros registrados. Usa la opción 1 para agregar uno.")
        return

    print("\n--- 📚 MI BIBLIOTECA PERSONAL (KeyDB/Redis) ---")
    print(f"{'ID (5 chars)':<7} | {'Título':<35} | {'Autor':<25} | {'Año':<4} | {'Leído'}")
    print("-" * 85)

    for libro in libros:
        estado_leido = "Sí (✅)" if libro.get('leido', False) else "No (❌)"
        id_display = libro['id'][-5:]

        print(f"{id_display:<7} | {libro['titulo'][:35]:<35} | {libro['autor'][:25]:<25} | {libro['anio_publicacion'] if libro['anio_publicacion'] else 'N/A':<4} | {estado_leido}")
    print("-" * 85)

def actualizar_libro(libro, key, **kwargs):
    """Actualiza un libro en KeyDB/Redis."""
    # Aplicar los cambios
    libro.update(kwargs)
    
    # Serializar y guardar
    libro_json = json.dumps(libro)
    redis_client.set(key, libro_json)
    
def marcar_como_leido():
    """Busca un libro por ID parcial y actualiza el campo 'leido' a True."""
    listar_libros()
    id_parcial = input("\nIngresa los ÚLTIMOS 5 dígitos del ID para marcar como LEÍDO: ").strip()

    if not id_parcial:
        print("❌ Error: El ID no puede estar vacío.")
        return

    try:
        libro, key = buscar_libro_por_id_parcial(id_parcial)

        if not libro:
            print(f"⚠️ Advertencia: No se encontró un libro cuyo ID termine en {id_parcial}.")
            return

        if libro.get('leido', False):
            print(f"⚠️ Advertencia: El libro ya estaba marcado como leído.")
            return

        # Actualizar el estado y guardar
        actualizar_libro(libro, key, leido=True)
        print(f"✅ Libro con ID final {id_parcial} ('{libro['titulo']}') marcado como LEÍDO.")

    except Exception as e:
        print(f"❌ Error al actualizar el libro: {e}")


def eliminar_libro():
    """Busca un libro por ID parcial, lo elimina del almacenamiento y de la lista de IDs."""
    listar_libros()
    id_parcial = input("\nIngresa los ÚLTIMOS 5 dígitos del ID para ELIMINAR: ").strip()

    if not id_parcial:
        print("❌ Error: El ID no puede estar vacío.")
        return

    try:
        libro, key = buscar_libro_por_id_parcial(id_parcial)

        if not libro:
            print(f"⚠️ Advertencia: No se encontró un libro cuyo ID termine en {id_parcial}.")
            return

        # 1. Eliminar la clave del libro (la eliminación devuelve el número de claves eliminadas)
        deleted_count = redis_client.delete(key)
        
        if deleted_count > 0:
            # 2. Eliminar el ID del SET de IDs
            redis_client.srem(ALL_BOOKS_KEY, libro['id'])
            print(f"✅ Libro con ID final {id_parcial} ('{libro['titulo']}') eliminado exitosamente.")
        else:
            print(f"⚠️ Advertencia: No se pudo eliminar la clave del libro.")

    except Exception as e:
        print(f"❌ Error al eliminar el libro: {e}")


def mostrar_menu():
    print("\n" + "="*38)
    print(" ADMINISTRADOR DE BIBLIOTECA (KeyDB/Redis)")
    print("="*38)
    print("1. Agregar nuevo libro")
    print("2. Listar todos los libros")
    print("3. Marcar libro como leído")
    print("4. Eliminar libro por ID (últimos 5 dígitos)")
    print("5. Salir")
    print("-" * 38)

def main():
    while True:
        mostrar_menu()
        opcion = input("Selecciona una opción (1-5): ").strip()

        if opcion == '1':
            agregar_libro()
        elif opcion == '2':
            listar_libros()
        elif opcion == '3':
            marcar_como_leido()
        elif opcion == '4':
            eliminar_libro()
        elif opcion == '5':
            print("👋 Gracias por usar la Biblioteca CLI con KeyDB/Redis.")
            break
        else:
            print("❌ Opción no válida. Por favor, selecciona un número entre 1 y 5.")

        input("\nPresiona Enter para continuar...")
        os.system('cls' if os.name == 'nt' else 'clear')

if __name__ == "__main__":
    if redis_client: # Asegura que la aplicación solo corra si la conexión fue exitosa
        main()