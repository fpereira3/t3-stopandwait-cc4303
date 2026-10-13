"""
Servidor: recibe un archivo desde cliente.py usando SocketTCP y lo imprime en salida estándar.
Uso:   python3 servidor.py [host] [puerto] [debug]      (por defecto localhost 8000)
Ej.:   python3 servidor.py
       python3 servidor.py localhost 8000 debug

Escucha en la dirección indicada, acepta una única conexión (3-way handshake),
recibe el archivo llamando a ``recv`` las veces necesarias e imprime su
contenido en salida estándar. Al terminar, continúa el cierre de conexión
iniciado por el cliente con ``recv_close``.

Args:
    host (str, opcional): Dirección donde escuchar. Por defecto ``localhost``.
    puerto (int, opcional): Puerto donde escuchar. Por defecto ``8000``.
    debug (opcional): Si se incluye la palabra ``debug``, se imprimen los
        mensajes de depuración de SocketTCP.

Note:
    El contenido se escribe como bytes en ``sys.stdout.buffer``, por lo que un
    carácter de varios bytes (por ejemplo, "á") no se corrompe aunque quede
    dividido entre dos llamadas a ``recv``. Con ``debug``, los mensajes de
    depuración salen por la misma salida que el archivo; para comparar el
    archivo recibido con el original (por ejemplo, con ``diff``), conviene
    ejecutar sin ``debug``. El servidor atiende una sola conexión y termina.
"""
import sys
import SocketTCP

args = [a for a in sys.argv[1:] if a != "debug"]
host = args[0] if len(args) > 0 else "localhost"
port = int(args[1]) if len(args) > 1 else 8000
debug = "debug" in sys.argv[1:]
buff_size = 1024    # tamaño de cada recv; el archivo puede ser más grande

server_socketTCP = SocketTCP.SocketTCP()
server_socketTCP.debug = debug
server_socketTCP.bind((host, port))
connection_socketTCP, new_address = server_socketTCP.accept()   # 3-way handshake

# se llama a recv las veces necesarias hasta completar el mensaje:
# el mensaje termina cuando no faltan bytes por llegar ni quedan bytes guardados
while True:
    chunk = connection_socketTCP.recv(buff_size)
    sys.stdout.buffer.write(chunk)   # bytes tal cual (evita cortar caracteres multibyte)
    sys.stdout.flush()
    if connection_socketTCP.remaining_bytes == 0 and len(connection_socketTCP.recv_buffer) == 0:
        break

connection_socketTCP.recv_close()   # el cliente inicia el cierre; aquí se continúa
if debug:
    print("[servidor] archivo recibido y conexión cerrada")