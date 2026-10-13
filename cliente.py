"""
Cliente: envía un archivo al servidor usando SocketTCP (TCP simplificado con Stop & Wait).
Uso:   python3 cliente.py <host> <puerto> [debug] < archivo
Ej.:   python3 cliente.py localhost 8000 < archivo.txt

Lee completo el archivo recibido por entrada estándar y lo envía al servidor en
tres etapas: establece la conexión con ``connect`` (3-way handshake), envía el
contenido con ``send`` (Stop & Wait, en segmentos de a lo más 16 bytes de datos)
y cierra la conexión con ``close``.

Args:
    host (str): Dirección IP o nombre del equipo donde escucha el servidor.
    puerto (int): Puerto donde escucha el servidor.
    debug (opcional): Si se incluye la palabra ``debug``, se imprimen los
        mensajes de depuración de SocketTCP (envíos, ACK, timeouts y reenvíos).

Note:
    El archivo se lee como bytes, por lo que se puede enviar cualquier tipo de
    archivo (texto o binario). Si no se redirige un archivo con ``<``, el
    programa espera la entrada desde el teclado hasta recibir EOF (Ctrl+D).
    El cliente es quien inicia el cierre de la conexión, ya que es quien sabe
    cuándo terminó de enviar.
"""
import sys
import SocketTCP

if len(sys.argv) < 3:
    print("Uso: python3 cliente.py <host> <puerto> [debug] < archivo")
    sys.exit(1)

address = (sys.argv[1], int(sys.argv[2]))
debug = "debug" in sys.argv[3:]

# el contenido del archivo llega por entrada estándar; se lee completo como bytes
data = sys.stdin.buffer.read()

client_socketTCP = SocketTCP.SocketTCP()
client_socketTCP.debug = debug
client_socketTCP.connect(address)   # 3-way handshake
client_socketTCP.send(data)         # Stop & Wait, en trozos de 16 bytes
client_socketTCP.close()            # cierre FIN, FIN+ACK, ACK

if debug:
    print(f"[cliente] archivo de {len(data)} bytes enviado y conexión cerrada")