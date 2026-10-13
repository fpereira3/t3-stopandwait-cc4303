"""
Tests del lado cliente, organizados por paso del enunciado.
Uso:   python test_client.py <paso> [debug]
Ej.:   python test_client.py 5          python test_client.py 5 debug
Ejecutar siempre DESPUÉS de test_server.py con el mismo número de paso.

Cada paso del enunciado que tiene una prueba corresponde a una función
``paso_N``, que debe ejecutarse junto a la función del mismo paso en
``test_server.py``. El paso 2 (constructor) no tiene prueba propia, ya que se
ejecuta cada vez que se crea un SocketTCP.

Attributes:
    ADDRESS (tuple): Dirección ``(ip, puerto)`` del servidor.
    DEBUG (bool): ``True`` si se pasó ``debug`` por línea de comandos.
    LONG_MESSAGE (bytes): Mensaje de 30 líneas (261 bytes) para las pruebas
        con pérdidas; debe ser igual al de ``test_server.py``.
    TESTS (dict): Relaciona el número de paso (``str``) con su función.
"""
import socket
import sys
import time
import SocketTCP

ADDRESS = ('localhost', 8000)
DEBUG = "debug" in sys.argv


def nuevo_socket_cliente():
    """Crea un SocketTCP y lo conecta al servidor en ``ADDRESS``.

    Returns:
        SocketTCP.SocketTCP: Socket conectado (handshake completo), con el
        modo debug según ``DEBUG``.
    """
    client_socketTCP = SocketTCP.SocketTCP()
    client_socketTCP.debug = DEBUG
    client_socketTCP.connect(ADDRESS)
    return client_socketTCP


def leer_archivo():
    """Pide la ruta de un archivo con ``input`` y retorna su contenido.

    Returns:
        bytes: Contenido completo del archivo.
    """
    path = input("Ruta del archivo: ")
    with open(path, "rb") as f:
        return f.read()


# ---------- Paso 1: UDP sin headers, trozos de 16 bytes ----------
def paso_1():
    """Paso 1 (Test 1 y Test 2): cliente UDP provisorio, sin headers.

    Envía el archivo en trozos de a lo más 16 bytes, sin confirmación ni
    reenvíos. El Test 2 es la misma prueba con netem activo.
    """
    data = leer_archivo()
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for i in range(0, len(data), 16):
        udp_socket.sendto(data[i:i + 16], ADDRESS)
    udp_socket.close()


# ---------- Paso 3: UDP con headers ----------
def paso_3():
    """Paso 3: cliente UDP provisorio que agrega headers con ``create_segment``.

    Envía el archivo en segmentos con header y hasta 16 bytes de datos. El seq
    parte en 0 y avanza según los bytes enviados, como en Stop & Wait, aunque
    aún no se esperan ACK.
    """
    data = leer_archivo()
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    seq = 0
    for i in range(0, len(data), 16):
        chunk = data[i:i + 16]
        d = {"SYN": False, "ACK": False, "FIN": False, "SEQ": seq, "DATA": chunk}
        udp_socket.sendto(SocketTCP.SocketTCP.create_segment(d), ADDRESS)
        seq += len(chunk)
    udp_socket.close()


# ---------- Paso 4: 3-way handshake ----------
def paso_4():
    """Paso 4: lado cliente del 3-way handshake (``connect``).

    Imprime la dirección propia, el destino (que debe ser el socket nuevo del
    servidor) y el número de secuencia, que debe coincidir con el del
    servidor.
    """
    client_socketTCP = nuevo_socket_cliente()
    print("Cliente:", client_socketTCP.origin_address)
    print("Destino:", client_socketTCP.destination_address)
    print("seq:", client_socketTCP.seq)


# ---------- Paso 5: send y recv ----------
def paso_5():
    """Paso 5: envía los mensajes de los Tests 1 a 4 con ``send``.

    Returns:
        SocketTCP.SocketTCP: Socket de la conexión, para que los pasos
        siguientes continúen con el cierre.
    """
    client_socketTCP = nuevo_socket_cliente()
    # test 1
    client_socketTCP.send("Mensje de len=16".encode())
    # test 2
    client_socketTCP.send("Mensaje de largo 19".encode())
    # test 3
    client_socketTCP.send("Mensaje de largo 19".encode())
    # test 4
    n = 21
    client_socketTCP.send(b"A" * n + b"B" * n)

    return client_socketTCP


# ---------- Paso 6: cierre de conexión (tests del paso 5 + cierre) ----------
def paso_6():
    """Paso 6: envíos del paso 5 seguidos del cierre de conexión (``close``).

    Se ejecuta sin pérdidas.
    """
    client_socketTCP = paso_5()
    client_socketTCP.close()
    print("Conexión cerrada con éxito (close)")


# ---------- Paso 7: handshake con pérdidas (usar con netem) ----------
LONG_MESSAGE = "".join(f"linea {i}\n" for i in range(1, 31)).encode()  # 231 bytes, 15 segmentos


def paso_7():
    """Paso 7: envíos del paso 5 y un mensaje largo, con pérdidas (netem).

    Luego inicia el cierre con ``close``.
    """
    client_socketTCP = paso_5()
    client_socketTCP.send(LONG_MESSAGE)
    print("Datos enviados. Cerrando conexión (puede no terminar con pérdidas hasta el paso 8)...")
    client_socketTCP.close()
    print("Conexión cerrada con éxito (close)")


# ---------- Paso 8: cierre con pérdidas (usar con netem) ----------
# Misma prueba del paso 7, pero ahora el cierre debe terminar siempre.
def paso_8():
    """Paso 8: misma prueba del paso 7, ahora con el cierre tolerante a pérdidas.

    ``close`` debe terminar siempre. Imprime cuánto tardó el cierre.
    """
    client_socketTCP = paso_5()
    client_socketTCP.send(LONG_MESSAGE)
    print("Cerrando conexión (close)...")
    start = time.time()
    client_socketTCP.close()
    print(f"Conexión cerrada con éxito (close) en {time.time() - start:.1f} s")


TESTS = {"1": paso_1, "3": paso_3, "4": paso_4, "5": paso_5, "6": paso_6, "7": paso_7, "8": paso_8}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in TESTS:
        print(f"Uso: python {sys.argv[0]} <paso> [debug]   pasos disponibles: {', '.join(TESTS)}")
        sys.exit(1)
    TESTS[sys.argv[1]]()