"""
Tests del lado servidor, organizados por paso del enunciado.
Uso:   python test_server.py <paso> [debug]
Ej.:   python test_server.py 5          python test_server.py 5 debug
Ejecutar siempre ANTES que test_client.py con el mismo número de paso.

Cada paso del enunciado que tiene una prueba corresponde a una función
``paso_N``. El paso 2 (constructor) no tiene prueba propia, ya que se ejecuta
cada vez que se crea un SocketTCP. Las pruebas de los pasos 1 y 3 usan sockets
UDP directamente; las de los pasos 4 a 8 usan SocketTCP. Las pruebas con
pérdidas (Test 2 del paso 1, pasos 7 y 8) se ejecutan con netem activo.

Attributes:
    ADDRESS (tuple): Dirección ``(ip, puerto)`` donde escucha el servidor.
    DEBUG (bool): ``True`` si se pasó ``debug`` por línea de comandos.
    LONG_MESSAGE (bytes): Mensaje de 30 líneas (261 bytes) para las pruebas
        con pérdidas, que ocupa varios segmentos.
    TESTS (dict): Relaciona el número de paso (``str``) con su función.
"""
import socket
import sys
import time
import SocketTCP

ADDRESS = ('localhost', 8000)
DEBUG = "debug" in sys.argv


def check(name, received, expected):
    """Imprime el resultado de una prueba y si coincide con lo esperado.

    Args:
        name (str): Nombre de la prueba (por ejemplo, ``"Test 1"``).
        received: Valor recibido.
        expected: Valor esperado. Se compara con ``==``.
    """
    print(f"{name} received: {received}")
    print(f"{name}: {'Passed' if received == expected else 'Failed'}")


def nuevo_socket_servidor():
    """Crea un SocketTCP asociado a ``ADDRESS``, listo para llamar a ``accept``.

    Returns:
        SocketTCP.SocketTCP: Socket que escucha, con el modo debug según
        ``DEBUG``.
    """
    server_socketTCP = SocketTCP.SocketTCP()
    server_socketTCP.debug = DEBUG
    server_socketTCP.bind(ADDRESS)
    return server_socketTCP


# ---------- Paso 1: UDP sin headers (Test 1 sin pérdida / Test 2 con netem) ----------
def paso_1():
    """Paso 1 (Test 1 y Test 2): servidor UDP provisorio, sin headers.

    Recibe trozos de hasta 16 bytes e imprime su contenido. Como aún no se
    conoce el largo del mensaje, se queda esperando al terminar; se detiene
    con Ctrl+C. El Test 2 es la misma prueba con netem activo, para observar
    que se pierden trozos.
    """
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(ADDRESS)
    print("Esperando datos (Ctrl+C para terminar)...")
    while True:
        chunk, _ = udp_socket.recvfrom(16)
        print(chunk.decode(), end="", flush=True)


# ---------- Paso 3: UDP con headers (create_segment / parse_segment) ----------
def paso_3():
    """Paso 3: servidor UDP provisorio que extrae los datos de cada segmento.

    Usa ``parse_segment`` para separar el header de los datos e imprime solo
    los datos, de modo que el mensaje original se rearma en pantalla. Con
    ``DEBUG`` también imprime el número de secuencia de cada segmento. Se
    detiene con Ctrl+C.
    """
    buff = SocketTCP.SocketTCP.header_size + SocketTCP.SocketTCP.data_size
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(ADDRESS)
    print("Esperando datos (Ctrl+C para terminar)...")
    while True:
        segment, _ = udp_socket.recvfrom(buff)
        d = SocketTCP.SocketTCP.parse_segment(segment)
        if DEBUG:
            print(f"\n[seq={d['SEQ']}] ", end="")
        print(d["DATA"].decode(), end="", flush=True)


# ---------- Paso 4: 3-way handshake ----------
def paso_4():
    """Paso 4: lado servidor del 3-way handshake (``bind`` y ``accept``).

    Imprime la dirección del socket que escucha, la del socket nuevo, la del
    cliente y el número de secuencia. La prueba es correcta si el seq coincide
    con el que imprime el cliente y si el destino del cliente es la dirección
    del socket nuevo (con un puerto distinto al del socket que escucha).
    """
    server_socketTCP = nuevo_socket_servidor()
    connection_socketTCP, new_address = server_socketTCP.accept()
    print("Socket que escucha:", server_socketTCP.origin_address)
    print("Socket nuevo:", new_address)
    print("Cliente:", connection_socketTCP.destination_address)
    print("seq:", connection_socketTCP.seq)


# ---------- Paso 5: send y recv ----------
def paso_5():
    """Paso 5: pruebas de ``recv`` (apartado "Test send y recv" del enunciado).

    Ejecuta los Tests 1, 2 y 3 del enunciado y un Test 4 adicional, que envía
    2n bytes con n = 21 (no múltiplo de 16) y los recibe con dos llamadas a
    ``recv(n)``, como se pide en la sección de pruebas.

    Returns:
        SocketTCP.SocketTCP: Socket de la conexión, para que los pasos
        siguientes continúen con el cierre.
    """
    server_socketTCP = nuevo_socket_servidor()
    connection_socketTCP, new_address = server_socketTCP.accept()

    # test 1
    check("Test 1", connection_socketTCP.recv(16), "Mensje de len=16".encode())
    # test 2
    check("Test 2", connection_socketTCP.recv(19), "Mensaje de largo 19".encode())
    # test 3
    part_1 = connection_socketTCP.recv(14)
    part_2 = connection_socketTCP.recv(14)
    check("Test 3", part_1 + part_2, "Mensaje de largo 19".encode())
    # test 4: 2n bytes con n = 21 (no múltiplo de 16), dos recv(n)
    n = 21
    part_1 = connection_socketTCP.recv(n)
    part_2 = connection_socketTCP.recv(n)
    check("Test 4", (part_1, part_2), (b"A" * n, b"B" * n))

    return connection_socketTCP


# ---------- Paso 6: cierre de conexión (tests del paso 5 + cierre) ----------
def paso_6():
    """Paso 6: pruebas del paso 5 seguidas del cierre de conexión (``recv_close``).

    Se ejecuta sin pérdidas.
    """
    connection_socketTCP = paso_5()
    connection_socketTCP.recv_close()
    print("Conexión cerrada con éxito (recv_close)")


# ---------- Paso 7: handshake con pérdidas (usar con netem) ----------
# Mismos tests del paso 5 + un mensaje largo, y luego el cierre.
# El cierre aún no tolera pérdidas (paso 8): si se queda pegado ahí, es esperable.
LONG_MESSAGE = "".join(f"linea {i}\n" for i in range(1, 31)).encode()  # 231 bytes, 15 segmentos


def paso_7():
    """Paso 7: pruebas del paso 5 y un mensaje largo, con pérdidas (netem).

    Verifica que, a pesar de las pérdidas en el handshake y en los datos, el
    mensaje llega íntegro (Test 5). Luego continúa el cierre con
    ``recv_close``.
    """
    connection_socketTCP = paso_5()
    check("Test 5 (mensaje largo)", connection_socketTCP.recv(1000), LONG_MESSAGE)
    print("Datos completos. Cerrando conexión (puede no terminar con pérdidas hasta el paso 8)...")
    connection_socketTCP.recv_close()
    print("Conexión cerrada con éxito (recv_close)")


# ---------- Paso 8: cierre con pérdidas (usar con netem) ----------
# Misma prueba del paso 7, pero ahora el cierre debe terminar siempre.
def paso_8():
    """Paso 8: misma prueba del paso 7, ahora con el cierre tolerante a pérdidas.

    Una vez que ``recv_close`` recibe un FIN, el cierre debe terminar siempre.
    Imprime cuánto tardó el cierre.
    """
    connection_socketTCP = paso_5()
    check('Test 5 (mensaje largo)', connection_socketTCP.recv(1000), LONG_MESSAGE)
    print("Cerrando conexión (recv_close)...")
    start = time.time()
    connection_socketTCP.recv_close()
    print(f"Conexión cerrada con éxito (recv_close) en {time.time() - start:.1f} s")


TESTS = {"1": paso_1, "3": paso_3, "4": paso_4, "5": paso_5, "6": paso_6, "7": paso_7, "8": paso_8}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in TESTS:
        print(f"Uso: python {sys.argv[0]} <paso> [debug]   pasos disponibles: {', '.join(TESTS)}")
        sys.exit(1)
    TESTS[sys.argv[1]]()