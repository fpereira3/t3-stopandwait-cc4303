"""TCP simplificado sobre UDP con Stop & Wait.

Actividad "Sockets orientados a conexión con Stop & Wait" (CC4303 Redes, Módulo 3).

Este módulo define la clase ``SocketTCP``, que ofrece una interfaz similar a la de
los sockets orientados a conexión de Python (``bind``, ``connect``, ``accept``,
``send``, ``recv``, ``close``), pero transmite toda la información mediante un
socket UDP interno. Implementa:

* 3-way handshake (SYN, SYN-ACK, ACK) tolerante a pérdidas.
* Transferencia confiable con Stop & Wait: números de secuencia por bytes,
  mensajes ACK, timeout fijo y retransmisión.
* Cierre de conexión (FIN, FIN+ACK, ACK) tolerante a pérdidas.

Formato de un segmento (header de 4 bytes seguido de 0 a 16 bytes de datos)::

    byte 0     bytes 1-3                 bytes 4-19
    [flags]    [seq, entero big endian]  [datos]

    flags: SYN = 0b001, ACK = 0b010, FIN = 0b100 (combinables con OR)

Ejemplo:
    Servidor::

        server = SocketTCP()
        server.bind(("localhost", 8000))
        connection, address = server.accept()
        data = connection.recv(1024)
        connection.recv_close()

    Cliente::

        client = SocketTCP()
        client.connect(("localhost", 8000))
        client.send(b"Hola mundo")
        client.close()
"""
import socket
import random # generar seq
import time

class SocketTCP:
    """Socket orientado a conexión implementado sobre un socket UDP.

    Cada instancia representa un extremo de una conexión de TCP simplificado.
    El socket que llama a ``accept`` solo atiende peticiones de conexión; la
    comunicación posterior ocurre a través del socket nuevo que ``accept``
    retorna, asociado a una dirección distinta.

    Attributes:
        data_size (int): Máximo de bytes de datos por segmento (16).
        header_size (int): Tamaño del header en bytes: 1 byte de flags y 3 de
            número de secuencia.
        SYN (int): Bit del flag SYN dentro del byte de flags.
        ACK (int): Bit del flag ACK dentro del byte de flags.
        FIN (int): Bit del flag FIN dentro del byte de flags.
        socket_udp (socket.socket): Socket UDP interno por el que viajan los
            segmentos.
        origin_address (tuple | None): Dirección ``(ip, puerto)`` propia; se
            fija con ``bind``.
        destination_address (tuple | None): Dirección ``(ip, puerto)`` del
            otro extremo; simula la conexión, ya que UDP no la guarda.
        seq (int | None): Último número de secuencia almacenado. En el emisor
            es el seq del próximo segmento a enviar; en el receptor, el seq del
            próximo segmento esperado.
        timeout (float): Timeout fijo en segundos para las retransmisiones.
        udp_buff_size (int): Tamaño del buffer del socket UDP interno
            (header + datos). Es independiente del ``buff_size`` de ``recv``.
        remaining_bytes (int): Bytes del mensaje actual que aún no llegan.
        recv_buffer (bytes): Bytes ya recibidos que ``recv`` todavía no ha
            retornado por exceder ``buff_size``.
        debug (bool): Si es ``True``, se imprimen mensajes de depuración que
            muestran envíos, recepciones, timeouts y retransmisiones.
    """

    # atributos fijos
    data_size = 16
    header_size = 4 # 1 byte para los mensajes SYN ACK FIN, 3 bytes para seq son aprx hasta 16 millones

    # mensajes se verificaran con operadores binarios
    SYN = 0b001
    ACK = 0b010
    FIN = 0b100

    def __init__(self):
        """Inicializa un SocketTCP sin conexión.

        Crea el socket UDP interno y deja en ``None`` los datos que aún no se
        conocen (direcciones y número de secuencia). No recibe parámetros.
        """
        self.socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # socket udp interno

        # direcciones
        self.origin_address = None # dirección propia fijada por bind en el servidor
        self.destination_address = None # dirección del otro extremo, esto simula conexión

        # Secuencia
        self.seq = None

        # timeout
        self.timeout = 2.0 #2 seg

        # Buff size
        self.udp_buff_size = self.header_size + self.data_size

        # bytes restantes por recibir
        self.remaining_bytes = 0

        # bytes recibidos sin retornar
        self.recv_buffer = b""

        #modo debug
        self.debug = False

    # Funciones
    @staticmethod
    def parse_segment(segment_bytes):
        """Convierte un segmento en bytes a un diccionario.

        Args:
            segment_bytes (bytes): Segmento completo (header + datos) tal como
                se recibió del socket UDP.

        Returns:
            dict: Diccionario con las llaves ``"SYN"``, ``"ACK"`` y ``"FIN"``
            (``bool``), ``"SEQ"`` (``int``) y ``"DATA"`` (``bytes``).
        """
        d = {
            "SYN": (segment_bytes[0] & SocketTCP.SYN) == SocketTCP.SYN, # si el bit de SYN es 1 retorna True sino False
            "ACK": (segment_bytes[0] & SocketTCP.ACK) == SocketTCP.ACK,
            "FIN": (segment_bytes[0] & SocketTCP.FIN) == SocketTCP.FIN,
            "SEQ": int.from_bytes(segment_bytes[1:SocketTCP.header_size], "big"), # bytes a entero, big = byte más significativo primero
            "DATA": segment_bytes[SocketTCP.header_size:]
        }
        return d

    @staticmethod
    def create_segment(d):
        """Construye un segmento en bytes a partir de un diccionario.

        Es la operación inversa de ``parse_segment``: se cumple que
        ``parse_segment(create_segment(d)) == d``.

        Args:
            d (dict): Diccionario con las llaves ``"SYN"``, ``"ACK"``, ``"FIN"``
                (``bool``), ``"SEQ"`` (``int``) y ``"DATA"`` (``bytes`` de a lo
                más ``data_size`` bytes).

        Returns:
            bytes: Segmento listo para enviarse por el socket UDP.

        Raises:
            OverflowError: Si ``d["SEQ"]`` no cabe en 3 bytes (mayor a
                2**24 - 1).
        """
        # parte en 0 y se enciende el bit de cada mensaje presente con |
        tipo_mensaje = 0b000
        if d["SYN"]:
            tipo_mensaje |= SocketTCP.SYN # tipo_mensaje = tipo_mensaje | SYN
        if d["ACK"]:
            tipo_mensaje |= SocketTCP.ACK
        if d["FIN"]:
            tipo_mensaje |= SocketTCP.FIN

        seq_bytes = d["SEQ"].to_bytes(SocketTCP.header_size - 1, "big")  # int a bytes

        return bytes([tipo_mensaje]) + seq_bytes + d["DATA"]  # header + datos
    
    def log(self, msg):
        """Imprime un mensaje de depuración si el modo debug está activo.

        Args:
            msg (str): Mensaje a imprimir. Se antepone la dirección propia del
                socket para distinguir qué socket lo generó.
        """
        if self.debug:
            print(f"DEBUG {self.origin_address} {msg}")

    def bind(self, address):
        """Asocia el socket a una dirección para que pueda ser contactado.

        Args:
            address (tuple): Dirección ``(ip, puerto)``. Con puerto 0, el
                sistema operativo asigna un puerto libre.

        Note:
            ``origin_address`` se obtiene con ``getsockname()``, de modo que
            guarda el puerto real incluso cuando se pidió el puerto 0.
        """
        self.socket_udp.bind(address)
        self.origin_address = self.socket_udp.getsockname()

    def connect(self, address):
        """Establece una conexión con un SocketTCP que escucha en ``address``.

        Implementa el lado del cliente del 3-way handshake con Stop & Wait:
        envía un SYN con un número de secuencia aleatorio ``x`` entre 0 y 100
        y lo reenvía en cada timeout hasta recibir un SYN-ACK con ``x + 1``.
        Luego envía el ACK con ``x + 2``. Si el socket no tiene dirección
        propia, primero se asocia a ``localhost`` con un puerto libre.

        Args:
            address (tuple): Dirección ``(ip, puerto)`` del socket que escucha.

        Returns:
            bool: ``True`` al completar el handshake.

        Note:
            El SYN-ACK proviene del socket nuevo creado por ``accept``, por lo
            que su dirección de origen pasa a ser ``destination_address``.
            Si el ACK final se pierde, el servidor reenviará el SYN-ACK y
            ``send`` o ``recv`` responderán con el mismo ACK.
        """
        if self.origin_address is None:
            self.bind(('localhost', 0))

        self.destination_address = address
        x = random.randint(0, 100)

        # SYN seq = x con stop and wait
        syn = SocketTCP.create_segment({"SYN": True, "ACK": False, "FIN": False, "SEQ": x, "DATA": b""})
        self.socket_udp.settimeout(self.timeout)
        self.socket_udp.sendto(syn, self.destination_address)
        self.log(f"enviado SYN seq={x} a {self.destination_address}")

        # SYN-ACK seq = x + 1
        while True:
            try:
                segment, address_from = self.socket_udp.recvfrom(self.udp_buff_size)
            except socket.timeout: # se perdió el SYN o el SYN-ACK, se reenvía el SYN al socket que escucha
                self.log(f"timeout esperando SYN-ACK, reenviando SYN seq={x}")
                self.socket_udp.sendto(syn, self.destination_address)
                continue

            syn_ack = SocketTCP.parse_segment(segment)
            if syn_ack["SYN"] and syn_ack["ACK"] and (syn_ack["SEQ"] == x + 1):
                self.log(f"recibido SYN-ACK seq={x + 1} desde {address_from}")
                break
            # cualquier otra cosa se ignora
            self.log(f"ignorado {syn_ack}, se esperaba SYN-ACK seq={x + 1}")

        # socket nuevo
        self.destination_address = address_from

        # ACK seq = x + 2
        ack = {"SYN": False, "ACK": True, "FIN": False, "SEQ": x + 2, "DATA": b""}
        self.socket_udp.sendto(SocketTCP.create_segment(ack), self.destination_address)
        self.log(f"enviado ACK seq={x + 2} a {self.destination_address}")

        self.seq = x + 2
        return True


    def accept(self):
        """Espera una petición de conexión y completa el handshake.

        Implementa el lado del servidor del 3-way handshake: espera un SYN en
        el socket que escucha, crea un socket nuevo asociado a la misma IP con
        un puerto libre, y desde él envía el SYN-ACK con ``x + 1`` (con Stop &
        Wait) hasta recibir el ACK con ``x + 2``.

        Si en lugar del ACK llega un segmento de datos con seq ``x + 2``, se
        considera un ACK implícito: el cliente ya terminó ``connect``, por lo
        que recibió el SYN-ACK.

        Returns:
            tuple: ``(new_socket, address)``, donde ``new_socket`` es el
            ``SocketTCP`` que se usará para enviar y recibir datos y
            ``address`` es su dirección ``(ip, puerto)``, distinta de la del
            socket que llamó a ``accept``.
        """
        # SYN seq = x se espera en el socket que escucha
        self.socket_udp.settimeout(None)
        while True:
            segment, client_address = self.socket_udp.recvfrom(self.udp_buff_size)
            syn = SocketTCP.parse_segment(segment)
            if syn["SYN"] and (not syn["ACK"]):
                break
            self.log(f"ignorado {syn}, se esperaba SYN")
        x = syn["SEQ"]
        self.log(f"recibido SYN seq={x} desde {client_address}")

        # socket nuevo
        new_socket = SocketTCP()
        new_socket.bind((self.origin_address[0], 0))
        new_socket.destination_address = client_address
        new_socket.debug = self.debug

        # SYN-ACK seq = x + 1 enviado desde el socket nuevo con Stop and wait
        syn_ack = SocketTCP.create_segment({"SYN": True, "ACK": True, "FIN": False, "SEQ": x + 1, "DATA": b""})
        new_socket.socket_udp.settimeout(new_socket.timeout)
        new_socket.socket_udp.sendto(syn_ack, new_socket.destination_address)
        new_socket.log(f"enviado SYN-ACK seq={x + 1} a {new_socket.destination_address}")

        # ACK seq = x + 2 recibido en el socket nuevo
        while True:
            try:
                segment, address_from = new_socket.socket_udp.recvfrom(new_socket.udp_buff_size)
            except socket.timeout: # se perdio el SYN-ACK o el ACK, se reenvía SYN-ACK
                new_socket.log(f"timeout esperando ACK, reenviando SYN-ACK seq={x + 1}")
                new_socket.socket_udp.sendto(syn_ack, new_socket.destination_address)
                continue

            ack = SocketTCP.parse_segment(segment)
            if ack["ACK"] and (ack["SEQ"] == x + 2):
                new_socket.log(f"recibido ACK seq={x + 2} desde {address_from}")
                break
            if (not ack["SYN"]) and (ack["SEQ"] == x + 2):
                new_socket.log(f"recibido segmento seq={x + 2} en vez de ACK: ACK implícito")
                break
            new_socket.log(f"ignorado {ack}, se esperaba ACK seq={x + 2}")

        new_socket.seq = x + 2
        return new_socket, new_socket.origin_address

    def send(self, message):
        """Envía un mensaje completo usando Stop & Wait.

        El primer segmento contiene el largo del mensaje en bytes (codificado
        como texto); los siguientes contienen el mensaje en trozos de a lo más
        ``data_size`` bytes. Cada segmento se reenvía en cada timeout hasta
        recibir su ACK, cuyo seq debe ser el seq del segmento más la cantidad
        de bytes de datos que lleva. Parte desde el último número de secuencia
        almacenado (``self.seq``) y lo deja actualizado al terminar.

        Args:
            message (bytes): Mensaje a enviar.

        Note:
            Si llega un SYN-ACK repetido (se perdió el último ACK del
            handshake), se reenvía ese ACK y se sigue esperando.
        """
        chunks = [str(len(message)).encode()]
        for i in range(0, len(message), self.data_size):
            chunks.append(message[i:i + self.data_size])

        self.socket_udp.settimeout(self.timeout)

        for data in chunks:
            # manda los segmentos primero partiendo por el largo del mensaje (el primer elemento en chunks)
            segment = SocketTCP.create_segment(
                {"SYN": False, "ACK": False, "FIN": False, "SEQ": self.seq, "DATA": data})
            expected_ack = self.seq + len(data) # x + len data
            self.socket_udp.sendto(segment, self.destination_address)
            self.log(f"enviado seq={self.seq} data={data}")

            # Stop and wait, no se avanza hasta recibir el ACK correspondiente
            while True:
                try:
                    answer, _ = self.socket_udp.recvfrom(self.udp_buff_size)
                except socket.timeout: #timeout se asume perdido el segmento enviado o el ACk que deberia llegar
                    self.log(f"timeout esperando ACK {expected_ack}, reenviando seq={self.seq}") # reenvia el segmento con el seq
                    self.socket_udp.sendto(segment, self.destination_address)
                    continue

                ack = SocketTCP.parse_segment(answer)
                if ack["ACK"] and (ack["SEQ"] == expected_ack): # si seq corresponde a lo que se espera y ACK esta en 1 
                    self.log(f"recibido ACK {expected_ack}")
                    break
                if ack["SYN"] and ack["ACK"]:
                    # caso borde paso 7: se perdió el último ACK del handshake y el servidor
                    # reenvió su SYN-ACK, se responde de nuevo con ese ACK (seq = x + 2)
                    handshake_ack = {"SYN": False, "ACK": True, "FIN": False, "SEQ": ack["SEQ"] + 1, "DATA": b""}
                    self.socket_udp.sendto(SocketTCP.create_segment(handshake_ack), self.destination_address)
                    self.log(f"SYN-ACK repetido, reenviado ACK del handshake seq={ack['SEQ'] + 1}")
                    continue
                # ACK anterior se ignora y se sigue esperando el ACK que corresponde
                self.log(f"ignorado {ack}, se esperaba ACK {expected_ack}")

            self.seq = expected_ack

    def recv(self, buff_size):
        """Recibe datos del mensaje actual usando Stop & Wait.

        Si no hay un mensaje en curso, el primer segmento nuevo que llega
        contiene el largo del mensaje. Retorna apenas se han recibido
        ``min(buff_size, bytes restantes del mensaje)`` bytes, por lo que
        puede llamarse varias veces para recibir un mensaje más largo que
        ``buff_size``. Nunca mezcla datos de dos mensajes distintos.

        Cada segmento nuevo se confirma con un ACK; los duplicados se
        descartan y se vuelve a enviar el último ACK. Los segmentos de control
        (con algún flag) no se consideran datos.

        Args:
            buff_size (int): Cantidad máxima de bytes a retornar. No afecta el
                tamaño del buffer del socket UDP interno.

        Returns:
            bytes: A lo más ``buff_size`` bytes del mensaje. Los bytes
            recibidos que excedan ``buff_size`` se guardan en ``recv_buffer``
            y se entregan en la siguiente llamada.
        """
        self.socket_udp.settimeout(None) # receptor no tiene timeout

        waiting_length = (self.remaining_bytes == 0) and (len(self.recv_buffer) == 0)

        while True:
            if (not waiting_length):
                # recibe hasta tener el menor de (buff size o lo que queda del mensaje)
                target = min(buff_size, len(self.recv_buffer) + self.remaining_bytes)
                if len(self.recv_buffer) >= target:
                    break

            segment, _ = self.socket_udp.recvfrom(self.udp_buff_size)
            d = SocketTCP.parse_segment(segment)

            if d["SYN"] and d["ACK"]:
                # SYN-ACK repetido se reenvía ACK seq = x + 2
                handshake_ack = {"SYN": False, "ACK": True, "FIN": False, "SEQ": d["SEQ"] + 1, "DATA": b""}
                self.socket_udp.sendto(SocketTCP.create_segment(handshake_ack), self.destination_address)
                self.log(f"SYN-ACK repetido, reenviado ACK del handshake seq={d['SEQ'] + 1}")
                continue
            if d["SYN"] or d["ACK"] or d["FIN"]:
                self.log(f"ignorado segmento de control {d}")
                continue

            if d["SEQ"] == self.seq:
                # segmento nuevo avanza seq
                self.seq += len(d["DATA"])
                if waiting_length:
                    self.remaining_bytes = int(d["DATA"].decode())
                    waiting_length = False
                    self.log(f"nuevo mensaje de {self.remaining_bytes} bytes")
                else:
                    self.recv_buffer += d["DATA"]
                    self.remaining_bytes -= len(d["DATA"])
                    self.log(f"recibido seq={d['SEQ']} data={d['DATA']}")
            else:
                # duplicado se descartan
                self.log(f"duplicado seq={d['SEQ']} descartado")

            # se confirma con el seq esperado
            ack = {"SYN": False, "ACK": True, "FIN": False, "SEQ": self.seq, "DATA": b""}
            self.socket_udp.sendto(SocketTCP.create_segment(ack), self.destination_address)
            self.log(f"enviado ACK {self.seq}")

        # retorna máx. buff_size bytes y el resto para la proxima llamada
        result = self.recv_buffer[:buff_size]
        self.recv_buffer = self.recv_buffer[buff_size:]
        return result


    def close(self):
        """Cierra la conexión desde el lado que la termina ("Host A").

        Envía un FIN y espera el FIN+ACK, reenviando el FIN en cada timeout.
        Si tras el tercer timeout no ha llegado, asume que la contraparte se
        cerró. Si llega, envía el ACK final tres veces, esperando un timeout
        después de cada envío, ya que nadie confirma ese ACK. Finalmente,
        cierra el socket UDP interno.
        """
        # HOST A
        self.socket_udp.settimeout(self.timeout)

        # envía FIN seq = x
        fin = SocketTCP.create_segment({"SYN": False, "ACK": False, "FIN": True, "SEQ": self.seq, "DATA": b""})
        self.socket_udp.sendto(fin, self.destination_address)
        self.log(f"enviado FIN seq={self.seq}")

        # recibe FIN+ACK seq = x + 1, se espera hasta 3 timeouts reenviando el FIN
        timeouts = 0
        fin_ack_received = False
        while True:
            try:
                segment, _ = self.socket_udp.recvfrom(self.udp_buff_size)
            except socket.timeout:
                timeouts += 1
                if timeouts == 3: # tercer timeout: se asume que la contraparte se cerró
                    self.log("3 timeouts sin FIN+ACK, se asume que la contraparte se cerró")
                    break
                self.log(f"timeout {timeouts} esperando FIN+ACK, reenviando FIN seq={self.seq}")
                self.socket_udp.sendto(fin, self.destination_address)
                continue

            fin_ack = SocketTCP.parse_segment(segment)
            if fin_ack["FIN"] and fin_ack["ACK"] and (fin_ack["SEQ"] == self.seq + 1):
                self.log(f"recibido FIN+ACK seq={self.seq + 1}")
                fin_ack_received = True
                break
            # ACK viejo u otro segmento se ignora
            self.log(f"ignorado {fin_ack}, se esperaba FIN+ACK seq={self.seq + 1}")

        if fin_ack_received:
            # envía ACK seq = x + 2 tres veces, esperando un timeout entre cada envío
            # (nadie confirma este ACK, así que se repite por si se pierde)
            ack = SocketTCP.create_segment({"SYN": False, "ACK": True, "FIN": False, "SEQ": self.seq + 2, "DATA": b""})
            for i in range(3):
                self.socket_udp.sendto(ack, self.destination_address)
                self.log(f"enviado ACK seq={self.seq + 2} ({i + 1}/3)")
                time.sleep(self.timeout)

        self.socket_udp.close()
        self.log("conexion cerrada")

    def recv_close(self):
        """Continúa el cierre de la conexión desde el otro lado ("Host B").

        Espera indefinidamente un FIN; mientras tanto, responde con el último
        ACK a los segmentos de datos repetidos (por ejemplo, si se perdió el
        ACK del último segmento). Al recibir el FIN, envía el FIN+ACK y espera
        el ACK final, reenviando el FIN+ACK en cada timeout o si llega un FIN
        repetido. Tras el tercer timeout, asume que la contraparte se cerró.
        Finalmente, cierra el socket UDP interno.
        """
        # HOST B
        self.socket_udp.settimeout(None) # el FIN se espera sin límite

        # recibe FIN seq = x
        while True:
            segment, _ = self.socket_udp.recvfrom(self.udp_buff_size)
            fin = SocketTCP.parse_segment(segment)
            if fin["FIN"]:
                break
            # segmento repetido se reenvia ACK
            ack = {"SYN": False, "ACK": True, "FIN": False, "SEQ": self.seq, "DATA": b""}
            self.socket_udp.sendto(SocketTCP.create_segment(ack), self.destination_address)
            self.log(f"duplicado seq={fin['SEQ']} mientras se esperaba FIN, reenviado ACK {self.seq}")
        s = fin["SEQ"]
        self.log(f"recibido FIN seq={s}")

        # envía FIN+ACK seq = x + 1
        fin_ack = SocketTCP.create_segment({"SYN": False, "ACK": True, "FIN": True, "SEQ": s + 1, "DATA": b""})
        self.socket_udp.sendto(fin_ack, self.destination_address)
        self.log(f"enviado FIN+ACK seq={s + 1}")

        # recibe ACK seq = x + 2, se espera hasta 3 timeouts reenviando el FIN+ACK
        self.socket_udp.settimeout(self.timeout)
        timeouts = 0
        while True:
            try:
                segment, _ = self.socket_udp.recvfrom(self.udp_buff_size)
            except socket.timeout:
                timeouts += 1
                if timeouts == 3: # tercer timeout: se asume que la contraparte se cerró
                    self.log("3 timeouts sin ACK, se asume que la contraparte se cerró")
                    break
                self.log(f"timeout {timeouts} esperando ACK, reenviando FIN+ACK seq={s + 1}")
                self.socket_udp.sendto(fin_ack, self.destination_address)
                continue

            ack = SocketTCP.parse_segment(segment)
            if ack["ACK"] and (not ack["FIN"]) and (ack["SEQ"] == s + 2):
                self.log(f"recibido ACK seq={s + 2}")
                break
            if ack["FIN"]:
                # FIN repetido: se perdió nuestro FIN+ACK, se reenvía
                self.socket_udp.sendto(fin_ack, self.destination_address)
                self.log(f"FIN repetido, reenviado FIN+ACK seq={s + 1}")
                continue
            self.log(f"ignorado {ack}, se esperaba ACK seq={s + 2}")

        self.socket_udp.close()
        self.log("conexion cerrada")