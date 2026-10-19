import socket
import random
import time

class SocketTCP():
   
    SYN = 0x01   # 00000001
    ACK = 0x02   # 00000010
    FIN = 0x04   # 00000100

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.address = ('localhost', 5000)
        self.SEQ = 0
        self.socket.settimeout(2.0)
        self.bytes_por_recibir = 0
        self.buffer_interno = b''


    @staticmethod
    def parse_segment(segment):
        ##errores de que el header llego mal
        if not isinstance(segment, (bytes, bytearray)):
            raise TypeError("segment debe ser bytes")

        if len(segment) < 2:
            raise ValueError("Segmento demasiado corto")
        ##primer byte de flags, segundo byte de seq, y el resto es data
        flags = segment[0] 
        ack = False
        syn = False
        fin = False
        if (flags & SocketTCP.ACK) != 0:
            ack = True
        if (flags & SocketTCP.SYN) != 0:
            syn = True
        if (flags & SocketTCP.FIN) != 0:
            fin = True
        return {
            'seq': segment[1],
            'ack': ack,
            'syn': syn,
            'fin': fin,
            'data': bytes(segment[2:])
        }

    

    @staticmethod
    def create_segment(segment):
        seq = int(segment.get('seq', 0))

        data = segment.get('data', b'')
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data debe ser bytes")
        flags = 0
        if segment.get('syn', False):
            flags |= SocketTCP.SYN
        if segment.get('ack', False):
            flags |= SocketTCP.ACK
        if segment.get('fin', False):
            flags |= SocketTCP.FIN

        return bytes([flags, seq]) + bytes(data)




    def bind(self,address) : 
        self.socket.bind(address)
        self.address = address
        return address
        

    
    def connect(self, address): 
        #ahora el handshake client
        self.SEQ = random.randint(0, 100)
        respuesta = self.create_segment({ 'seq': self.SEQ, 'syn': True })
        while True:
            try:
                self.socket.sendto(respuesta, address) #salto accept
                response_bytes, address_nueva = self.socket.recvfrom(1024) ##Esperamos respuesta
                response = self.parse_segment(response_bytes)
                if response['syn'] == True and response['ack'] == True:
                    break
            except (socket.timeout, TimeoutError):
                continue
        self.SEQ = self.SEQ + 1
        respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
        self.socket.sendto(respuesta, address_nueva) #salto a accept
        self.address = address_nueva

        return self.address
    

    def accept(self): 
        ##handshake server
        self.socket.settimeout(None)
        message, address = self.socket.recvfrom(1024)
        #ahora nos deberia llegar un SYN del cliente conect
        mensaje_desempaquetado = self.parse_segment(message)
        if mensaje_desempaquetado['syn'] == True:
            #preparamos un nuevo socket, para que conecte en la llamada
            new_socket = SocketTCP()
            nuevo_puerto = random.randint(5001, 8000)
            new_socket.bind(('localhost', nuevo_puerto))
            new_socket.SEQ = random.randint(0, 100)
            respuesta = self.create_segment({'syn': True, 'ack': True, 'seq': new_socket.SEQ})
            
            seq_cliente = mensaje_desempaquetado['seq']
            seq_esperado = (seq_cliente + 1) % 256

            new_socket.socket.settimeout(2)
            while True:
                try:
                    new_socket.socket.sendto(respuesta, address)
                    message, address = new_socket.socket.recvfrom(1024)
                    mensaje_desempaquetado = self.parse_segment(message)
                    if mensaje_desempaquetado['syn'] == False and mensaje_desempaquetado['seq'] == seq_esperado:
                        break
                except (socket.timeout, TimeoutError):
                    pass  
            
            return new_socket, address


    def send(self, message):
        #emisor
        largo = len(message)
        seq_esperado = (self.SEQ + len(str(largo).encode())) % 256
        llego_length = False
        while not llego_length:
            try:
                respuesta = self.create_segment({'seq': self.SEQ, 'data': str(largo).encode()})
                self.socket.sendto(respuesta, self.address)
                
                response_bytes, address = self.socket.recvfrom(2 + 16)
                response = self.parse_segment(response_bytes)
                if response['ack'] and not response['syn'] and response['seq'] == seq_esperado:
                    llego_length = True
                    self.SEQ = seq_esperado
            except (socket.timeout, TimeoutError):
                pass  

        #ahora se manda el mensaje en trozos de 16 bytes
        trozo = 16
        for i in range(0, len(message), trozo):
            trozo_actual = message[i:i+trozo]
            seq_esperado = (self.SEQ + len(trozo_actual)) % 256
            llego_trozo = False
            while not llego_trozo:
                try:
                    respuesta = self.create_segment({'seq': self.SEQ, 'data': trozo_actual})
                    self.socket.sendto(respuesta, self.address)
                    
                    response_bytes, address = self.socket.recvfrom(2 + 16)
                    response = self.parse_segment(response_bytes)
                    if response['ack'] and not response['syn'] and response['seq'] == seq_esperado:
                        llego_trozo = True
                        self.SEQ = seq_esperado
                except (socket.timeout, TimeoutError):
                    pass


    def recv(self, buff_size):
        #receptor
        #recibimos el largo del mensaje primero
        if self.bytes_por_recibir == 0 and len(self.buffer_interno) == 0:
            while True:
                try:
                    message, address = self.socket.recvfrom(2 + 16)
                    mensaje_desempaquetado = self.parse_segment(message)
                    if mensaje_desempaquetado['data']:
                        self.bytes_por_recibir = int(mensaje_desempaquetado['data'].decode())
                        self.SEQ = (mensaje_desempaquetado['seq'] + len(mensaje_desempaquetado['data'])) % 256
                        respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
                        self.socket.sendto(respuesta, address)
                        break
                except (socket.timeout, TimeoutError):
                    pass

        #recibimos los datos en trozos
        while len(self.buffer_interno) < buff_size and self.bytes_por_recibir > 0:
            try:
                message, address = self.socket.recvfrom(2 + 16)
                mensaje_desempaquetado = self.parse_segment(message)
                
                if mensaje_desempaquetado['seq'] == self.SEQ:
                    #correcto seq
                    self.buffer_interno += mensaje_desempaquetado['data']
                    self.bytes_por_recibir -= len(mensaje_desempaquetado['data'])
                    
                    self.SEQ = (self.SEQ + len(mensaje_desempaquetado['data'])) % 256
                    respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
                    self.socket.sendto(respuesta, address)
                else:
                    #paquete perdido, reenviar el que corresponde
                    respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
                    self.socket.sendto(respuesta, address)
            except (socket.timeout, TimeoutError):
                pass

        #recortar los bytes a retornar
        retorno_final = min(buff_size, len(self.buffer_interno))
        datos_a_retornar = self.buffer_interno[:retorno_final]
        self.buffer_interno = self.buffer_interno[retorno_final:]
        return datos_a_retornar

    def close(self):
        contador_timeouts = 3
        respuesta = self.create_segment({'seq': self.SEQ, 'fin': True})
        self.socket.sendto(respuesta, self.address) #llega el mensaje

        while contador_timeouts > 0: 
            try:
                message, address = self.socket.recvfrom(1024)
                mensaje_desempaquetado = self.parse_segment(message)
                if mensaje_desempaquetado['ack'] == True and mensaje_desempaquetado['fin'] == True:
                    respuesta = self.create_segment({'ack': True, 'seq': (self.SEQ + 1) % 256})
                    for _ in range(3):
                        self.socket.sendto(respuesta, self.address)
                        time.sleep(2)
                    break
            except (socket.timeout, TimeoutError):
                contador_timeouts -= 1
                self.socket.sendto(respuesta, self.address)
        self.socket.close()

    def recv_close(self):
        # host b: espera el primer FIN con reintentos por timeout
        contador_timeouts = 3
        while contador_timeouts > 0:
            try:
                message, address = self.socket.recvfrom(1024)
                mensaje_desempaquetado = self.parse_segment(message)
                if mensaje_desempaquetado['fin'] == True:
                    respuesta = self.create_segment({'ack': True, 'fin': True, 'seq': (self.SEQ + 1) % 256})
                    self.socket.sendto(respuesta, address)
                    
                    # Ahora espera el último ACK hasta 3 timeouts
                    contador_ack = 3
                    while contador_ack > 0:
                        try:
                            message, address = self.socket.recvfrom(1024)
                            mensaje_ack = self.parse_segment(message)
                            if mensaje_ack['ack'] == True:
                                break
                        except (socket.timeout, TimeoutError, ConnectionResetError):
                            contador_ack -= 1
                            if contador_ack > 0:
                                self.socket.sendto(respuesta, address)
                    break
                else:
                    ack_duplicado = self.create_segment({'ack': True, 'seq': self.SEQ})
                    self.socket.sendto(ack_duplicado, address)
            except (socket.timeout, TimeoutError, ConnectionResetError):
                contador_timeouts -= 1
        
        self.socket.close()


