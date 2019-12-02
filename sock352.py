# Authors: Jake Zhou xz346, Mark Tarlow

import socket as syssock
import struct
import random
import time
from threading import Thread, Lock


# Our flags
SOCK352_SYN = 0x01
SOCK352_FIN = 0x02
SOCK352_ACK = 0x04
SOCK352_RESET = 0x08
SOCK352_HAS_OPT = 0xA0

# Defining init variables at module level
send_port = 0
recv_port = 0

# The max packet size we will use. 60,000 bytes of data
MAX_PACKET_SIZE = 60000

# The size of our receiving buffer window. 262,000 bytes of data
MAX_BUFFER_SIZE = 262000

# For when we need a lock while multi-threading
mutex = Lock()

# these functions are global to the class and
# define the UDP ports all messages are sent
# and received from


def init(UDPportTx, UDPportRx):  # initialize your UDP socket here
    # Saving the send port and receiving port for global access and assigning them to the values from server or client
    global send_port, recv_port
    send_port = int(UDPportTx)
    recv_port = int(UDPportRx)
    # Remainder of socket initialization at __init__


class socket:

    def __init__(self):  # fill in your code here
        # Setting up UDP socket for this instance of sock352
        self.udp_socket = syssock.socket(syssock.AF_INET, syssock.SOCK_DGRAM)

        # If this socket is a server, it knows if it is already connected. Send RESET flag during 3 way handshake
        self.server_connected = False
        # Store the address of where we would send to
        self.sending_addr = ''
        # The packets to send once the sending buffer is decomposed
        self.remaining_packets = []
        # Flag to let us know if we timed out in a thread
        self.timed_out = False
        # Counter to see how many bytes we sent in send
        self.bytes_sent = 0
        # Where we store data that we are receiving
        self.buffer = b''
        # The size of the available space in the buffer
        self.available_buffer = MAX_BUFFER_SIZE
        # Expected sequence number of the next byte to receive
        self.exp_seq_no = 0
        # Track if we should ignore update to exp_seq_no for first recv of file size
        self.first = True

        print('sock352 Socket created!')

        return

    def bind(self, address):
        # Server listens from anywhere at given recv_port
        self.udp_socket.bind(('0.0.0.0', recv_port))
        return

    # Client side of 3 way handshake to establish connection
    def connect(self, address):  # fill in your code here

        # Save the address we would send to as client
        self.sending_addr = address[0]

        # Setting the timeout value for the client
        self.udp_socket.settimeout(0.2)
        # Binding since only server socket is bound
        self.udp_socket.bind(('0.0.0.0', recv_port))

        # STEP 1 - Send SYN packet
        # Need rand seq_no for SYN message
        seq_no = random.randint(0, 100)
        # SEND SYN to server until we get ACK
        syn_packet = Packet(SOCK352_SYN, seq_no, 0, 0, b'').pack_self()
        server_syn_packet = None

        while server_syn_packet is None:
            try:
                self.udp_socket.sendto(syn_packet, (self.sending_addr, send_port))
                server_syn_packet = self.recv_packet()
            except syssock.timeout:
                # If we timeout on receiving the SYN/ACK packet, send again
                print('No ACK received for initial SYN. Resending')
                pass

        # STEP 2 - Receive ACK packet
        if server_syn_packet.flags == SOCK352_RESET:
            print('Server already has Sock352 Connection. Aborting connection.')
            return
        elif server_syn_packet.flags == SOCK352_SYN | SOCK352_ACK:
            if server_syn_packet.ack_no != (seq_no + 1):
                print('Three way handshake failed. Invalid ACK during Client step 2. Repeating attempt')
                return self.connect(address)

        # STEP 3
        ack_packet = Packet(SOCK352_ACK, 0, (server_syn_packet.sequence_no + 1), 0, b'').pack_self()
        self.udp_socket.sendto(ack_packet, (address[0], send_port))

        print('Client: Connection established')
        return

    def listen(self, backlog):
        return

    def accept(self):
        # Not using recv_packet(), it doesn't return the address. This is the only case we need the address

        # Don't timeout as the server
        self.udp_socket.settimeout(None)

        # Step 1 - Receive packet from client
        client_syn_packed_packet = None
        client_addr = None

        while client_syn_packed_packet is None\
                and client_addr is None:
            (client_syn_packed_packet, client_addr) = self.udp_socket.recvfrom(MAX_PACKET_SIZE + 40)

        # Save the address we would send to as server
        self.sending_addr = client_addr[0]

        # If we receive anything besides the syn packet, something went wrong. Restart accept
        try:
            overhead = struct.unpack_from('!BBBBHHLLQQLL', client_syn_packed_packet[:40])
            flags = overhead[1]
            sequence_no = overhead[8]
            ack_no = overhead[9]
            payload_len = overhead[11]
            client_syn_packet = Packet(flags, sequence_no, ack_no, payload_len, b'')
            if client_syn_packet.flags != SOCK352_SYN:
                return self.accept()
        except TypeError:
            return self.accept()

        # Step 2 - Send SYN packet back to client
        if self.server_connected:
            syn_flag = SOCK352_RESET
        else:
            syn_flag = SOCK352_SYN | SOCK352_ACK
        seq_no = random.randint(0, 100)
        syn_packet = Packet(syn_flag, seq_no, (client_syn_packet.sequence_no + 1), 0, b'').pack_self()

        # Step 2 + 3 - (Keep sending from Step 2) Receive ACK from client
        ack_packet = None

        while ack_packet is None:
            self.udp_socket.sendto(syn_packet, client_addr)
            ack_packet = self.recv_packet()

        if ack_packet.flags != SOCK352_ACK\
                or ack_packet.ack_no != (seq_no + 1):
            # Something went wrong in 3 way handshake
            print('Three way handshake failed. Invalid ACK during Server step 3. Reattempting accept')
            return self.accept()

        # Safe to connect udp socket
        self.server_connected = True

        print('Server: Connection established')

        return self, client_addr

    def send(self, buffer):

        # No bytes sent yet for this send call
        self.bytes_sent = 0

        # Break up the buffer into 60,000 byte packets
        print('Breaking file into packet segments. This may take a while for large files')
        remainder = len(buffer)
        packet_seq_no = 0
        while remainder > 0:
            # Biting off a chunk for this packet
            segment = buffer[:MAX_PACKET_SIZE]
            buffer = buffer[MAX_PACKET_SIZE:]
            # Creating a packet from this chunk and add it to the list
            new_packet = Packet(0x00, packet_seq_no, 0, len(segment), segment)
            self.remaining_packets.append(new_packet)
            # Increment the seq for the next packet, and decrement the remaining bytes to send
            packet_seq_no += len(segment)
            remainder -= len(segment)
            print(f'File segment {new_packet.sequence_no} created with size: {new_packet.payload_len}')

        # Keep sending streams of remaining packets until none left
        # Streams will not send until there is room in the receiving buffer
        while len(self.remaining_packets) > 0:
            self.send_stream()

        # We want to clear out the link for the final ACK for new window size before next call
        try:
            print(f'\nChecking for ACK with available buffer')
            self.recv_ack()
        except syssock.timeout:
            print('\nTimed out while waiting for buffer size')
            pass

        return self.bytes_sent

    # Sends packets that will fit in the window
    def send_stream(self):

        print('client sending stream of packets')
        # List of our threads
        thread_list = []

        # We don't check for available window ACK before we send
        if self.first:
            self.first = False
        else:
            # Get a new window size if we can't send a single packet
            try:
                print(f'\nChecking for ACK with available buffer')
                self.recv_ack()
            except syssock.timeout:
                print('\nTimed out while waiting for buffer size')
                pass

        # index to keep track of the packets we are sending
        index = 0

        print(f'\nPreparing to send packets.')

        # Keep sending packets that would fit in the receiver window
        while index < len(self.remaining_packets) and \
                self.remaining_packets[index].payload_len < self.available_buffer:
            packet = self.remaining_packets[index]
            # If we have timed out. Stop sending packets. Reading in mutex so we are sure of the state
            # send_stream() will be called again when send() sees there are remaining packets
            with mutex:
                if self.timed_out:
                    print(f'packet dropped. Timed out on ACK')
                    break

            print(f'Sending packet with sequence number: {packet.sequence_no}')
            self.udp_socket.sendto(packet.pack_self(), (self.sending_addr, send_port))
            # updating the window size with lock just in case one of the threads access it first
            with mutex:
                self.available_buffer -= packet.payload_len
            # Create a new thread to start the ACK timer
            ack_thread = Thread(target=self.recv_ack, args=())
            thread_list.append(ack_thread)
            ack_thread.start()

            index += 1

        # Ensuring we will have no zombie threads
        for thread in thread_list:
            thread.join()

        # Resetting time out flag
        self.timed_out = False

        print('\nEnd of send stream')
        return

    # This called every time a packet is sent. Will throw timeout that is caught before send_all_packets
    def recv_ack(self):
        # No need to run if we have already timed out on another thread. Reading with mutex to ensure true state
        with mutex:
            if self.timed_out:
                return
        # try to recv ACK until we timeout
        try:
            ack_packet = self.recv_packet()
        except syssock.timeout:
            print("ACK thread timed out")
            # Letting socket know we timed out on recv
            with mutex:
                # Only need to do this if another thread has not already reported timeout
                if self.timed_out is False:
                    print('Socket timed out. Resending remaining unacked packets')
                    self.timed_out = True
            # Ending this thread
            return

        # Locking while we update the list and number of bytes sent and the available window size
        with mutex:
            while len(self.remaining_packets) > 0 and ack_packet.ack_no > self.remaining_packets[0].sequence_no:
                print(f'Removing packet from list of packets to send with sequence number: {self.remaining_packets[0].sequence_no}')
                del self.remaining_packets[0]
            self.bytes_sent = ack_packet.ack_no
            self.available_buffer = ack_packet.sequence_no
            print(f'Received ACK for {self.bytes_sent} bytes. Buffer has {self.available_buffer} bytes available')

        return

    def recv(self, nbytes):

        print("\nSize of buffer: " + str(len(self.buffer))
              + "\nSize of nbytes: " + str(nbytes))

        # Keep receiving packets until we have enough data to return
        while len(self.buffer) < nbytes and self.available_buffer > 0:

            print(f'\nTrying to receive a packet' +
                  f'\nSize of buffer data: {len(self.buffer)}' +
                  f'\nExpected sequence number: {self.exp_seq_no}' +
                  f'\nBuffer available: {self.available_buffer}')

            # Receive a packet
            packet = self.recv_packet()
            # Check if this is the packet we are expecting or if we dropped one
            if packet.sequence_no == self.exp_seq_no:
                print(f'\nReceived packet with sequence number: {packet.sequence_no} and size: {packet.payload_len}\n')

                # Increase the expected sequence number to be the next expected sequence number. Also the ack no
                self.exp_seq_no += packet.payload_len
                # Add packet payload to our buffer
                self.buffer += packet.data
                # Decrease the size that we have in the buffer
                self.available_buffer -= packet.payload_len

                # Send ACK for packet
                # We will use the sequence number field to indicate remaining buffer size
                # We will use the ack number field to indicate next expected byte
                ack_packet = Packet(SOCK352_ACK, self.available_buffer, self.exp_seq_no, 0, b'').pack_self()
                self.udp_socket.sendto(ack_packet, (self.sending_addr, send_port))

        # Taking chunk of requested data to return
        if len(self.buffer) < nbytes:
            chunk = self.buffer
            self.buffer = b''
            self.available_buffer += len(chunk)
        else:
            chunk = self.buffer[:nbytes]
            self.buffer = self.buffer[nbytes:]
            self.available_buffer += nbytes

        # Send ack with updated window size
        window_ack_packet = Packet(SOCK352_ACK, self.available_buffer, 0, 0, b'').pack_self()
        self.udp_socket.sendto(window_ack_packet, (self.sending_addr, send_port))

        # Must reset the expected sequence number after sending file size
        if self.first:
            self.first = False
            self.exp_seq_no = 0

        print("data received")

        # Returning requested data
        return chunk

    # Calls a UDP recvfrom and Returns a Packet from the data received
    def recv_packet(self):
        (struct_packet, recv_addr) = self.udp_socket.recvfrom(MAX_PACKET_SIZE + 40)
        # Unpacking header
        overhead = struct.unpack_from('!BBBBHHLLQQLL', struct_packet[:40])
        flags = overhead[1]
        sequence_no = overhead[8]
        ack_no = overhead[9]
        payload_len = overhead[11]
        # Unpacking data
        data = struct.unpack_from(str(payload_len) + 's', struct_packet[40:])[0]

        return Packet(flags, sequence_no, ack_no, payload_len, data)

    # Two way double handshake to tear this bad boy down
    # Each side sends FIN, receives FIN, sends ACK, receive ACK
    def close(self):  # fill in your code here

        # We'll give both sockets .2 seconds to close. If something goes wrong, reattempt both sides to close
        self.udp_socket.settimeout(.2)

        while True:
            try:
                # Send FIN
                seq_no = random.randint(0, 100)
                send_fin_packet = Packet(SOCK352_FIN, seq_no, 0, 0, b'').pack_self()
                self.udp_socket.sendto(send_fin_packet, (self.sending_addr, send_port))

                # Receive FIN
                receive_fin_packet = self.recv_packet()
                # Stop accepting any packets except an FIN packet
                if receive_fin_packet.flags != SOCK352_FIN:
                    print('Received non-FIN packet in connection teardown. Reattempting close')
                    # Wait .3 seconds for the other side to timeout, then reattempt close
                    time.sleep(.3)
                    continue

                # Send ACK
                send_ack_packet = Packet(SOCK352_ACK, 0, (receive_fin_packet.sequence_no + 1), 0, b'').pack_self()
                self.udp_socket.sendto(send_ack_packet, (self.sending_addr, send_port))

                # Receive ACK
                # At this point, even if something went wrong, the other socket closed
                # No point checking values of this ack packet
                receive_ack_packet = self.recv_packet()

                break
            except syssock.timeout:
                print('Timed out on close. Reattempting')
                # Wait .3 seconds for other socket to timeout, then try again from the top
                time.sleep(.3)

        # Good to close down
        self.udp_socket.close()
        print('Connection Closed')

        return


# For our packet class, we need all the header fields
# We will need to change the flag field, sequence_no field, ack_no field, and payload_len
class Packet:

    def __init__(self, in_flags, in_seq_no, in_ack_no, in_payload_len, in_data):
        # These fields never change for our packets in Part 1
        self.version = 0x1
        self.opt_ptr = 0
        self.protocol = 0
        self.header_len = 40
        self.checksum = 0
        self.source_port = 0
        self.dest_port = 0
        self.window = 0

        # These fields depend on packet creation parameters
        self.flags = in_flags
        self.sequence_no = in_seq_no
        self.ack_no = in_ack_no
        self.payload_len = in_payload_len
        self.data = in_data

        # Python struct packing
        self.sock352PktHdrData = '!BBBBHHLLQQLL' + str(self.payload_len) + 's'
        self.udpPkt_header_data = struct.Struct(self.sock352PktHdrData)

    # Need this since we can't return packed struct of ourself in init :<
    def pack_self(self):
        return self.udpPkt_header_data.pack(self.version, self.flags, self.opt_ptr, self.protocol, self.header_len, self.checksum, self.source_port, self.dest_port, self.sequence_no, self.ack_no, self.window, self.payload_len, self.data)
