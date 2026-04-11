import socket
import threading
import sys

if len(sys.argv) < 2:
    port = 31338
else:
    port = sys.argv[1]

s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('0.0.0.0', 31338))
s.listen(100)
print('TCP echo on :31338')
while True:
    c, _ = s.accept()
    threading.Thread(target=lambda c=c: (c.sendall(c.recv(4096)), c.close()), daemon=True).start()