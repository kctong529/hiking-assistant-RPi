import bluetooth
import time
import json
from datetime import datetime, timezone
import hike

WATCH_BT_MAC = '94:B5:55:C8:DF:AE'
WATCH_BT_PORT = 1
BT_PROTOCOL_VERSION = 2


class HubBluetooth:
    """Handles RFCOMM Bluetooth synchronization with the watch.

    Breaking protocol change (v2):
        - Hub sends HELLO|2 and TIME_SYNC|<unix_epoch> immediately after connect.
        - Watch must acknowledge with HELLO_ACK|2 and TIME_SYNC_ACK|<unix_epoch>.
        - Hub requests sessions with SYNC_PULL.
        - Watch sends SESSION|<json> one at a time.
        - Hub acknowledges each stored session with SESSION_ACK|<session_id>.
        - Watch ends the sync cycle with SYNC_DONE.
    """

    connected = False
    sock = None

    def send_line(self, message: str):
        if self.sock is None:
            raise RuntimeError("Bluetooth socket is not connected")
        payload = f"{message}\n".encode("utf-8")
        self.sock.send(payload)
        print(f"Hub -> Watch: {message}")

    def current_unix_epoch(self) -> int:
        return int(datetime.now(timezone.utc).timestamp())

    def perform_handshake(self):
        self.send_line(f"HELLO|{BT_PROTOCOL_VERSION}")
        self.send_line(f"TIME_SYNC|{self.current_unix_epoch()}")
        self.send_line("SYNC_PULL")

    def wait_for_connection(self):
        if not self.connected:
            while True:
                print("Waiting for connection...")
                try:
                    self.sock = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
                    self.sock.connect((WATCH_BT_MAC, WATCH_BT_PORT))
                    self.sock.settimeout(2)
                    self.connected = True
                    self.perform_handshake()
                    print("Connected to Watch!")
                    break
                except bluetooth.btcommon.BluetoothError:
                    time.sleep(1)
                except Exception as e:
                    print(e)
                    print("Hub: Error occured while trying to connect to the Watch.")

            print("Hub: Established Bluetooth connection with Watch!")
            return

        print("WARNING Hub: Bluetooth is already connected.")

    def synchronize(self, callback):
        print("Synchronizing with watch...")
        remainder = b''
        while True:
            try:
                chunk = self.sock.recv(1024)
                if not chunk:
                    continue

                messages = chunk.split(b'\n')
                messages[0] = remainder + messages[0]
                remainder = messages.pop()

                for raw in messages:
                    line = raw.decode("utf-8").strip()
                    if not line:
                        continue

                    print(f"Watch -> Hub: {line}")

                    if line.startswith("HELLO_ACK|"):
                        continue

                    if line.startswith("TIME_SYNC_ACK|"):
                        continue

                    if line.startswith("TIME_SYNC_NACK|"):
                        raise RuntimeError(f"Watch rejected time sync: {line}")

                    if line == "SYNC_DONE":
                        print("Watch reported sync completion.")
                        return

                    if line.startswith("SESSION|"):
                        session = HubBluetooth.session_line_to_session(line)
                        callback([session])
                        self.send_line(f"SESSION_ACK|{session.session_id}")
                        continue

                    print(f"Ignoring unrecognized line: {line}")

            except KeyboardInterrupt:
                self.sock.close()
                raise KeyboardInterrupt("Shutting down the receiver.")

            except bluetooth.btcommon.BluetoothError as bt_err:
                if bt_err.errno == 11:
                    print("Lost connection with the watch.")
                    self.connected = False
                    self.sock.close()
                    break
                elif bt_err.errno is None:
                    self.send_line("SYNC_PULL")
                    print("Reminder sent to the watch to continue synchronization.")
                else:
                    raise

    @staticmethod
    def messages_to_sessions(messages: list[bytes]) -> list[hike.HikeSession]:
        sessions = []
        for msg in messages:
            if not msg.strip():
                continue
            try:
                sessions.append(HubBluetooth.mtos(msg))
            except Exception as e:
                print(f"Skipping corrupted message: {e}")
        return sessions

    @staticmethod
    def session_line_to_session(line: str) -> hike.HikeSession:
        payload = line.split("|", 1)[1]
        data = json.loads(payload)

        hs = hike.HikeSession()
        hs.session_id = data.get("session_id", "")
        hs.start_time = data.get("start_time", "")
        hs.end_time = data.get("end_time", "")
        hs.steps = int(data.get("steps", 0))
        hs.distance_m = int(data.get("distance_m", 0))
        hs.duration_s = int(data.get("duration_s", 0))
        hs.created_at = datetime.now().isoformat(timespec="seconds")
        return hs

    @staticmethod
    def mtos(message: bytes) -> hike.HikeSession:
        data = json.loads(message.decode('utf-8'))

        hs = hike.HikeSession()
        hs.session_id = data.get("session_id", "")
        hs.start_time = data.get("start_time", "")
        hs.end_time = data.get("end_time", "")
        hs.steps = int(data.get("steps", 0))
        hs.distance_m = int(data.get("distance_m", 0))
        hs.duration_s = int(data.get("duration_s", 0))

        return hs
