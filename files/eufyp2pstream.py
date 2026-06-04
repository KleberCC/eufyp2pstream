from websocket import EufySecurityWebSocket
import aiohttp
import asyncio
import json
import socket
import select
import threading
import time
import sys
import signal
import os
from queue import Empty, Queue
from typing import Any, Optional, Tuple

RECV_CHUNK_SIZE = 8192
SOCKET_BUFFER_SIZE = 262144
MAX_CAMERAS = int(os.getenv("EUFY_MAX_CAMERAS", "5"))
MAX_CAMERAS = max(1, min(MAX_CAMERAS, 5))
BASE_PORT = int(os.getenv("EUFY_BASE_PORT", "63336"))
PORTS_PER_CAMERA = 3

EVENT_CONFIGURATION: dict = {
    "livestream video data": {"name": "video_data", "value": "buffer", "type": "event"},
    "livestream audio data": {"name": "audio_data", "value": "buffer", "type": "event"},
}

START_P2P_LIVESTREAM_MESSAGE = {"messageId": "start_livestream", "command": "device.start_livestream", "serialNumber": None}
STOP_P2P_LIVESTREAM_MESSAGE = {"messageId": "stop_livestream", "command": "device.stop_livestream", "serialNumber": None}
START_TALKBACK = {"messageId": "start_talkback", "command": "device.start_talkback", "serialNumber": None}
SEND_TALKBACK_AUDIO_DATA = {"messageId": "talkback_audio_data", "command": "device.talkback_audio_data", "serialNumber": None, "buffer": None}
STOP_TALKBACK = {"messageId": "stop_talkback", "command": "device.stop_talkback", "serialNumber": None}
SET_API_SCHEMA = {"messageId": "set_api_schema", "command": "set_api_schema", "schemaVersion": 13}
START_LISTENING_MESSAGE = {"messageId": "start_listening", "command": "start_listening"}
DRIVER_CONNECT_MESSAGE = {"messageId": "driver_connect", "command": "driver.connect"}

run_event = threading.Event()

def exit_handler(signum, frame):
    print(f"Signal handler called with signal {signum}")
    run_event.set()

signal.signal(signal.SIGINT, exit_handler)
signal.signal(signal.SIGTERM, exit_handler)


def create_server_socket(port: int, keepalive: bool = False) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if keepalive:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_SIZE)
    sock.bind(("0.0.0.0", port))
    sock.settimeout(0.5)
    sock.listen(32)
    return sock


class ClientAcceptThread(threading.Thread):
    def __init__(self, server_socket, run_event, name, connector):
        super().__init__(daemon=True)
        self.socket = server_socket
        self.queues = []
        self.run_event = run_event
        self.name = name
        self.connector = connector
        self.my_threads = []
        self.last_cleanup_time = 0
        self.ready_to_accept = threading.Event()
        self.skip_non_idr = self.name == "Video"
        print(f"{self.connector.prefix}[{self.name}] ClientAcceptThread initialized, skip_non_idr={self.skip_non_idr}")
        sys.stdout.flush()

    def update_threads(self):
        my_threads_before = len(self.my_threads)
        for thread in list(self.my_threads):
            if not thread.is_alive():
                try:
                    self.queues.remove(thread.queue)
                except ValueError:
                    pass
        self.my_threads = [t for t in self.my_threads if t.is_alive()]

        current_time = time.time()
        if my_threads_before > 0 and len(self.my_threads) == 0:
            if self.last_cleanup_time == 0:
                self.last_cleanup_time = current_time
            elif current_time - self.last_cleanup_time >= 15.0:
                if self.name == "Video":
                    print(f"{self.connector.prefix}All video clients gone. Stopping stream after grace period")
                    sys.stdout.flush()
                    self.connector.schedule_stop_livestream()
                self.last_cleanup_time = 0
        elif len(self.my_threads) > 0:
            self.last_cleanup_time = 0

    def run(self):
        print(f"{self.connector.prefix}Accepting connection for {self.name} on port {self.connector.ports[self.name.lower()]}")
        sys.stdout.flush()
        while not self.run_event.is_set():
            self.update_threads()
            try:
                readable, _, _ = select.select([self.socket], [], [], 1.0)
                if not readable:
                    continue
                client_sock, client_addr = self.socket.accept()
                print(f"{self.connector.prefix}New connection added: {client_addr} for {self.name}")
                sys.stdout.flush()

                if self.name == "BackChannel":
                    self.connector.schedule_stop_talkback()
                    client_sock.setblocking(True)
                    try:
                        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_SIZE)
                        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    except OSError:
                        pass
                    thread = ClientRecvThread(client_sock, self.run_event, self.name, self.connector)
                    thread.start()
                    self.my_threads.append(thread)
                else:
                    client_sock.setblocking(False)
                    try:
                        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUFFER_SIZE)
                        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    except OSError:
                        pass
                    thread = ClientSendThread(client_sock, self.run_event, self.name, self.connector)
                    self.queues.append(thread.queue)
                    if self.name == "Video":
                        thread.skip_non_idr = True
                        self.skip_non_idr = True
                        self.connector.prime_video_queue(thread.queue)
                    self.connector.schedule_start_livestream()
                    self.my_threads.append(thread)
                    thread.start()
            except (socket.timeout, OSError):
                continue


class ClientSendThread(threading.Thread):
    def __init__(self, client_sock, run_event, name, connector):
        super().__init__(daemon=True)
        self.client_sock = client_sock
        self.queue = Queue(30)
        self.run_event = run_event
        self.name = name
        self.connector = connector
        self._last_send_error_log = 0.0
        self.skip_non_idr = False

    def run(self):
        print(f"{self.connector.prefix}Thread running: {self.name}")
        sys.stdout.flush()
        try:
            pending_item = None
            while not self.run_event.is_set():
                if pending_item is None:
                    try:
                        pending_item = self.queue.get(timeout=1.0)
                    except Empty:
                        continue
                ready_to_read, ready_to_write, in_error = select.select([], [self.client_sock], [self.client_sock], 1.0)
                if in_error:
                    break
                if ready_to_write:
                    try:
                        payload = pending_item["data"] if isinstance(pending_item, dict) and "data" in pending_item else pending_item
                        self.client_sock.sendall(bytearray(payload))
                        pending_item = None
                    except (BrokenPipeError, ConnectionResetError, OSError) as e:
                        now = time.time()
                        if now - self._last_send_error_log >= 5.0:
                            print(f"{self.connector.prefix}Send error on {self.name}: {e}")
                            sys.stdout.flush()
                            self._last_send_error_log = now
                        break
        finally:
            self._cleanup()

    def _cleanup(self):
        try:
            self.client_sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.client_sock.close()
        except OSError:
            pass
        print(f"{self.connector.prefix}Thread stopping: {self.name}")
        sys.stdout.flush()


class ClientRecvThread(threading.Thread):
    def __init__(self, client_sock, run_event, name, connector):
        super().__init__(daemon=True)
        self.client_sock = client_sock
        self.run_event = run_event
        self.name = name
        self.connector = connector

    def run(self):
        print(f"{self.connector.prefix}[BACKCHANNEL] Thread started, attempting to start talkback")
        sys.stdout.flush()
        self.connector.schedule_start_talkback()
        total_bytes_received = 0
        packets_sent = 0
        try:
            curr_packet = bytearray()
            no_data = 0
            last_send_time = time.time()
            while not self.run_event.is_set():
                try:
                    ready_to_read, _, in_error = select.select([self.client_sock], [], [self.client_sock], 1)
                    if in_error:
                        break
                    if ready_to_read:
                        data = self.client_sock.recv(RECV_CHUNK_SIZE)
                        if data:
                            curr_packet += bytearray(data)
                            total_bytes_received += len(data)
                            no_data = 0
                            current_time = time.time()
                            if len(curr_packet) >= 1600 or (current_time - last_send_time >= 0.1 and curr_packet):
                                self.connector.schedule_send_talkback_data(list(bytes(curr_packet)))
                                curr_packet = bytearray()
                                last_send_time = current_time
                                packets_sent += 1
                        else:
                            break
                    else:
                        no_data += 1
                        if curr_packet and time.time() - last_send_time >= 0.2:
                            self.connector.schedule_send_talkback_data(list(bytes(curr_packet)))
                            curr_packet = bytearray()
                            last_send_time = time.time()
                            packets_sent += 1
                        if no_data >= 30:
                            print(f"{self.connector.prefix}[BACKCHANNEL] 30 seconds idle")
                            sys.stdout.flush()
                            no_data = 0
                except BlockingIOError:
                    pass
        except (socket.error, select.error) as e:
            print(f"{self.connector.prefix}[BACKCHANNEL] Connection error: {e}")
            sys.stdout.flush()
        finally:
            print(f"{self.connector.prefix}[BACKCHANNEL] Thread stopping (total: {total_bytes_received} bytes, {packets_sent} packets)")
            sys.stdout.flush()
            self._cleanup()
            self.connector.schedule_stop_talkback()

    def _cleanup(self):
        try:
            self.client_sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.client_sock.close()
        except OSError:
            pass


class CameraConnector:
    def __init__(self, manager, serialno: str, index: int):
        self.manager = manager
        self.ws = None
        self.run_event = manager.run_event
        self.serialno = serialno
        self.index = index
        first_port = BASE_PORT + (index * PORTS_PER_CAMERA)
        self.ports = {"video": first_port, "audio": first_port + 1, "backchannel": first_port + 2}
        self.prefix = f"[CAM {index + 1} {serialno}] "
        self.video_sock = create_server_socket(self.ports["video"])
        self.audio_sock = create_server_socket(self.ports["audio"])
        self.backchannel_sock = create_server_socket(self.ports["backchannel"], keepalive=True)
        self.loop = None
        self.livestream_active = False
        self.talkback_active = False
        self.livestream_lock: Optional[asyncio.Lock] = None
        self.talkback_lock: Optional[asyncio.Lock] = None
        self.last_livestream_start = 0
        self.last_talkback_start = 0
        self.video_codec: Optional[str] = None
        self.audio_codec: Optional[str] = None
        self._video_buffer_shape: Optional[str] = None
        self._video_parse_buffer = bytearray()
        self._last_vps: Optional[bytes] = None
        self._last_sps: Optional[bytes] = None
        self._last_pps: Optional[bytes] = None
        self._last_idr: Optional[bytes] = None
        self.event_stats_video_count = 0
        self.event_stats_audio_count = 0
        self.event_stats_start_time = time.time()
        self.video_thread = ClientAcceptThread(self.video_sock, self.run_event, "Video", self)
        self.audio_thread = ClientAcceptThread(self.audio_sock, self.run_event, "Audio", self)
        self.backchannel_thread = ClientAcceptThread(self.backchannel_sock, self.run_event, "BackChannel", self)

    def start_threads(self):
        self.audio_thread.start()
        self.video_thread.start()
        self.backchannel_thread.start()
        print(f"{self.prefix}Ports: video={self.ports['video']} audio={self.ports['audio']} backchannel={self.ports['backchannel']}")
        sys.stdout.flush()

    def set_ws(self, ws):
        self.ws = ws

    def set_loop(self, loop):
        self.loop = loop
        self._ensure_async_primitives()

    def _ensure_async_primitives(self):
        if self.livestream_lock is None:
            self.livestream_lock = asyncio.Lock()
        if self.talkback_lock is None:
            self.talkback_lock = asyncio.Lock()

    def stop(self):
        for sock in (self.video_sock, self.audio_sock, self.backchannel_sock):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def has_clients(self) -> bool:
        return bool(self.video_thread.queues or self.audio_thread.queues)

    def schedule_start_livestream(self):
        if self.loop and not self.loop.is_closed() and not self.run_event.is_set():
            asyncio.run_coroutine_threadsafe(self._start_livestream(), self.loop)

    def schedule_stop_livestream(self):
        if self.loop and not self.loop.is_closed() and not self.run_event.is_set():
            asyncio.run_coroutine_threadsafe(self._stop_livestream(), self.loop)

    def schedule_start_talkback(self):
        if self.loop and not self.loop.is_closed() and not self.run_event.is_set():
            asyncio.run_coroutine_threadsafe(self._start_talkback(), self.loop)

    def schedule_stop_talkback(self):
        if self.loop and not self.loop.is_closed() and not self.run_event.is_set():
            asyncio.run_coroutine_threadsafe(self._stop_talkback(), self.loop)

    def schedule_send_talkback_data(self, data):
        if self.loop and not self.loop.is_closed() and not self.run_event.is_set():
            asyncio.run_coroutine_threadsafe(self._send_talkback_data(data), self.loop)

    def prime_video_queue(self, queue: Queue) -> None:
        codec = None
        if self.video_codec:
            codec = self.video_codec.lower()
        elif self._last_vps or self._last_sps or self._last_pps:
            codec = "hevc" if self._last_vps else "h264"
        if codec is None:
            return
        parts: list[bytes] = []
        if "h265" in codec or "hevc" in codec:
            for p in (self._last_vps, self._last_sps, self._last_pps):
                if p: parts.append(p)
        else:
            for p in (self._last_sps, self._last_pps):
                if p: parts.append(p)
        if self._last_idr:
            parts.append(self._last_idr)
        if not parts:
            return
        preamble = b"".join(parts)
        try:
            queue.put_nowait(list(preamble) if self._video_buffer_shape == "list" else {"data": list(preamble)})
        except Exception:
            pass

    def _extract_buffer_bytes(self, event_value: Any) -> Tuple[Optional[bytes], Optional[str]]:
        if isinstance(event_value, dict) and "data" in event_value and isinstance(event_value["data"], list):
            return bytes(event_value["data"]), "dict"
        if isinstance(event_value, list):
            return bytes(event_value), "list"
        return None, None

    @staticmethod
    def _find_start_codes(data: bytes) -> list[Tuple[int, int]]:
        out: list[Tuple[int, int]] = []
        i = 0
        n = len(data)
        while i + 3 < n:
            if data[i] == 0 and data[i + 1] == 0:
                if data[i + 2] == 1:
                    out.append((i, 3)); i += 3; continue
                if i + 3 < n and data[i + 2] == 0 and data[i + 3] == 1:
                    out.append((i, 4)); i += 4; continue
            i += 1
        return out

    def _is_idr_frame(self, chunk: bytes) -> bool:
        if not self.video_codec:
            return False
        codec_l = self.video_codec.lower()
        starts = self._find_start_codes(chunk)
        for (idx, sc_len), next_start in zip(starts, starts[1:] + [(len(chunk), 0)]):
            if idx + sc_len + 1 > len(chunk):
                continue
            header = chunk[idx + sc_len]
            if "h265" in codec_l or "hevc" in codec_l:
                if ((header >> 1) & 0x3F) in (19, 20):
                    return True
            else:
                if (header & 0x1F) == 5:
                    return True
        return False

    def _update_video_codec_cache(self, chunk: bytes, codec: Optional[str]) -> None:
        if codec and not self.video_codec:
            self.video_codec = codec
            print(f"{self.prefix}Video codec detected: {codec}")
            sys.stdout.flush()
        self._video_parse_buffer.extend(chunk)
        if len(self._video_parse_buffer) > 512 * 1024:
            self._video_parse_buffer = self._video_parse_buffer[-128 * 1024:]
        buf = bytes(self._video_parse_buffer)
        starts = self._find_start_codes(buf)
        if len(starts) < 2:
            return
        for (idx, sc_len), (next_idx, _next_len) in zip(starts, starts[1:]):
            nal = buf[idx:next_idx]
            if len(nal) <= sc_len:
                continue
            header = nal[sc_len]
            codec_l = (self.video_codec or "").lower()
            if "h265" in codec_l or "hevc" in codec_l:
                nal_type = (header >> 1) & 0x3F
                if nal_type == 32: self._last_vps = nal
                elif nal_type == 33: self._last_sps = nal
                elif nal_type == 34: self._last_pps = nal
                elif nal_type in (19, 20): self._last_idr = nal
            else:
                nal_type = header & 0x1F
                if nal_type == 7: self._last_sps = nal
                elif nal_type == 8: self._last_pps = nal
                elif nal_type == 5: self._last_idr = nal
        last_start_idx, _ = starts[-1]
        self._video_parse_buffer = bytearray(buf[last_start_idx:])

    async def _start_livestream(self):
        if not self.ws or not self.serialno:
            print(f"{self.prefix}[LIVESTREAM] Cannot start: ws={self.ws is not None}, serial={self.serialno}")
            sys.stdout.flush()
            return
        self._ensure_async_primitives()
        async with self.livestream_lock:
            current_time = time.time()
            if self.livestream_active or (current_time - self.last_livestream_start) < 2.0:
                return
            self.livestream_active = True
            self.last_livestream_start = current_time
            msg = START_P2P_LIVESTREAM_MESSAGE.copy()
            msg["serialNumber"] = self.serialno
            msg["messageId"] = f"start_livestream_{self.serialno}"
            try:
                print(f"{self.prefix}[LIVESTREAM] Sending START command")
                sys.stdout.flush()
                await self.ws.send_message(json.dumps(msg))
            except Exception as e:
                print(f"{self.prefix}[LIVESTREAM] Error starting livestream: {e}")
                sys.stdout.flush()
                self.livestream_active = False

    async def _stop_livestream(self):
        if not self.ws or not self.serialno:
            return
        self._ensure_async_primitives()
        async with self.livestream_lock:
            if not self.livestream_active:
                return
            self.livestream_active = False
            msg = STOP_P2P_LIVESTREAM_MESSAGE.copy()
            msg["serialNumber"] = self.serialno
            msg["messageId"] = f"stop_livestream_{self.serialno}"
            try:
                print(f"{self.prefix}[LIVESTREAM] Sending STOP command")
                sys.stdout.flush()
                await self.ws.send_message(json.dumps(msg))
            except Exception as e:
                print(f"{self.prefix}[LIVESTREAM] Error stopping livestream: {e}")
                sys.stdout.flush()

    async def _start_talkback(self):
        if not self.ws or not self.serialno:
            return
        self._ensure_async_primitives()
        async with self.talkback_lock:
            current_time = time.time()
            if self.talkback_active or (current_time - self.last_talkback_start) < 1.0:
                return
            self.talkback_active = True
            self.last_talkback_start = current_time
            msg = START_TALKBACK.copy()
            msg["serialNumber"] = self.serialno
            msg["messageId"] = f"start_talkback_{self.serialno}"
            try:
                await self.ws.send_message(json.dumps(msg))
            except Exception as e:
                print(f"{self.prefix}[BACKCHANNEL] Error starting talkback: {e}")
                sys.stdout.flush()
                self.talkback_active = False

    async def _stop_talkback(self):
        if not self.ws or not self.serialno:
            return
        self._ensure_async_primitives()
        async with self.talkback_lock:
            if not self.talkback_active:
                return
            self.talkback_active = False
            msg = STOP_TALKBACK.copy()
            msg["serialNumber"] = self.serialno
            msg["messageId"] = f"stop_talkback_{self.serialno}"
            try:
                await self.ws.send_message(json.dumps(msg))
            except Exception as e:
                print(f"{self.prefix}[BACKCHANNEL] Error stopping talkback: {e}")
                sys.stdout.flush()

    async def _send_talkback_data(self, data):
        if not self.ws or not self.serialno or not self.talkback_active:
            return
        msg = SEND_TALKBACK_AUDIO_DATA.copy()
        msg["serialNumber"] = self.serialno
        msg["messageId"] = f"talkback_audio_data_{self.serialno}"
        msg["buffer"] = data
        try:
            await self.ws.send_message(json.dumps(msg))
        except Exception as e:
            print(f"{self.prefix}[BACKCHANNEL] Error sending talkback data ({len(data)} bytes): {e}")
            sys.stdout.flush()

    async def handle_stream_event(self, message: dict, event_type: str):
        if event_type == "livestream audio data":
            self.event_stats_audio_count += 1
            if not self.audio_thread.ready_to_accept.is_set():
                self.audio_thread.ready_to_accept.set()
                print(f"{self.prefix}Audio data flowing")
            try:
                meta = message.get("metadata") or {}
                codec = meta.get("audioCodec") if isinstance(meta, dict) else None
                if codec and not self.audio_codec:
                    self.audio_codec = codec
                    print(f"{self.prefix}Audio codec detected: {codec}")
                    sys.stdout.flush()
            except Exception:
                pass
            event_value = message[EVENT_CONFIGURATION[event_type]["value"]]
            for queue in self.audio_thread.queues:
                while queue.qsize() > 5:
                    try: queue.get(False)
                    except Exception: break
                try: queue.put(event_value, block=False)
                except Exception: pass

        elif event_type == "livestream video data":
            self.event_stats_video_count += 1
            if not self.video_thread.ready_to_accept.is_set():
                self.video_thread.ready_to_accept.set()
                print(f"{self.prefix}Video data flowing")
            is_idr = False
            try:
                meta = message.get("metadata") or {}
                codec = meta.get("videoCodec") if isinstance(meta, dict) else None
                event_value = message[EVENT_CONFIGURATION[event_type]["value"]]
                chunk, shape = self._extract_buffer_bytes(event_value)
                if shape and not self._video_buffer_shape:
                    self._video_buffer_shape = shape
                if chunk:
                    self._update_video_codec_cache(chunk, codec)
                    is_idr = self._is_idr_frame(chunk)
                    if is_idr:
                        for thread in self.video_thread.my_threads:
                            thread.skip_non_idr = False
                        self.video_thread.skip_non_idr = False
            except Exception as e:
                print(f"{self.prefix}[VIDEO] Error in video frame processing: {e}")
                sys.stdout.flush()
            event_value = message[EVENT_CONFIGURATION[event_type]["value"]]
            for thread in self.video_thread.my_threads:
                if thread.skip_non_idr and not is_idr:
                    continue
                while thread.queue.qsize() > 3:
                    try: thread.queue.get(False)
                    except Exception: break
                try: thread.queue.put(event_value, block=False)
                except Exception: pass

    async def handle_livestream_error(self):
        self._ensure_async_primitives()
        async with self.livestream_lock:
            self.livestream_active = False
            self.video_thread.ready_to_accept.clear()
            self.audio_thread.ready_to_accept.clear()
        await asyncio.sleep(1.5)
        if self.ws and self.video_thread.queues:
            await self._start_livestream()


class MultiConnector:
    def __init__(self, run_event):
        self.run_event = run_event
        self.ws = None
        self.loop = None
        self.ws_closed_event: Optional[asyncio.Event] = None
        self.cameras: dict[str, CameraConnector] = {}
        self.last_event_time = time.time()
        self.ws_event_timeout_seconds = 60
        self.ws_monitor_task: Optional[asyncio.Task] = None
        self.stats_reporter_task: Optional[asyncio.Task] = None

    def set_loop(self, loop):
        self.loop = loop
        if self.ws_closed_event is None:
            self.ws_closed_event = asyncio.Event()
        for camera in self.cameras.values():
            camera.set_loop(loop)

    def setWs(self, ws):
        self.ws = ws
        for camera in self.cameras.values():
            camera.set_ws(ws)

    def stop(self):
        for camera in self.cameras.values():
            camera.stop()

    @staticmethod
    def _extract_serial_from_device(dev: Any) -> Optional[str]:
        if isinstance(dev, str):
            return dev
        if isinstance(dev, dict):
            for key in ("serialNumber", "deviceSerial", "deviceSerialNumber", "serial"):
                if dev.get(key):
                    return str(dev[key])
        return None

    @staticmethod
    def _extract_event_serial(message: dict) -> Optional[str]:
        for key in ("serialNumber", "deviceSerial", "deviceSerialNumber", "serial"):
            if message.get(key):
                return str(message[key])
        meta = message.get("metadata") or {}
        if isinstance(meta, dict):
            for key in ("serialNumber", "deviceSerial", "deviceSerialNumber", "serial"):
                if meta.get(key):
                    return str(meta[key])
        return None

    def _ensure_cameras(self, devices: list[Any]) -> None:
        serials = []
        for dev in devices:
            serial = self._extract_serial_from_device(dev)
            if serial and serial not in serials:
                serials.append(serial)
        serials = serials[:MAX_CAMERAS]

        if not serials:
            print("[MULTI] No devices returned by start_listening yet")
            sys.stdout.flush()
            return

        print(f"[MULTI] Devices discovered: {', '.join(serials)}")
        if len(serials) > MAX_CAMERAS:
            print(f"[MULTI] Limiting to {MAX_CAMERAS} cameras")
        sys.stdout.flush()

        for serial in serials:
            if serial in self.cameras:
                continue
            index = len(self.cameras)
            if index >= MAX_CAMERAS:
                break
            camera = CameraConnector(self, serial, index)
            camera.set_ws(self.ws)
            if self.loop:
                camera.set_loop(self.loop)
            self.cameras[serial] = camera
            camera.start_threads()

        self._print_port_summary()

    def _print_port_summary(self):
        print("[MULTI] Camera port map:")
        for camera in self.cameras.values():
            print(f"[MULTI]   {camera.serialno}: video=tcp://HOST:{camera.ports['video']} audio=tcp://HOST:{camera.ports['audio']} backchannel=tcp://HOST:{camera.ports['backchannel']}")
        sys.stdout.flush()

    async def on_open(self):
        print("[WS_LIFECYCLE] WebSocket connection opened")
        sys.stdout.flush()
        if self.ws_closed_event is not None:
            self.ws_closed_event.clear()

    async def on_close(self):
        print("[WS_LIFECYCLE] WebSocket connection closed")
        sys.stdout.flush()
        self.ws = None
        for camera in self.cameras.values():
            camera.set_ws(None)
            camera.livestream_active = False
            camera.talkback_active = False
            camera.video_thread.ready_to_accept.clear()
            camera.audio_thread.ready_to_accept.clear()
        if self.ws_closed_event is not None:
            self.ws_closed_event.set()

    async def on_error(self, message):
        print(f"on_error - executed - {message}")
        sys.stdout.flush()
        self.last_event_time = time.time()

    async def _report_event_statistics(self):
        print("[WS_STATS] Statistics reporter started")
        sys.stdout.flush()
        while not self.run_event.is_set():
            try:
                await asyncio.sleep(30)
                for camera in self.cameras.values():
                    if camera.has_clients():
                        elapsed = time.time() - camera.event_stats_start_time
                        if elapsed <= 0:
                            continue
                        print(f"{camera.prefix}[WS_STATS] {camera.event_stats_video_count} video frames ({camera.event_stats_video_count/elapsed:.1f}/s), {camera.event_stats_audio_count} audio frames ({camera.event_stats_audio_count/elapsed:.1f}/s) in {elapsed:.0f}s")
                        camera.event_stats_video_count = 0
                        camera.event_stats_audio_count = 0
                        camera.event_stats_start_time = time.time()
                        sys.stdout.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[WS_STATS] Reporter error: {e}")
                sys.stdout.flush()

    async def _monitor_websocket_health(self):
        print("[WS_MONITOR] Event monitor started")
        sys.stdout.flush()
        while not self.run_event.is_set():
            try:
                await asyncio.sleep(5)
                for camera in self.cameras.values():
                    camera.video_thread.update_threads()
                    camera.audio_thread.update_threads()
                has_clients = any(camera.has_clients() for camera in self.cameras.values())
                ws_connected = self.ws and hasattr(self.ws, "ws") and self.ws.ws and not self.ws.ws.closed
                print(f"[WS_MONITOR] status={'connected' if ws_connected else 'disconnected'}, cameras={len(self.cameras)}, active_clients={has_clients}, last_event_age={(time.time() - self.last_event_time):.1f}s")
                sys.stdout.flush()
                if has_clients and time.time() - self.last_event_time > self.ws_event_timeout_seconds:
                    print("[WS_MONITOR] ALERT: stale websocket; forcing reconnect")
                    sys.stdout.flush()
                    if ws_connected:
                        await self.ws.ws.close()
                    self.last_event_time = time.time()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[WS_MONITOR] Monitor error: {e}")
                sys.stdout.flush()

    async def on_message(self, message):
        self.last_event_time = time.time()
        payload = message.json()
        message_type = payload.get("type")

        if message_type == "result":
            message_id = payload.get("messageId")
            if message_id != "talkback_audio_data":
                print(f"[WS_RX] Result message: {message_id}")
                sys.stdout.flush()
            if message_id == START_LISTENING_MESSAGE["messageId"]:
                states = (payload.get("result") or {}).get("state") or {}
                devices = states.get("devices") or []
                self._ensure_cameras(devices)
            return

        if message_type != "event":
            return

        event_message = payload.get("event") or {}
        event_type = event_message.get("event")
        if not event_type:
            return

        serial = self._extract_event_serial(event_message)
        camera = self.cameras.get(serial) if serial else None

        # Some eufy-security-ws builds do not include serial in every stream packet. If only one
        # camera has an active client, route the packet to that camera as a safe fallback.
        if camera is None and event_type in ("livestream video data", "livestream audio data", "livestream error"):
            active = [cam for cam in self.cameras.values() if cam.has_clients() or cam.livestream_active]
            if len(active) == 1:
                camera = active[0]

        if event_type in ("livestream video data", "livestream audio data"):
            if camera:
                await camera.handle_stream_event(event_message, event_type)
            else:
                print(f"[MULTI] Dropping {event_type}: cannot identify camera serial in event")
                sys.stdout.flush()
            return

        if event_type == "livestream error":
            if camera:
                print(f"{camera.prefix}Livestream error - attempting restart")
                sys.stdout.flush()
                await camera.handle_livestream_error()
            else:
                print("[MULTI] Livestream error without camera serial")
                sys.stdout.flush()
            return

        print(f"[WS_RX] Event: {event_type}")
        sys.stdout.flush()


c = MultiConnector(run_event)

async def init_websocket() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Missing argument: expected eufy-security-ws port as argv[1]")
    c.set_loop(asyncio.get_running_loop())
    backoff_s = 1.0
    async with aiohttp.ClientSession() as session:
        while not run_event.is_set():
            print(f"[WS_INIT] Attempting WebSocket connection (backoff: {backoff_s:.1f}s)...")
            sys.stdout.flush()
            ws = EufySecurityWebSocket(
                "402f1039-eufy-security-ws",
                int(sys.argv[1]),
                session,
                c.on_open,
                c.on_message,
                c.on_close,
                c.on_error,
            )
            c.setWs(ws)
            if c.ws_closed_event is not None:
                c.ws_closed_event.clear()
            try:
                await ws.connect()
                print("[WS_INIT] WebSocket connected successfully")
                sys.stdout.flush()
                c.last_event_time = time.time()
                if c.ws_monitor_task is None or c.ws_monitor_task.done():
                    c.ws_monitor_task = asyncio.create_task(c._monitor_websocket_health())
                if c.stats_reporter_task is None or c.stats_reporter_task.done():
                    c.stats_reporter_task = asyncio.create_task(c._report_event_statistics())
                await ws.send_message(json.dumps(SET_API_SCHEMA))
                await ws.send_message(json.dumps(START_LISTENING_MESSAGE))
                await ws.send_message(json.dumps(DRIVER_CONNECT_MESSAGE))
                print("[WS_INIT] Initialization messages sent")
                sys.stdout.flush()
                backoff_s = 1.0
                if c.ws_closed_event is not None:
                    await c.ws_closed_event.wait()
                else:
                    while not run_event.is_set():
                        await asyncio.sleep(1)
            except Exception as ex:
                print(f"[WS_INIT] WebSocket error: {ex}")
                sys.stdout.flush()
            finally:
                for task_attr in ("ws_monitor_task", "stats_reporter_task"):
                    task = getattr(c, task_attr)
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                    setattr(c, task_attr, None)
                c.setWs(None)
            if run_event.is_set():
                break
            print(f"[WS_INIT] Reconnecting in {backoff_s:.1f}s...")
            sys.stdout.flush()
            await asyncio.sleep(backoff_s)
            backoff_s = min(backoff_s * 2.0, 30.0)
    print("Cleaning up...")
    sys.stdout.flush()
    c.stop()

if __name__ == "__main__":
    try:
        asyncio.run(init_websocket())
    except KeyboardInterrupt:
        print("Interrupted by user")
