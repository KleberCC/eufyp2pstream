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
from http.server import BaseHTTPRequestHandler, HTTPServer
import os
from queue import Empty, Queue
from typing import Any, Optional, Tuple

RECV_CHUNK_SIZE = 8192  
SOCKET_BUFFER_SIZE = 262144  

EVENT_CONFIGURATION: dict = {
    "livestream video data": {
        "name": "video_data",
        "value": "buffer",
        "type": "event",
    },
    "livestream audio data": {
        "name": "audio_data",
        "value": "buffer",
        "type": "event",
    },
}

START_P2P_LIVESTREAM_MESSAGE = {
    "messageId": "start_livestream",
    "command": "device.start_livestream",
    "serialNumber": None,
}

STOP_P2P_LIVESTREAM_MESSAGE = {
    "messageId": "stop_livestream",
    "command": "device.stop_livestream",
    "serialNumber": None,
}

START_TALKBACK = {
    "messageId": "start_talkback",
    "command": "device.start_talkback",
    "serialNumber": None,
}

SEND_TALKBACK_AUDIO_DATA = {
    "messageId": "talkback_audio_data",
    "command": "device.talkback_audio_data",
    "serialNumber": None,
    "buffer": None
}

STOP_TALKBACK = {
    "messageId": "stop_talkback",
    "command": "device.stop_talkback",
    "serialNumber": None,
}

SET_API_SCHEMA = {
    "messageId": "set_api_schema",
    "command": "set_api_schema",
    "schemaVersion": 13,
}

P2P_LIVESTREAMING_STATUS = "p2pLiveStreamingStatus"
START_LISTENING_MESSAGE = {"messageId": "start_listening", "command": "start_listening"}
TALKBACK_RESULT_MESSAGE = {"messageId": "talkback_audio_data", "errorCode": "device_talkback_not_running"}
DRIVER_CONNECT_MESSAGE = {"messageId": "driver_connect", "command": "driver.connect"}

run_event = threading.Event()

def exit_handler(signum, frame):
    print(f'Signal handler called with signal {signum}')
    run_event.set()

signal.signal(signal.SIGINT, exit_handler)
signal.signal(signal.SIGTERM, exit_handler)

class ClientAcceptThread(threading.Thread):
    def __init__(self, socket, run_event, name, connector, serialno):
        threading.Thread.__init__(self)
        self.socket = socket
        self.queues = []
        self.run_event = run_event
        self.name = name
        self.connector = connector
        self.serialno = serialno
        self.my_threads = []
        self.last_cleanup_time = 0
        self.ready_to_accept = threading.Event()  
        self.skip_non_idr = self.name == "Video"
        print(f"[{self.serialno} - {self.name}] ClientAcceptThread initialized, skip_non_idr={self.skip_non_idr}")
        sys.stdout.flush()

    def update_threads(self):
        my_threads_before = len(self.my_threads)
        for thread in self.my_threads:
            if not thread.is_alive():
                self.queues.remove(thread.queue)
        self.my_threads = [t for t in self.my_threads if t.is_alive()]
        
        current_time = time.time()
        if my_threads_before > 0 and len(self.my_threads) == 0:
            if self.last_cleanup_time == 0:
                self.last_cleanup_time = current_time
            elif current_time - self.last_cleanup_time >= 15.0:
                if self.name == "BackChannel":
                    print(f"[{self.serialno}] All clients died (BackChannel)")
                    sys.stdout.flush()
                else:
                    if self.name == "Video":
                        print(f"[{self.serialno}] All video clients gone. Stopping Stream (grace elapsed)")
                        sys.stdout.flush()
                        self.connector.schedule_stop_livestream(self.serialno)
                self.last_cleanup_time = 0
        elif len(self.my_threads) > 0:
            self.last_cleanup_time = 0

    def run(self):
        print(f"[{self.serialno}] Accepting connection for {self.name}")
        while not self.run_event.is_set():
            self.update_threads()
            sys.stdout.flush()
            try:
                readable, _, _ = select.select([self.socket], [], [], 1.0)
                if not readable:
                    continue

                client_sock, client_addr = self.socket.accept()
                print(f"[{self.serialno}] New connection added: {client_addr} for {self.name}")
                sys.stdout.flush()

                if self.name == "BackChannel":
                    self.connector.schedule_stop_talkback(self.serialno)
                    client_sock.setblocking(True)
                    try:
                        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_SIZE)
                        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    except OSError:
                        pass
                    print(f"[{self.serialno} - BACKCHANNEL] Starting BackChannel thread")
                    sys.stdout.flush()
                    thread = ClientRecvThread(client_sock, run_event, self.name, self.connector, self.serialno)
                    thread.start()
                else:
                    client_sock.setblocking(False)
                    try:
                        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUFFER_SIZE)
                        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    except OSError:
                        pass
                    thread = ClientSendThread(client_sock, run_event, self.name, self.connector, self.serialno)
                    self.queues.append(thread.queue)
                    if self.name == "Video":
                        thread.skip_non_idr = True
                        self.skip_non_idr = True
                        print(f"[{self.serialno} - Video] New client thread created with skip_non_idr=True")
                        sys.stdout.flush()
                        self.connector.prime_video_queue(self.serialno, thread.queue)
                    
                    self.connector.schedule_start_livestream(self.serialno)
                    self.my_threads.append(thread)
                    thread.start()
            except (socket.timeout, OSError):
                continue

class ClientSendThread(threading.Thread):
    def __init__(self, client_sock, run_event, name, connector, serialno):
        threading.Thread.__init__(self)
        self.client_sock = client_sock
        self.queue = Queue(30)  
        self.run_event = run_event
        self.name = name
        self.connector = connector
        self.serialno = serialno
        self._last_send_error_log = 0.0
        self.skip_non_idr = False 
        sys.stdout.flush()

    def run(self):
        try:
            pending_item = None
            while not self.run_event.is_set():
                if pending_item is None:
                    try:
                        pending_item = self.queue.get(timeout=1.0)
                    except Empty:
                        continue

                if self.run_event.is_set():
                    break

                ready_to_read, ready_to_write, in_error = \
                    select.select([], [self.client_sock], [self.client_sock], 1.0)
                if len(in_error):
                    break
                if len(ready_to_write):
                    try:
                        if isinstance(pending_item, dict) and "data" in pending_item:
                            payload = pending_item["data"]
                        else:
                            payload = pending_item
                        self.client_sock.sendall(bytearray(payload))
                        pending_item = None
                    except (BrokenPipeError, ConnectionResetError, OSError) as e:
                        now = time.time()
                        if now - self._last_send_error_log >= 5.0:
                            print(f"[{self.serialno}] Send error on {self.name}: {e}")
                            sys.stdout.flush()
                            self._last_send_error_log = now
                        break
                else:
                    time.sleep(0.05)
        except socket.error:
            pass
        except socket.timeout:
            pass
        self._cleanup()

    def _cleanup(self):
        try:
            self.client_sock.shutdown(socket.SHUT_RDWR)
            self.client_sock.close()
        except OSError:
            pass
        sys.stdout.flush()

class ClientRecvThread(threading.Thread):
    def __init__(self, client_sock, run_event, name, connector, serialno):
        threading.Thread.__init__(self)
        self.client_sock = client_sock
        self.run_event = run_event
        self.name = name
        self.connector = connector
        self.serialno = serialno

    def run(self):
        self.connector.schedule_start_talkback(self.serialno)
        try:
            curr_packet = bytearray() 
            no_data = 0
            last_send_time = time.time()
            
            while not self.run_event.is_set():
                try:
                    ready_to_read, ready_to_write, in_error = \
                        select.select([self.client_sock,], [], [self.client_sock], 1)
                    if len(in_error):
                        break
                    if len(ready_to_read):
                        data = self.client_sock.recv(RECV_CHUNK_SIZE)
                        if len(data) > 0:
                            curr_packet += bytearray(data)
                            no_data = 0
                            current_time = time.time()
                            if len(curr_packet) >= 1600 or (current_time - last_send_time >= 0.1 and len(curr_packet) > 0):
                                self.connector.schedule_send_talkback_data(self.serialno, list(bytes(curr_packet)))
                                curr_packet = bytearray()
                                last_send_time = current_time
                        else:
                            break
                    else:
                        no_data += 1
                        if len(curr_packet) > 0 and time.time() - last_send_time >= 0.2:
                            self.connector.schedule_send_talkback_data(self.serialno, list(bytes(curr_packet)))
                            curr_packet = bytearray()
                            last_send_time = time.time()
                    
                    if no_data >= 30:  
                        no_data = 0
                except BlockingIOError:
                    pass
        except (socket.error, select.error):
            pass
        except socket.timeout:
            pass
        
        self._cleanup()
        self.connector.schedule_stop_talkback(self.serialno)

    def _cleanup(self):
        try:
            self.client_sock.shutdown(socket.SHUT_RDWR)
            self.client_sock.close()
        except OSError:
            pass

class CameraSession:
    def __init__(self, serialno, base_port, connector, run_event):
        self.serialno = serialno
        self.video_port = base_port
        self.audio_port = base_port + 1
        self.backchannel_port = base_port + 2
        
        self.livestream_active = False
        self.talkback_active = False
        self.livestream_lock = None 
        self.talkback_lock = None
        self.last_livestream_start = 0
        self.last_talkback_start = 0

        self.video_codec = None
        self.audio_codec = None
        self._video_buffer_shape = None
        self._video_parse_buffer = bytearray()
        self._last_vps = None
        self._last_sps = None
        self._last_pps = None
        self._last_idr = None

        self.video_sock = self._create_socket(self.video_port)
        self.audio_sock = self._create_socket(self.audio_port)
        self.backchannel_sock = self._create_socket(self.backchannel_port)

        self.video_thread = ClientAcceptThread(self.video_sock, run_event, "Video", connector, serialno)
        self.audio_thread = ClientAcceptThread(self.audio_sock, run_event, "Audio", connector, serialno)
        self.backchannel_thread = ClientAcceptThread(self.backchannel_sock, run_event, "BackChannel", connector, serialno)

    def _create_socket(self, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_SIZE)
        s.bind(("0.0.0.0", port))
        s.settimeout(0.5)
        s.listen(32)
        return s

    def close_all(self):
        for sock in [self.video_sock, self.audio_sock, self.backchannel_sock]:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass


class Connector:
    def __init__(self, run_event):
        self.cameras = {} 
        self.base_port_start = 63336
        
        self.ws = None
        self.run_event = run_event
        self.loop = None
        self.ws_closed_event: Optional[asyncio.Event] = None
        
        self.command_queue = Queue()
        self.last_event_time = time.time()
        self.ws_event_timeout_seconds = 60  
        self.ws_monitor_task: Optional[asyncio.Task] = None
        
        self.event_stats_video_count = 0
        self.event_stats_audio_count = 0
        self.event_stats_start_time = time.time()
        self.stats_reporter_task: Optional[asyncio.Task] = None

    def stop(self):
        for session in self.cameras.values():
            session.close_all()

    def setWs(self, ws: EufySecurityWebSocket):
        self.ws = ws

    def set_loop(self, loop):
        self.loop = loop
        self._ensure_async_primitives()

    def _ensure_async_primitives(self, serialno=None) -> None:
        if self.ws_closed_event is None:
            self.ws_closed_event = asyncio.Event()
        
        if serialno:
            session = self.cameras.get(serialno)
            if session:
                if session.livestream_lock is None:
                    session.livestream_lock = asyncio.Lock()
                if session.talkback_lock is None:
                    session.talkback_lock = asyncio.Lock()
        else:
            for session in self.cameras.values():
                if session.livestream_lock is None:
                    session.livestream_lock = asyncio.Lock()
                if session.talkback_lock is None:
                    session.talkback_lock = asyncio.Lock()

    def schedule_start_livestream(self, serialno):
        if self.loop and not self.loop.is_closed() and not self.run_event.is_set():
            asyncio.run_coroutine_threadsafe(self._start_livestream(serialno), self.loop)

    def schedule_stop_livestream(self, serialno):
        if self.loop and not self.loop.is_closed() and not self.run_event.is_set():
            asyncio.run_coroutine_threadsafe(self._stop_livestream(serialno), self.loop)

    def schedule_start_talkback(self, serialno):
        if self.loop and not self.loop.is_closed() and not self.run_event.is_set():
            asyncio.run_coroutine_threadsafe(self._start_talkback(serialno), self.loop)

    def schedule_stop_talkback(self, serialno):
        if self.loop and not self.loop.is_closed() and not self.run_event.is_set():
            asyncio.run_coroutine_threadsafe(self._stop_talkback(serialno), self.loop)

    def schedule_send_talkback_data(self, serialno, data):
        if self.loop and not self.loop.is_closed() and not self.run_event.is_set():
            asyncio.run_coroutine_threadsafe(self._send_talkback_data(serialno, data), self.loop)

    def prime_video_queue(self, serialno, queue: Queue) -> None:
        session = self.cameras.get(serialno)
        if not session: return

        codec = None
        if session.video_codec:
            codec = session.video_codec.lower()
        elif session._last_vps or session._last_sps or session._last_pps:
            codec = "hevc" if session._last_vps else "h264"

        if codec is None: return

        parts: list[bytes] = []
        if "h265" in codec or "hevc" in codec:
            if session._last_vps: parts.append(session._last_vps)
            if session._last_sps: parts.append(session._last_sps)
            if session._last_pps: parts.append(session._last_pps)
        else:
            if session._last_sps: parts.append(session._last_sps)
            if session._last_pps: parts.append(session._last_pps)

        if not parts: return
        if session._last_idr: parts.append(session._last_idr)

        preamble = b"".join(parts)
        try:
            if session._video_buffer_shape == "list":
                queue.put_nowait(list(preamble))
            else:
                queue.put_nowait({"data": list(preamble)})
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
                    out.append((i, 3))
                    i += 3
                    continue
                if i + 3 < n and data[i + 2] == 0 and data[i + 3] == 1:
                    out.append((i, 4))
                    i += 4
                    continue
            i += 1
        return out

    def _is_idr_frame(self, session, chunk: bytes) -> bool:
        if not session.video_codec: return False
        codec_l = session.video_codec.lower()
        starts = self._find_start_codes(chunk)
        if len(starts) < 1: return False
        
        for (idx, sc_len), next_start in zip(starts, starts[1:] + [(len(chunk), 0)]):
            if idx + sc_len + 1 > len(chunk): continue
            header = chunk[idx + sc_len]
            if "h265" in codec_l or "hevc" in codec_l:
                nal_type = (header >> 1) & 0x3F
                if nal_type in (19, 20): return True
            else:
                nal_type = header & 0x1F
                if nal_type == 5: return True
        return False

    def _update_video_codec_cache(self, session, chunk: bytes, codec: Optional[str]) -> None:
        if codec and not session.video_codec:
            session.video_codec = codec
            print(f"[{session.serialno}] Video codec detected: {codec}")
            sys.stdout.flush()

        if session.video_codec:
            codec_l = session.video_codec.lower()
            if ("h265" in codec_l or "hevc" in codec_l) and session._last_vps and session._last_sps and session._last_pps and session._last_idr: return
            if not ("h265" in codec_l or "hevc" in codec_l) and session._last_sps and session._last_pps and session._last_idr: return

        session._video_parse_buffer.extend(chunk)
        if len(session._video_parse_buffer) > 512 * 1024:
            session._video_parse_buffer = session._video_parse_buffer[-128 * 1024 :]

        buf = bytes(session._video_parse_buffer)
        starts = self._find_start_codes(buf)
        if len(starts) < 2: return

        for (idx, sc_len), (next_idx, _next_len) in zip(starts, starts[1:]):
            nal = buf[idx:next_idx]
            if len(nal) <= sc_len: continue
            header = nal[sc_len]
            codec_l = (session.video_codec or "").lower()
            if "h265" in codec_l or "hevc" in codec_l:
                nal_type = (header >> 1) & 0x3F
                if nal_type == 32: session._last_vps = nal
                elif nal_type == 33: session._last_sps = nal
                elif nal_type == 34: session._last_pps = nal
                elif nal_type in (19, 20): session._last_idr = nal
            else:
                nal_type = header & 0x1F
                if nal_type == 7: session._last_sps = nal
                elif nal_type == 8: session._last_pps = nal
                elif nal_type == 5: session._last_idr = nal

        last_start_idx, _ = starts[-1]
        session._video_parse_buffer = bytearray(buf[last_start_idx:])

    async def _start_livestream(self, serialno):
        session = self.cameras.get(serialno)
        if not self.ws or not session: return
        self._ensure_async_primitives(serialno)

        async with session.livestream_lock:
            current_time = time.time()
            if session.livestream_active or (current_time - session.last_livestream_start) < 2.0:
                return
            
            session.livestream_active = True
            session.last_livestream_start = current_time
            msg = START_P2P_LIVESTREAM_MESSAGE.copy()
            msg["serialNumber"] = serialno
            try:
                await self.ws.send_message(json.dumps(msg))
            except Exception as e:
                session.livestream_active = False

    async def _stop_livestream(self, serialno):
        session = self.cameras.get(serialno)
        if not self.ws or not session: return
        self._ensure_async_primitives(serialno)

        async with session.livestream_lock:
            if not session.livestream_active: return
            
            session.livestream_active = False
            msg = STOP_P2P_LIVESTREAM_MESSAGE.copy()
            msg["serialNumber"] = serialno
            try:
                await self.ws.send_message(json.dumps(msg))
            except Exception as e:
                pass

    async def _start_talkback(self, serialno):
        session = self.cameras.get(serialno)
        if not self.ws or not session: return
        self._ensure_async_primitives(serialno)

        async with session.talkback_lock:
            current_time = time.time()
            if session.talkback_active or (current_time - session.last_talkback_start) < 1.0: return
            
            session.talkback_active = True
            session.last_talkback_start = current_time
            msg = START_TALKBACK.copy()
            msg["serialNumber"] = serialno
            try:
                await self.ws.send_message(json.dumps(msg))
            except Exception as e:
                session.talkback_active = False

    async def _stop_talkback(self, serialno):
        session = self.cameras.get(serialno)
        if not self.ws or not session: return
        self._ensure_async_primitives(serialno)

        async with session.talkback_lock:
            if not session.talkback_active: return
            session.talkback_active = False
            msg = STOP_TALKBACK.copy()
            msg["serialNumber"] = serialno
            try:
                await self.ws.send_message(json.dumps(msg))
            except Exception:
                pass

    async def _send_talkback_data(self, serialno, data):
        session = self.cameras.get(serialno)
        if not self.ws or not session or not session.talkback_active: return
        
        msg = SEND_TALKBACK_AUDIO_DATA.copy()
        msg["serialNumber"] = serialno
        msg["buffer"] = data
        try:
            await self.ws.send_message(json.dumps(msg))
        except Exception:
            pass

    async def on_open(self):
        print(f"[WS_LIFECYCLE] WebSocket connection opened")
        sys.stdout.flush()
        if self.ws_closed_event is not None:
            self.ws_closed_event.clear()

    async def on_close(self):
        print(f"[WS_LIFECYCLE] WebSocket connection closed")
        sys.stdout.flush()
        self.ws = None
        
        for session in self.cameras.values():
            self._ensure_async_primitives(session.serialno)
            async with session.livestream_lock:
                session.livestream_active = False
            async with session.talkback_lock:
                session.talkback_active = False
            try:
                session.video_thread.ready_to_accept.clear()
                session.audio_thread.ready_to_accept.clear()
            except Exception:
                pass
                
        if self.ws_closed_event is not None:
            self.ws_closed_event.set()

    def _update_event_timestamp(self):
        self.last_event_time = time.time()

    async def _report_event_statistics(self) -> None:
        while not self.run_event.is_set():
            try:
                await asyncio.sleep(30)
                
                any_active = any(len(s.video_thread.queues) > 0 or len(s.audio_thread.queues) > 0 for s in self.cameras.values())
                
                if any_active:
                    elapsed = time.time() - self.event_stats_start_time
                    video_rate = self.event_stats_video_count / elapsed if elapsed > 0 else 0
                    audio_rate = self.event_stats_audio_count / elapsed if elapsed > 0 else 0
                    print(f"[WS_STATS] {self.event_stats_video_count} video frames ({video_rate:.1f}/s), {self.event_stats_audio_count} audio frames ({audio_rate:.1f}/s) in {elapsed:.0f}s")
                    sys.stdout.flush()
                    
                    self.event_stats_video_count = 0
                    self.event_stats_audio_count = 0
                    self.event_stats_start_time = time.time()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _monitor_websocket_health(self) -> None:
        while not self.run_event.is_set():
            try:
                await asyncio.sleep(5)
                
                for session in self.cameras.values():
                    session.video_thread.update_threads()
                    session.audio_thread.update_threads()

                any_active = any(len(s.video_thread.queues) > 0 or len(s.audio_thread.queues) > 0 for s in self.cameras.values())
                
                if any_active:
                    time_since_event = time.time() - self.last_event_time
                    if time_since_event > self.ws_event_timeout_seconds:
                        if self.ws and hasattr(self.ws, "ws") and not self.ws.ws.closed:
                            try:
                                await self.ws.ws.close()
                            except Exception:
                                pass
                        self.last_event_time = time.time()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def on_error(self, message):
        self._update_event_timestamp()

    async def on_message(self, message):
        self._update_event_timestamp()
        payload = message.json()
        message_type: str = payload["type"]
        
        if message_type == "result":
            message_id = payload["messageId"]
            
            if message_id == START_LISTENING_MESSAGE["messageId"]:
                message_result = payload[message_type]
                states = message_result["state"]
                devices = states.get("devices") or []
                
                camera_serials = []
                for dev in devices:
                    if isinstance(dev, str):
                        camera_serials.append(dev)
                    elif isinstance(dev, dict) and "serialNumber" in dev:
                        if dev.get("type", 0) != "station": 
                            camera_serials.append(dev["serialNumber"])

                camera_serials.sort()
                camera_serials = camera_serials[:5]
                
                for idx, serial in enumerate(camera_serials):
                    if serial not in self.cameras:
                        base_port = self.base_port_start + (idx * 3)
                        print(f"[INIT] Creating P2P streams for {serial} on ports {base_port}-{base_port+2}")
                        sys.stdout.flush()
                        session = CameraSession(serial, base_port, self, run_event)
                        session.video_thread.start()
                        session.audio_thread.start()
                        session.backchannel_thread.start()
                        self.cameras[serial] = session
                        
                        # Trigger inicial se threads ja existirem
                        if len(session.video_thread.queues) > 0:
                            self.schedule_start_livestream(serial)
            
            serial_number = payload.get("serialNumber")
            session = self.cameras.get(serial_number) if serial_number else None
            
            if session:
                if message_id == "start_livestream" and payload.get("success"):
                    self._ensure_async_primitives(serial_number)
                    async with session.livestream_lock: session.livestream_active = True
                elif message_id == "stop_livestream" and payload.get("success"):
                    self._ensure_async_primitives(serial_number)
                    async with session.livestream_lock: session.livestream_active = False
                elif message_id == "start_talkback" and payload.get("success"):
                    self._ensure_async_primitives(serial_number)
                    async with session.talkback_lock: session.talkback_active = True
                elif message_id == "stop_talkback" and payload.get("success"):
                    self._ensure_async_primitives(serial_number)
                    async with session.talkback_lock: session.talkback_active = False
                
                if message_id == TALKBACK_RESULT_MESSAGE["messageId"] and payload.get("errorCode") == "device_talkback_not_running":
                    await self._start_talkback(serial_number)

        if message_type == "event":
            message = payload[message_type]
            event_type = message["event"]
            
            serial_number = message.get("serialNumber")
            if not serial_number:
                serial_number = message.get("device", {}).get("serialNumber")

            session = self.cameras.get(serial_number) if serial_number else None
            if not session: return

            if event_type == "livestream video data":
                self.event_stats_video_count += 1
            elif event_type == "livestream audio data":
                self.event_stats_audio_count += 1
            
            if event_type == "livestream audio data":
                if not session.audio_thread.ready_to_accept.is_set():
                    session.audio_thread.ready_to_accept.set()

                try:
                    meta = message.get("metadata") or {}
                    codec = meta.get("audioCodec") if isinstance(meta, dict) else None
                    if codec and not session.audio_codec:
                        session.audio_codec = codec
                except Exception:
                    pass
                
                event_value = message[EVENT_CONFIGURATION[event_type]["value"]]
                if EVENT_CONFIGURATION[event_type]["type"] == "event":
                    for queue in session.audio_thread.queues:
                        while queue.qsize() > 5:
                            try: queue.get(False)
                            except: break
                        try: queue.put(event_value, block=False)
                        except: pass
            
            if event_type == "livestream video data":
                if not session.video_thread.ready_to_accept.is_set():
                    session.video_thread.ready_to_accept.set()

                try:
                    meta = message.get("metadata") or {}
                    codec = meta.get("videoCodec") if isinstance(meta, dict) else None
                    event_value = message[EVENT_CONFIGURATION[event_type]["value"]]
                    chunk, shape = self._extract_buffer_bytes(event_value)
                    is_idr = False
                    if shape and not session._video_buffer_shape:
                        session._video_buffer_shape = shape
                    if chunk:
                        self._update_video_codec_cache(session, chunk, codec)
                        is_idr = self._is_idr_frame(session, chunk)
                        if is_idr:
                            any_waiting = any(thread.skip_non_idr for thread in session.video_thread.my_threads)
                            if any_waiting:
                                for thread in session.video_thread.my_threads: thread.skip_non_idr = False
                            session.video_thread.skip_non_idr = False
                except Exception:
                    pass
                
                event_value = message[EVENT_CONFIGURATION[event_type]["value"]]
                if EVENT_CONFIGURATION[event_type]["type"] == "event":
                    for thread in session.video_thread.my_threads:
                        if thread.skip_non_idr and not is_idr: continue
                        while thread.queue.qsize() > 3:
                            try: thread.queue.get(False)
                            except: break
                        try: thread.queue.put(event_value, block=False)
                        except: pass
            
            if event_type == "livestream error":
                self._ensure_async_primitives(serial_number)
                async with session.livestream_lock:
                    session.livestream_active = False
                    try:
                        if session.video_thread: session.video_thread.ready_to_accept.clear()
                        if session.audio_thread: session.audio_thread.ready_to_accept.clear()
                    except Exception: pass
                await asyncio.sleep(1.5)
                try:
                    if self.ws and len(session.video_thread.queues) > 0:
                        await self._start_livestream(serial_number)
                except Exception: pass

c = Connector(run_event)

async def init_websocket() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Missing argument: expected websocket access token / API key as argv[1]")

    c.set_loop(asyncio.get_running_loop())

    backoff_s = 1.0
    async with aiohttp.ClientSession() as session:
        while not run_event.is_set():
            sys.stdout.flush()
            ws: EufySecurityWebSocket = EufySecurityWebSocket(
                "402f1039-eufy-security-ws",
                sys.argv[1],
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
                c.last_event_time = time.time()
                
                if c.ws_monitor_task is None or c.ws_monitor_task.done():
                    c.ws_monitor_task = asyncio.create_task(c._monitor_websocket_health())
                
                if c.stats_reporter_task is None or c.stats_reporter_task.done():
                    c.stats_reporter_task = asyncio.create_task(c._report_event_statistics())

                await ws.send_message(json.dumps(SET_API_SCHEMA))
                await ws.send_message(json.dumps(START_LISTENING_MESSAGE))
                await ws.send_message(json.dumps(DRIVER_CONNECT_MESSAGE))
                sys.stdout.flush()

                backoff_s = 1.0

                if c.ws_closed_event is not None:
                    await c.ws_closed_event.wait()
                else:
                    while not run_event.is_set():
                        await asyncio.sleep(1)
            except Exception as ex:
                pass
            finally:
                if c.ws_monitor_task and not c.ws_monitor_task.done():
                    c.ws_monitor_task.cancel()
                    try: await c.ws_monitor_task
                    except asyncio.CancelledError: pass
                    c.ws_monitor_task = None
                
                if c.stats_reporter_task and not c.stats_reporter_task.done():
                    c.stats_reporter_task.cancel()
                    try: await c.stats_reporter_task
                    except asyncio.CancelledError: pass
                    c.stats_reporter_task = None
                
                c.setWs(None)

            if run_event.is_set():
                break
            await asyncio.sleep(backoff_s)
            backoff_s = min(backoff_s * 2.0, 30.0)

    c.stop()

if __name__ == "__main__":
    try:
        asyncio.run(init_websocket())
    except KeyboardInterrupt:
        print("Interrupted by user")
