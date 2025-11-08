# """
# Ultra-fast UDP file transfer with proper flow control
# Strategy: UNSEQUENCED for speed + application-level selective retransmission
# Research: Aspera FASP, RBUDP, UDT protocols

# CRITICAL FIXES from analysis:
# 1. UNSEQUENCED for maximum speed (no head-of-line blocking)
# 2. Application-level bitmap ACK (like RBUDP)
# 3. Receiver-driven flow control (prevents server flooding)
# 4. Separate control channel for ACKs
# 5. Rate limiting based on RTT feedback

# Windows: pip install pyenet-updated numba numpy xxhash psutil
# """

# import ctypes
# import logging
# import mmap
# import multiprocessing as mp
# import os
# import platform
# import socket
# import struct
# import sys
# import time
# from collections import deque
# from pathlib import Path
# from typing import Set, Tuple, Dict

# import numpy as np
# import xxhash
# from numba import jit

# try:
#     import enet
#     ENET_AVAILABLE = True
# except ImportError:
#     ENET_AVAILABLE = False

# try:
#     import psutil
#     PSUTIL_AVAILABLE = True
# except ImportError:
#     PSUTIL_AVAILABLE = False

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
# logger = logging.getLogger(__name__)

# # Optimized for speed + stability
# DEFAULT_WORKERS = 8
# DEFAULT_CHUNK_SIZE = 1048576  # 1MB
# DEFAULT_WINDOW_SIZE = 64  # Chunks in flight per worker

# TRANSFER_DIR = Path("./transfers")
# TRANSFER_DIR.mkdir(exist_ok=True)

# # Windows API for concurrent file access
# if platform.system() == 'Windows':
#     import msvcrt

#     GENERIC_READ = 0x80000000
#     GENERIC_WRITE = 0x40000000
#     FILE_SHARE_READ = 0x00000001
#     FILE_SHARE_WRITE = 0x00000002
#     OPEN_EXISTING = 3
#     FILE_ATTRIBUTE_NORMAL = 0x80
#     FILE_FLAG_RANDOM_ACCESS = 0x10000000
#     FILE_FLAG_WRITE_THROUGH = 0x80000000

#     kernel32 = ctypes.windll.kernel32


# @jit(nopython=True, cache=True, fastmath=True, inline='always')
# def fast_hash(data: np.ndarray) -> np.uint32:
#     """Optimized hash"""
#     h = np.uint32(2166136261)
#     prime = np.uint32(16777619)

#     for i in range(0, len(data), 16):
#         h = np.uint32((h ^ data[i]) * prime)

#     return h


# @jit(nopython=True, cache=True, fastmath=True)
# def verify_hash(data: np.ndarray, expected: np.uint32) -> bool:
#     """Fast verify"""
#     return fast_hash(data) == expected


# class PacketType:
#     CHUNK_DATA = 1
#     CHUNK_REQUEST = 2
#     BITMAP_ACK = 3  # Receiver sends bitmap of received chunks
#     WORKER_READY = 4
#     FILE_METADATA = 5
#     RATE_FEEDBACK = 6  # Client sends rate info to server


# def open_shared_file_windows(filepath: str, file_size: int):
#     """Open file with Windows API for concurrent access"""
#     if platform.system() != 'Windows':
#         raise OSError("Windows-only function")

#     import msvcrt

#     try:
#         filepath_wide = ctypes.create_unicode_buffer(filepath)

#         handle = kernel32.CreateFileW(
#             filepath_wide,
#             GENERIC_READ | GENERIC_WRITE,
#             FILE_SHARE_READ | FILE_SHARE_WRITE,
#             None,
#             OPEN_EXISTING,
#             FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_WRITE_THROUGH,
#             None
#         )

#         if handle == -1 or handle == 0:
#             error_code = kernel32.GetLastError()
#             raise OSError(f"CreateFile failed: {error_code}")

#         fd = msvcrt.open_osfhandle(handle, os.O_RDWR | os.O_BINARY)
#         return fd, handle

#     except Exception as e:
#         logger.error(f"Failed to open file: {e}")
#         raise


# def write_at_offset_windows(handle, offset: int, data: bytes):
#     """Direct write at offset using Windows API"""
#     if platform.system() != 'Windows':
#         raise OSError("Windows-only function")

#     offset_low = ctypes.c_ulong(offset & 0xFFFFFFFF)
#     offset_high = ctypes.c_long(offset >> 32)

#     result = kernel32.SetFilePointer(handle, offset_low, ctypes.byref(offset_high), 0)

#     if result == 0xFFFFFFFF:
#         error = kernel32.GetLastError()
#         if error != 0:
#             raise OSError(f"SetFilePointer failed: {error}")

#     bytes_written = ctypes.c_ulong()
#     success = kernel32.WriteFile(handle, data, len(data), ctypes.byref(bytes_written), None)

#     if not success:
#         raise OSError(f"WriteFile failed: {kernel32.GetLastError()}")

#     return bytes_written.value


# def server_worker(worker_id: int, port: int, filepath: str, chunk_size: int, window_size: int):
#     """
#     Server worker with UNSEQUENCED + receiver-driven flow control.

#     Strategy:
#     - Send chunks as fast as client requests (UNSEQUENCED for speed)
#     - Client controls rate via BITMAP_ACK feedback
#     - Stay alive until client explicitly disconnects
#     """
#     try:
#         file_path = Path(filepath)
#         f = open(file_path, 'rb')
#         file_size = file_path.stat().st_size
#         m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

#         # ENet host with 2 channels: data (0) and control (1)
#         address = enet.Address(b'0.0.0.0', port + worker_id)

#         try:
#             host = enet.Host(
#                 address,
#                 1,  # One peer
#                 2,  # Two channels
#                 0,  # Unlimited incoming
#                 0   # Unlimited outgoing (let network handle it)
#             )
#         except Exception as bind_error:
#             logger.error(f"Worker {worker_id}: Failed to bind port {port + worker_id}: {bind_error}")
#             logger.error(f"Worker {worker_id}: Port may be in use or firewall blocking")
#             m.close()
#             f.close()
#             return

#         logger.info(f"Worker {worker_id}: Listening on 0.0.0.0:{port + worker_id}")

#         peer = None
#         chunks_sent = 0
#         last_stats = time.time()
#         bytes_sent_interval = 0

#         # Track client's received status (updated via bitmap ACKs)
#         client_received = set()
#         last_request_time = time.time()

#         while True:
#             # CRITICAL: Stay responsive but don't timeout too fast
#             event = host.service(100)  # 100ms timeout for better responsiveness

#             if event.type == enet.EVENT_TYPE_CONNECT:
#                 peer = event.peer
#                 last_request_time = time.time()
#                 logger.info(f"Worker {worker_id}: Client connected")

#             elif event.type == enet.EVENT_TYPE_RECEIVE:
#                 if not peer:
#                     continue

#                 last_request_time = time.time()  # Reset timeout on any activity
#                 packet_type = event.packet.data[0]

#                 if packet_type == PacketType.CHUNK_REQUEST:
#                     # Client requests specific chunk
#                     chunk_index, = struct.unpack('!I', event.packet.data[1:5])

#                     offset = chunk_index * chunk_size
#                     if offset >= file_size:
#                         logger.warning(f"Worker {worker_id}: Invalid chunk {chunk_index}")
#                         continue

#                     end = min(offset + chunk_size, file_size)
#                     chunk_actual_size = end - offset

#                     # Zero-copy read
#                     chunk_data = m[offset:end]

#                     # Fast hash
#                     data_array = np.frombuffer(chunk_data, dtype=np.uint8)
#                     checksum = fast_hash(data_array)

#                     # Send UNSEQUENCED for maximum speed
#                     packet_data = struct.pack('!BIII', PacketType.CHUNK_DATA, chunk_index,
#                                             chunk_actual_size, checksum) + chunk_data
#                     packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#                     peer.send(0, packet)  # Data channel

#                     chunks_sent += 1
#                     bytes_sent_interval += chunk_actual_size

#                     # Flush periodically to prevent buffer buildup
#                     if chunks_sent % 10 == 0:
#                         host.flush()

#                 elif packet_type == PacketType.BITMAP_ACK:
#                     # Client sends bitmap of received chunks
#                     num_received, = struct.unpack('!I', event.packet.data[1:5])
#                     if num_received > 0 and len(event.packet.data) >= 5 + num_received * 4:
#                         received_chunks = struct.unpack(f'!{num_received}I',
#                                                        event.packet.data[5:5 + num_received * 4])
#                         client_received.update(received_chunks)
#                         logger.debug(f"Worker {worker_id}: Client has {len(client_received)} chunks")

#                 # Stats
#                 current_time = time.time()
#                 if current_time - last_stats >= 2.0:
#                     elapsed = current_time - last_stats
#                     if elapsed > 0:
#                         speed_mbps = (bytes_sent_interval / elapsed) / (1024 * 1024)
#                         logger.info(f"Worker {worker_id}: {chunks_sent} sent, {speed_mbps:.1f} MB/s, "
#                                   f"client has {len(client_received)} chunks")
#                         bytes_sent_interval = 0
#                         last_stats = current_time

#             elif event.type == enet.EVENT_TYPE_DISCONNECT:
#                 logger.info(f"Worker {worker_id} client disconnected: {chunks_sent} chunks sent")
#                 break

#             # Check for client timeout (no activity for 60 seconds)
#             if peer and (time.time() - last_request_time) > 60:
#                 logger.warning(f"Worker {worker_id}: Client timeout, closing")
#                 break

#         m.close()
#         f.close()

#     except Exception as e:
#         logger.error(f"Worker {worker_id} error: {e}")
#         import traceback
#         traceback.print_exc()


# def client_worker_windows(worker_id: int, server_host: str, port: int, chunk_indices: list,
#                           output_path: str, file_size: int, chunk_size: int, window_size: int,
#                           result_queue: mp.Queue):
#     """
#     Client worker with UNSEQUENCED + application-level retransmission.

#     Strategy (like RBUDP):
#     1. Request chunks in batches (window-based)
#     2. Receive UNSEQUENCED for low latency
#     3. Track received chunks in bitmap
#     4. Periodically send bitmap ACK to server
#     5. Retransmit missing chunks at end
#     """
#     if platform.system() != 'Windows':
#         logger.error(f"Worker {worker_id}: Windows required")
#         return

#     import msvcrt

#     try:
#         fd, handle = open_shared_file_windows(output_path, file_size)
#         logger.info(f"Worker {worker_id}: File opened")

#         # Connect
#         client_host = enet.Host(None, 1, 2, 0, 0)
#         address = enet.Address(server_host.encode('utf-8'), port + worker_id)
#         peer = client_host.connect(address, 2)

#         # Wait for connection with retry
#         logger.info(f"Worker {worker_id}: Connecting to port {port + worker_id}...")

#         connected = False
#         for attempt in range(3):
#             event = client_host.service(2000)  # 2 second timeout per attempt
#             if event.type == enet.EVENT_TYPE_CONNECT:
#                 connected = True
#                 break

#             if attempt < 2:
#                 logger.warning(f"Worker {worker_id}: Connection attempt {attempt + 1} failed, retrying...")
#                 time.sleep(1)

#         if not connected:
#             logger.error(f"Worker {worker_id}: Connection failed after 3 attempts to port {port + worker_id}")
#             kernel32.CloseHandle(handle)
#             result_queue.put(('worker_done', worker_id, 0))
#             return

#         logger.info(f"Worker {worker_id}: Connected to port {port + worker_id}")

#         # State
#         received = set()
#         requested = set()

#         # Request initial window - use smaller window to start
#         initial_window = min(window_size // 2, 32, len(chunk_indices))
#         logger.info(f"Worker {worker_id}: Requesting initial window of {initial_window} chunks")

#         for i in range(initial_window):
#             chunk_idx = chunk_indices[i]
#             packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#             packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#             peer.send(0, packet)
#             requested.add(chunk_idx)

#         # Flush to ensure requests sent
#         client_host.flush()

#         next_to_request = initial_window
#         last_progress = time.time()
#         last_ack_sent = time.time()

#         # Diagnostic counters
#         packets_received = 0
#         hash_failures = 0
#         write_failures = 0
#         duplicates = 0

#         # Main receive loop
#         while len(received) < len(chunk_indices):
#             # Frequent service for fast packet processing
#             event = client_host.service(5)

#             if event.type == enet.EVENT_TYPE_RECEIVE:
#                 packets_received += 1
#                 packet_type = event.packet.data[0]

#                 if packet_type == PacketType.CHUNK_DATA:
#                     if len(event.packet.data) < 13:
#                         continue

#                     chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
#                                                                             event.packet.data[1:13])
#                     chunk_data = event.packet.data[13:13 + received_chunk_size]

#                     if chunk_idx not in chunk_indices:
#                         continue

#                     if chunk_idx in received:
#                         duplicates += 1
#                         continue

#                     if len(chunk_data) != received_chunk_size:
#                         logger.warning(f"Worker {worker_id}: Chunk {chunk_idx} size mismatch")
#                         continue

#                     # Verify hash
#                     data_array = np.frombuffer(chunk_data, dtype=np.uint8)
#                     if not verify_hash(data_array, np.uint32(checksum)):
#                         hash_failures += 1
#                         logger.warning(f"Worker {worker_id}: Chunk {chunk_idx} hash failed")
#                         continue

#                     # Write to disk
#                     try:
#                         offset = chunk_idx * chunk_size
#                         bytes_written = write_at_offset_windows(handle, offset, chunk_data)

#                         if bytes_written != received_chunk_size:
#                             write_failures += 1
#                             logger.error(f"Worker {worker_id}: Write failed for {chunk_idx}")
#                             continue
#                     except Exception as e:
#                         write_failures += 1
#                         logger.error(f"Worker {worker_id}: Write error {chunk_idx}: {e}")
#                         continue

#                     # Success
#                     received.add(chunk_idx)

#                     # Request more chunks to maintain window
#                     while next_to_request < len(chunk_indices) and len(requested - received) < window_size:
#                         new_chunk_idx = chunk_indices[next_to_request]
#                         if new_chunk_idx not in requested:
#                             packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, new_chunk_idx)
#                             packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#                             peer.send(0, packet)
#                             requested.add(new_chunk_idx)
#                         next_to_request += 1

#                     # Progress with diagnostics
#                     current_time = time.time()
#                     if current_time - last_progress >= 2.0:
#                         progress = len(received) / len(chunk_indices) * 100
#                         in_flight = len(requested - received)
#                         logger.info(f"Worker {worker_id}: {len(received)}/{len(chunk_indices)} "
#                                   f"({progress:.1f}%), in-flight: {in_flight}, "
#                                   f"pkts: {packets_received}, hash_fail: {hash_failures}, "
#                                   f"write_fail: {write_failures}, dup: {duplicates}")
#                         last_progress = current_time

#             elif event.type == enet.EVENT_TYPE_DISCONNECT:
#                 logger.warning(f"Worker {worker_id}: Server disconnected unexpectedly")
#                 logger.warning(f"Worker {worker_id}: Diagnostics - received: {len(received)}, "
#                              f"requested: {len(requested)}, missing: {len(requested - received)}")
#                 break

#             # Send periodic bitmap ACK (like RBUDP)
#             current_time = time.time()
#             if current_time - last_ack_sent >= 1.0 and len(received) > 0:
#                 received_list = sorted(list(received))
#                 if received_list:
#                     # Send up to 1000 chunk IDs at a time
#                     batch_size = min(1000, len(received_list))
#                     ack_data = struct.pack(f'!BI{batch_size}I',
#                                           PacketType.BITMAP_ACK,
#                                           batch_size,
#                                           *received_list[:batch_size])
#                     packet = enet.Packet(ack_data, enet.PACKET_FLAG_RELIABLE)
#                     peer.send(1, packet)  # Control channel
#                     last_ack_sent = current_time

#             # Check for stalled transfer - re-request missing chunks
#             if current_time - last_progress > 5.0:
#                 in_flight_chunks = requested - received
#                 if len(in_flight_chunks) > 0:
#                     # Re-request first 10 in-flight chunks
#                     retry_chunks = sorted(list(in_flight_chunks))[:10]
#                     logger.info(f"Worker {worker_id}: Stalled, retrying {len(retry_chunks)} chunks")

#                     for chunk_idx in retry_chunks:
#                         packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#                         packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#                         peer.send(0, packet)

#                     client_host.flush()
#                     last_progress = current_time

#         # Retransmission phase for missing chunks
#         missing = set(chunk_indices) - received
#         if missing:
#             logger.info(f"Worker {worker_id}: Retransmitting {len(missing)} missing chunks")

#             # Send all missing chunk requests with RELIABLE
#             for chunk_idx in sorted(missing):
#                 packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#                 packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
#                 peer.send(0, packet)

#             # Flush to ensure all requests sent
#             client_host.flush()

#             # Wait for retransmitted chunks with longer timeout
#             retrans_start = time.time()
#             retrans_received = 0

#             while len(received) < len(chunk_indices) and (time.time() - retrans_start) < 60:
#                 event = client_host.service(100)  # Longer service time for retransmit

#                 if event.type == enet.EVENT_TYPE_RECEIVE:
#                     packet_type = event.packet.data[0]

#                     if packet_type == PacketType.CHUNK_DATA:
#                         if len(event.packet.data) < 13:
#                             continue

#                         chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
#                                                                                 event.packet.data[1:13])
#                         chunk_data = event.packet.data[13:13 + received_chunk_size]

#                         if chunk_idx in received or chunk_idx not in chunk_indices:
#                             continue

#                         if len(chunk_data) != received_chunk_size:
#                             logger.warning(f"Worker {worker_id}: Retransmit chunk {chunk_idx} size mismatch")
#                             continue

#                         # Verify hash
#                         data_array = np.frombuffer(chunk_data, dtype=np.uint8)
#                         if not verify_hash(data_array, np.uint32(checksum)):
#                             logger.warning(f"Worker {worker_id}: Retransmit chunk {chunk_idx} hash failed")
#                             continue

#                         # Write to disk
#                         try:
#                             offset = chunk_idx * chunk_size
#                             bytes_written = write_at_offset_windows(handle, offset, chunk_data)

#                             if bytes_written == received_chunk_size:
#                                 received.add(chunk_idx)
#                                 retrans_received += 1

#                                 if retrans_received % 10 == 0:
#                                     logger.info(f"Worker {worker_id}: Retransmit progress {retrans_received}/{len(missing)}")
#                         except Exception as e:
#                             logger.error(f"Worker {worker_id}: Retransmit write error {chunk_idx}: {e}")
#                             continue

#                 elif event.type == enet.EVENT_TYPE_DISCONNECT:
#                     logger.warning(f"Worker {worker_id}: Server disconnected during retransmit")
#                     break

#             if len(received) < len(chunk_indices):
#                 still_missing = len(chunk_indices) - len(received)
#                 logger.warning(f"Worker {worker_id}: Still missing {still_missing} chunks after retransmit")

#         # Final flush and close
#         kernel32.FlushFileBuffers(handle)
#         kernel32.CloseHandle(handle)

#         # Graceful disconnect
#         if peer:
#             peer.disconnect_later()

#             # Wait for disconnect event
#             timeout = time.time() + 2
#             while time.time() < timeout:
#                 event = client_host.service(100)
#                 if event.type == enet.EVENT_TYPE_DISCONNECT:
#                     break

#         result_queue.put(('worker_done', worker_id, len(received)))
#         logger.info(f"Worker {worker_id} complete: {len(received)}/{len(chunk_indices)}")

#     except Exception as e:
#         logger.error(f"Worker {worker_id} error: {e}")
#         import traceback
#         traceback.print_exc()
#         result_queue.put(('worker_done', worker_id, 0))


# def client_worker_unix(worker_id: int, server_host: str, port: int, chunk_indices: list,
#                        output_path: str, file_size: int, chunk_size: int, window_size: int,
#                        result_queue: mp.Queue):
#     """Unix/Linux client worker using pwrite"""
#     try:
#         fd = os.open(output_path, os.O_RDWR)
#         logger.info(f"Worker {worker_id}: File opened (fd: {fd})")

#         client_host = enet.Host(None, 1, 2, 0, 0)
#         address = enet.Address(server_host.encode('utf-8'), port + worker_id)
#         peer = client_host.connect(address, 2)

#         event = client_host.service(5000)
#         if event.type != enet.EVENT_TYPE_CONNECT:
#             logger.error(f"Worker {worker_id}: Connection failed")
#             os.close(fd)
#             return

#         logger.info(f"Worker {worker_id}: Connected")

#         received = set()
#         requested = set()

#         # Request initial window - smaller to start
#         initial_window = min(window_size // 2, 32, len(chunk_indices))
#         logger.info(f"Worker {worker_id}: Requesting initial {initial_window} chunks")

#         for i in range(initial_window):
#             chunk_idx = chunk_indices[i]
#             packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#             packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#             peer.send(0, packet)
#             requested.add(chunk_idx)

#         client_host.flush()

#         next_to_request = initial_window
#         last_progress = time.time()
#         last_ack_sent = time.time()

#         # Diagnostics
#         packets_received = 0
#         hash_failures = 0

#         while len(received) < len(chunk_indices):
#             event = client_host.service(5)

#             if event.type == enet.EVENT_TYPE_RECEIVE:
#                 packets_received += 1
#                 packet_type = event.packet.data[0]

#                 if packet_type == PacketType.CHUNK_DATA:
#                     if len(event.packet.data) < 13:
#                         continue

#                     chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
#                                                                             event.packet.data[1:13])
#                     chunk_data = event.packet.data[13:13 + received_chunk_size]

#                     if chunk_idx not in chunk_indices or chunk_idx in received:
#                         continue

#                     data_array = np.frombuffer(chunk_data, dtype=np.uint8)
#                     if not verify_hash(data_array, np.uint32(checksum)):
#                         hash_failures += 1
#                         continue

#                     try:
#                         offset = chunk_idx * chunk_size
#                         bytes_written = os.pwrite(fd, chunk_data, offset)

#                         if bytes_written == received_chunk_size:
#                             received.add(chunk_idx)
#                     except Exception as e:
#                         logger.error(f"Worker {worker_id}: Write error: {e}")
#                         continue

#                     # Maintain window
#                     while next_to_request < len(chunk_indices) and len(requested - received) < window_size:
#                         new_chunk_idx = chunk_indices[next_to_request]
#                         if new_chunk_idx not in requested:
#                             packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, new_chunk_idx)
#                             packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#                             peer.send(0, packet)
#                             requested.add(new_chunk_idx)
#                         next_to_request += 1

#                     # Progress
#                     current_time = time.time()
#                     if current_time - last_progress >= 2.0:
#                         progress = len(received) / len(chunk_indices) * 100
#                         logger.info(f"Worker {worker_id}: {len(received)}/{len(chunk_indices)} ({progress:.1f}%), "
#                                   f"pkts: {packets_received}, hash_fail: {hash_failures}")
#                         last_progress = current_time

#             elif event.type == enet.EVENT_TYPE_DISCONNECT:
#                 logger.warning(f"Worker {worker_id}: Server disconnected")
#                 break

#             # Periodic ACK
#             current_time = time.time()
#             if current_time - last_ack_sent >= 1.0 and len(received) > 0:
#                 received_list = sorted(list(received))[:1000]
#                 if received_list:
#                     ack_data = struct.pack(f'!BI{len(received_list)}I',
#                                           PacketType.BITMAP_ACK,
#                                           len(received_list),
#                                           *received_list)
#                     packet = enet.Packet(ack_data, enet.PACKET_FLAG_RELIABLE)
#                     peer.send(1, packet)
#                     last_ack_sent = current_time

#             # Stall detection and retry
#             if current_time - last_progress > 5.0:
#                 in_flight = requested - received
#                 if len(in_flight) > 0:
#                     retry = sorted(list(in_flight))[:10]
#                     logger.info(f"Worker {worker_id}: Stalled, retrying {len(retry)} chunks")
#                     for chunk_idx in retry:
#                         packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#                         packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#                         peer.send(0, packet)
#                     client_host.flush()
#                     last_progress = current_time

#         # Retransmit missing
#         missing = set(chunk_indices) - received
#         if missing:
#             logger.info(f"Worker {worker_id}: Retransmitting {len(missing)} chunks")

#             for chunk_idx in sorted(missing):
#                 packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#                 packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
#                 peer.send(0, packet)

#             client_host.flush()

#             retrans_start = time.time()
#             retrans_received = 0

#             while len(received) < len(chunk_indices) and (time.time() - retrans_start) < 60:
#                 event = client_host.service(100)

#                 if event.type == enet.EVENT_TYPE_RECEIVE:
#                     packet_type = event.packet.data[0]

#                     if packet_type == PacketType.CHUNK_DATA:
#                         if len(event.packet.data) < 13:
#                             continue

#                         chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
#                                                                                 event.packet.data[1:13])
#                         chunk_data = event.packet.data[13:13 + received_chunk_size]

#                         if chunk_idx in received or chunk_idx not in chunk_indices:
#                             continue

#                         data_array = np.frombuffer(chunk_data, dtype=np.uint8)
#                         if not verify_hash(data_array, np.uint32(checksum)):
#                             continue

#                         try:
#                             offset = chunk_idx * chunk_size
#                             bytes_written = os.pwrite(fd, chunk_data, offset)

#                             if bytes_written == received_chunk_size:
#                                 received.add(chunk_idx)
#                                 retrans_received += 1

#                                 if retrans_received % 10 == 0:
#                                     logger.info(f"Worker {worker_id}: Retransmit {retrans_received}/{len(missing)}")
#                         except Exception as e:
#                             logger.error(f"Worker {worker_id}: Retransmit write error: {e}")
#                             continue

#                 elif event.type == enet.EVENT_TYPE_DISCONNECT:
#                     logger.warning(f"Worker {worker_id}: Server disconnected during retransmit")
#                     break

#             if len(received) < len(chunk_indices):
#                 logger.warning(f"Worker {worker_id}: Still missing {len(chunk_indices) - len(received)} chunks")

#         os.fsync(fd)
#         os.close(fd)

#         if peer:
#             peer.disconnect_later()
#             timeout = time.time() + 2
#             while time.time() < timeout:
#                 event = client_host.service(100)
#                 if event.type == enet.EVENT_TYPE_DISCONNECT:
#                     break

#         result_queue.put(('worker_done', worker_id, len(received)))
#         logger.info(f"Worker {worker_id} complete: {len(received)}/{len(chunk_indices)}")

#     except Exception as e:
#         logger.error(f"Worker {worker_id} error: {e}")
#         import traceback
#         traceback.print_exc()
#         result_queue.put(('worker_done', worker_id, 0))


# def metadata_server_process(base_port: int, filepath: str, file_size: int, total_chunks: int, chunk_size: int):
#     """
#     Standalone metadata server process (must be top-level function for multiprocessing).
#     Sends file metadata to clients.
#     """
#     try:
#         # Convert string back to Path inside the process
#         file_path = Path(filepath)

#         address = enet.Address(b'0.0.0.0', base_port)
#         host = enet.Host(address, 8, 1, 0, 0)

#         logger.info(f"Metadata server listening on port {base_port}")

#         while True:
#             event = host.service(100)

#             if event.type == enet.EVENT_TYPE_CONNECT:
#                 logger.info("Client connected to metadata server")

#             elif event.type == enet.EVENT_TYPE_RECEIVE:
#                 if event.packet.data[0] == PacketType.WORKER_READY:
#                     filename_bytes = file_path.name.encode('utf-8')

#                     # Calculate file hash
#                     hasher = xxhash.xxh64()
#                     with open(file_path, 'rb') as f:
#                         while chunk := f.read(chunk_size * 4):
#                             hasher.update(chunk)
#                     file_hash = hasher.hexdigest()

#                     # Send metadata packet
#                     packet_data = struct.pack(
#                         '!BIQQI',
#                         PacketType.FILE_METADATA,
#                         len(filename_bytes),
#                         file_size,
#                         total_chunks,
#                         chunk_size
#                     ) + filename_bytes + file_hash.encode('utf-8')

#                     packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
#                     event.peer.send(0, packet)

#                     logger.info(f"Sent metadata: {file_path.name}, {total_chunks} chunks, {chunk_size} bytes/chunk")

#     except Exception as e:
#         logger.error(f"Metadata server error: {e}")
#         import traceback
#         traceback.print_exc()


# class ParallelServer:
#     """Server with multiple workers"""

#     def __init__(self, host: str, base_port: int, num_workers: int = DEFAULT_WORKERS,
#                  chunk_size: int = DEFAULT_CHUNK_SIZE, window_size: int = DEFAULT_WINDOW_SIZE):
#         self.host = host
#         self.base_port = base_port
#         self.num_workers = num_workers
#         self.chunk_size = chunk_size
#         self.window_size = window_size
#         self.workers = []

#     def serve_file(self, filepath: Path):
#         """Start parallel workers"""
#         if not filepath.exists():
#             logger.error(f"File not found: {filepath}")
#             return

#         file_size = filepath.stat().st_size
#         total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size

#         logger.info("=" * 70)
#         logger.info(f"SERVER STARTING")
#         logger.info("=" * 70)
#         logger.info(f"File: {filepath.name}, {file_size / (1024**2):.1f} MB, {total_chunks} chunks")
#         logger.info(f"Workers: {self.num_workers}, Chunk size: {self.chunk_size / (1024**2):.1f} MB")
#         logger.info(f"Window: {self.window_size} chunks/worker")
#         logger.info(f"Metadata port: {self.base_port}")
#         logger.info(f"Worker ports: {self.base_port + 1} to {self.base_port + self.num_workers}")
#         logger.info("=" * 70)

#         # Start workers FIRST (they need to be listening before client connects)
#         logger.info(f"Starting {self.num_workers} worker processes...")
#         for i in range(self.num_workers):
#             p = mp.Process(
#                 target=server_worker,
#                 args=(i, self.base_port + 1, str(filepath), self.chunk_size, self.window_size)
#             )
#             p.start()
#             self.workers.append(p)
#             logger.info(f"Started worker {i} on port {self.base_port + 1 + i}")

#         # Give workers time to bind to ports
#         logger.info("Waiting for workers to initialize...")
#         time.sleep(2)

#         # Start metadata server LAST (use standalone function, not method)
#         logger.info(f"Starting metadata server on port {self.base_port}...")
#         metadata_proc = mp.Process(
#             target=metadata_server_process,
#             args=(self.base_port, str(filepath), file_size, total_chunks, self.chunk_size)
#         )
#         metadata_proc.start()

#         logger.info("=" * 70)
#         logger.info("SERVER READY - Waiting for clients...")
#         logger.info("=" * 70)

#         try:
#             while any(p.is_alive() for p in self.workers) or metadata_proc.is_alive():
#                 time.sleep(0.5)
#         except KeyboardInterrupt:
#             logger.info("Stopping server...")
#         finally:
#             metadata_proc.terminate()
#             for p in self.workers:
#                 p.terminate()
#             logger.info("Server stopped")


# class ParallelClient:
#     """Client with multiple workers"""

#     def __init__(self, num_workers: int = DEFAULT_WORKERS, window_size: int = DEFAULT_WINDOW_SIZE):
#         self.num_workers = num_workers
#         self.window_size = window_size

#     def receive_file(self, server_host: str, base_port: int) -> bool:
#         """Receive file from server"""
#         try:
#             logger.info(f"Connecting to {server_host}:{base_port}")

#             client_host = enet.Host(None, 1, 1, 0, 0)
#             address = enet.Address(server_host.encode('utf-8'), base_port)
#             peer = client_host.connect(address, 1)

#             event = client_host.service(5000)
#             if event.type != enet.EVENT_TYPE_CONNECT:
#                 logger.error("Connection failed")
#                 return False

#             logger.info("Connected - requesting metadata")

#             config_packet = struct.pack('!B', PacketType.WORKER_READY)
#             packet = enet.Packet(config_packet, enet.PACKET_FLAG_RELIABLE)
#             peer.send(0, packet)

#             # Wait for metadata
#             file_info = None
#             start = time.time()

#             while time.time() - start < 10:
#                 event = client_host.service(100)

#                 if event.type == enet.EVENT_TYPE_RECEIVE:
#                     if event.packet.data[0] == PacketType.FILE_METADATA:
#                         offset = 1
#                         fname_len, = struct.unpack('!I', event.packet.data[offset:offset+4])
#                         offset += 4

#                         file_size, total_chunks, chunk_size = struct.unpack('!QQI',
#                                                                             event.packet.data[offset:offset+20])
#                         offset += 20

#                         filename = event.packet.data[offset:offset+fname_len].decode('utf-8')
#                         offset += fname_len
#                         file_hash = event.packet.data[offset:offset+16].decode('utf-8')

#                         file_info = {
#                             'filename': filename,
#                             'size': file_size,
#                             'total': total_chunks,
#                             'chunk_size': chunk_size,
#                             'hash': file_hash
#                         }

#                         logger.info(f"File: {filename}, {file_size/(1024**2):.1f} MB, {total_chunks} chunks")
#                         break

#             peer.disconnect()

#             if not file_info:
#                 logger.error("No metadata received")
#                 return False

#             output_path = TRANSFER_DIR / file_info['filename']

#             # Pre-allocate file
#             with open(output_path, 'wb') as f:
#                 f.truncate(file_info['size'])

#             logger.info(f"Pre-allocated file: {output_path}")
#             logger.info(f"Waiting for server workers to be ready...")
#             time.sleep(3)  # Give server workers time to bind ports

#             # Distribute chunks among workers
#             chunks_per_worker_base = file_info['total'] // self.num_workers
#             worker_chunks = []

#             for i in range(self.num_workers):
#                 start_chunk = i * chunks_per_worker_base
#                 end_chunk = (start_chunk + chunks_per_worker_base
#                            if i < self.num_workers - 1 else file_info['total'])
#                 worker_chunks.append(list(range(start_chunk, end_chunk)))

#             # Start workers
#             result_queue = mp.Queue()
#             workers = []
#             start_time = time.time()

#             is_windows = platform.system() == 'Windows'
#             worker_func = client_worker_windows if is_windows else client_worker_unix

#             logger.info("=" * 70)
#             logger.info(f"CLIENT STARTING")
#             logger.info("=" * 70)
#             logger.info(f"Platform: {platform.system()}")
#             logger.info(f"Workers: {self.num_workers}")
#             logger.info(f"Window size: {self.window_size}")
#             logger.info(f"Server: {server_host}")
#             logger.info(f"Worker ports: {base_port + 1} to {base_port + 1 + self.num_workers - 1}")
#             logger.info("=" * 70)
#             logger.info(f"Starting {self.num_workers} workers...")

#             for i in range(self.num_workers):
#                 p = mp.Process(
#                     target=worker_func,
#                     args=(i, server_host, base_port + 1, worker_chunks[i], str(output_path),
#                           file_info['size'], file_info['chunk_size'], self.window_size, result_queue)
#                 )
#                 p.start()
#                 workers.append(p)

#             # Wait for completion
#             completed = 0
#             total_received = 0

#             while completed < self.num_workers:
#                 try:
#                     msg = result_queue.get(timeout=2)
#                     if msg[0] == 'worker_done':
#                         completed += 1
#                         total_received += msg[2]
#                         logger.info(f"Worker {msg[1]} done: {msg[2]} chunks")
#                 except:
#                     if not any(p.is_alive() for p in workers):
#                         break

#             for p in workers:
#                 p.join(timeout=2)

#             elapsed = time.time() - start_time
#             speed = (file_info['size'] / elapsed) / (1024 * 1024) if elapsed > 0 else 0

#             logger.info("=" * 70)
#             logger.info(f"TRANSFER COMPLETE")
#             logger.info(f"Time: {elapsed:.2f}s")
#             logger.info(f"Speed: {speed:.1f} MB/s")
#             logger.info(f"Received: {total_received}/{file_info['total']} chunks")
#             logger.info(f"Saved: {output_path}")

#             # Verify
#             actual_size = output_path.stat().st_size
#             if actual_size == file_info['size']:
#                 logger.info("Size: VERIFIED")

#                 hasher = xxhash.xxh64()
#                 with open(output_path, 'rb') as f:
#                     while chunk := f.read(file_info['chunk_size'] * 4):
#                         hasher.update(chunk)

#                 if hasher.hexdigest() == file_info['hash']:
#                     logger.info("Hash: VERIFIED - Transfer successful!")
#                 else:
#                     logger.warning("Hash: MISMATCH - File may be corrupted")
#             else:
#                 logger.warning(f"Size: MISMATCH ({actual_size} vs {file_info['size']})")

#             logger.info("=" * 70)

#             return total_received == file_info['total']

#         except Exception as e:
#             logger.error(f"Error: {e}")
#             import traceback
#             traceback.print_exc()
#             return False


# def run_server(filepath: str, host: str = "0.0.0.0", port: int = 9000,
#                workers: int = None, chunk_size: int = None, window: int = None):
#     """Start server with optional config"""
#     num_workers = workers or DEFAULT_WORKERS
#     chunk_sz = chunk_size or DEFAULT_CHUNK_SIZE
#     window_sz = window or DEFAULT_WINDOW_SIZE

#     server = ParallelServer(host, port, num_workers, chunk_sz, window_sz)
#     server.serve_file(Path(filepath))


# def run_client(server_host: str, port: int = 9000, workers: int = None, window: int = None):
#     """Start client with optional config"""
#     num_workers = workers or DEFAULT_WORKERS
#     window_sz = window or DEFAULT_WINDOW_SIZE

#     client = ParallelClient(num_workers, window_sz)
#     client.receive_file(server_host, port)


# if __name__ == "__main__":
#     import sys

#     mp.set_start_method('spawn', force=True)

#     if not ENET_AVAILABLE:
#         print("ERROR: pip install pyenet-updated numba numpy xxhash psutil")
#         sys.exit(1)

#     print("\n" + "=" * 70)
#     print("ULTRA-FAST UDP FILE TRANSFER")
#     print("=" * 70)
#     print("Strategy: UNSEQUENCED packets + Application-level retransmission")
#     print("Protocol: Receiver-driven flow control (like RBUDP/FASP)")
#     print()
#     print("CRITICAL: FIREWALL MUST ALLOW UDP PORTS")
#     print("=" * 70)

#     if platform.system() == 'Windows':
#         print("Windows Firewall:")
#         print("  1. Run as Administrator OR")
#         print("  2. Add firewall rule:")
#         print('     netsh advfirewall firewall add rule name="UDP File Transfer"')
#         print('     dir=in action=allow protocol=UDP localport=9000-9020')
#         print()
#         print("  3. Check if ports are open:")
#         print("     netstat -an | findstr :9000")
#     else:
#         print("Linux Firewall:")
#         print("  sudo ufw allow 9000:9020/udp")
#         print("  OR: sudo iptables -A INPUT -p udp --dport 9000:9020 -j ACCEPT")
#         print()
#         print("  Check: sudo netstat -uln | grep 9000")

#     print()
#     print("TROUBLESHOOTING CONNECTION FAILURES:")
#     print("  1. Check server is running FIRST")
#     print("  2. Wait 5 seconds after server starts")
#     print("  3. Verify ports with netstat command above")
#     print("  4. Temporarily disable firewall to test")
#     print("  5. Check antivirus isn't blocking UDP")
#     print("=" * 70)

#     if len(sys.argv) < 2:
#         print("\nUSAGE:")
#         print("  Server: python script.py server <file> [host] [port] [workers] [chunk_mb] [window]")
#         print("  Client: python script.py client <host> [port] [workers] [window]")
#         print()
#         print("DEFAULTS:")
#         print(f"  Workers: {DEFAULT_WORKERS}")
#         print(f"  Chunk size: {DEFAULT_CHUNK_SIZE / (1024**2):.0f} MB")
#         print(f"  Window: {DEFAULT_WINDOW_SIZE} chunks per worker")
#         print()
#         print("EXAMPLES:")
#         print("  Server: python script.py server bigfile.bin 0.0.0.0 9000 8 1 64")
#         print("  Client: python script.py client 192.168.1.100 9000 8 64")
#         print()
#         print("TUNING:")
#         print("  - More workers = higher throughput (but more CPU/memory)")
#         print("  - Larger chunks = fewer syscalls (1-4 MB recommended)")
#         print("  - Larger window = more in-flight data (32-128 recommended)")
#         print("  - For LAN: 8 workers, 1MB chunks, 64 window")
#         print("  - For WAN: 16 workers, 2MB chunks, 128 window")
#         sys.exit(1)

#     mode = sys.argv[1]

#     if mode == "server":
#         if len(sys.argv) < 3:
#             print("Error: Specify file to serve")
#             sys.exit(1)

#         filepath = sys.argv[2]
#         host = sys.argv[3] if len(sys.argv) > 3 else "0.0.0.0"
#         port = int(sys.argv[4]) if len(sys.argv) > 4 else 9000
#         workers = int(sys.argv[5]) if len(sys.argv) > 5 else None
#         chunk_mb = int(sys.argv[6]) if len(sys.argv) > 6 else None
#         window = int(sys.argv[7]) if len(sys.argv) > 7 else None

#         chunk_size = chunk_mb * 1024 * 1024 if chunk_mb else None

#         run_server(filepath, host, port, workers, chunk_size, window)

#     elif mode == "client":
#         if len(sys.argv) < 3:
#             print("Error: Specify server host")
#             sys.exit(1)

#         server_host = sys.argv[2]
#         port = int(sys.argv[3]) if len(sys.argv) > 3 else 9000
#         workers = int(sys.argv[4]) if len(sys.argv) > 4 else None
#         window = int(sys.argv[5]) if len(sys.argv) > 5 else None

#         run_client(server_host, port, workers, window)

#     else:
#         print(f"Unknown mode: {mode}")
#         sys.exit(1)


# """
# Ultra-fast UDP file transfer with proper flow control
# Strategy: UNSEQUENCED for speed + application-level selective retransmission
# Research: Aspera FASP, RBUDP, UDT protocols

# CRITICAL FIXES from analysis:
# 1. UNSEQUENCED for maximum speed (no head-of-line blocking)
# 2. Application-level bitmap ACK (like RBUDP)
# 3. Receiver-driven flow control (prevents server flooding)
# 4. Separate control channel for ACKs
# 5. Rate limiting based on RTT feedback

# Windows: pip install pyenet-updated numba numpy xxhash psutil
# """

# import ctypes
# import logging
# import mmap
# import multiprocessing as mp
# import os
# import platform
# import socket
# import struct
# import sys
# import time
# from collections import deque
# from pathlib import Path
# from typing import Set, Tuple, Dict

# import numpy as np
# import xxhash
# from numba import jit

# try:
#     import enet
#     ENET_AVAILABLE = True
# except ImportError:
#     ENET_AVAILABLE = False

# try:
#     import psutil
#     PSUTIL_AVAILABLE = True
# except ImportError:
#     PSUTIL_AVAILABLE = False

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
# logger = logging.getLogger(__name__)

# # Optimized for speed + stability
# DEFAULT_WORKERS = 8
# DEFAULT_CHUNK_SIZE = 1048576  # 1MB
# DEFAULT_WINDOW_SIZE = 64  # Chunks in flight per worker

# TRANSFER_DIR = Path("./transfers")
# TRANSFER_DIR.mkdir(exist_ok=True)

# # Windows API for concurrent file access
# if platform.system() == 'Windows':
#     import msvcrt

#     GENERIC_READ = 0x80000000
#     GENERIC_WRITE = 0x40000000
#     FILE_SHARE_READ = 0x00000001
#     FILE_SHARE_WRITE = 0x00000002
#     OPEN_EXISTING = 3
#     FILE_ATTRIBUTE_NORMAL = 0x80
#     FILE_FLAG_RANDOM_ACCESS = 0x10000000
#     FILE_FLAG_WRITE_THROUGH = 0x80000000

#     kernel32 = ctypes.windll.kernel32


# @jit(nopython=True, cache=True, fastmath=True, inline='always')
# def fast_hash(data: np.ndarray) -> np.uint32:
#     """Optimized hash"""
#     h = np.uint32(2166136261)
#     prime = np.uint32(16777619)

#     for i in range(0, len(data), 16):
#         h = np.uint32((h ^ data[i]) * prime)

#     return h


# @jit(nopython=True, cache=True, fastmath=True)
# def verify_hash(data: np.ndarray, expected: np.uint32) -> bool:
#     """Fast verify"""
#     return fast_hash(data) == expected


# class PacketType:
#     CHUNK_DATA = 1
#     CHUNK_REQUEST = 2
#     BITMAP_ACK = 3  # Receiver sends bitmap of received chunks
#     WORKER_READY = 4
#     FILE_METADATA = 5
#     RATE_FEEDBACK = 6  # Client sends rate info to server


# def open_shared_file_windows(filepath: str, file_size: int):
#     """Open file with Windows API for concurrent access"""
#     if platform.system() != 'Windows':
#         raise OSError("Windows-only function")

#     import msvcrt

#     try:
#         filepath_wide = ctypes.create_unicode_buffer(filepath)

#         handle = kernel32.CreateFileW(
#             filepath_wide,
#             GENERIC_READ | GENERIC_WRITE,
#             FILE_SHARE_READ | FILE_SHARE_WRITE,
#             None,
#             OPEN_EXISTING,
#             FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_WRITE_THROUGH,
#             None
#         )

#         if handle == -1 or handle == 0:
#             error_code = kernel32.GetLastError()
#             raise OSError(f"CreateFile failed: {error_code}")

#         fd = msvcrt.open_osfhandle(handle, os.O_RDWR | os.O_BINARY)
#         return fd, handle

#     except Exception as e:
#         logger.error(f"Failed to open file: {e}")
#         raise


# def write_at_offset_windows(handle, offset: int, data: bytes):
#     """Direct write at offset using Windows API"""
#     if platform.system() != 'Windows':
#         raise OSError("Windows-only function")

#     offset_low = ctypes.c_ulong(offset & 0xFFFFFFFF)
#     offset_high = ctypes.c_long(offset >> 32)

#     result = kernel32.SetFilePointer(handle, offset_low, ctypes.byref(offset_high), 0)

#     if result == 0xFFFFFFFF:
#         error = kernel32.GetLastError()
#         if error != 0:
#             raise OSError(f"SetFilePointer failed: {error}")

#     bytes_written = ctypes.c_ulong()
#     success = kernel32.WriteFile(handle, data, len(data), ctypes.byref(bytes_written), None)

#     if not success:
#         raise OSError(f"WriteFile failed: {kernel32.GetLastError()}")

#     return bytes_written.value


# def server_worker(worker_id: int, port: int, filepath: str, chunk_size: int, window_size: int):
#     """
#     Server worker with UNSEQUENCED + receiver-driven flow control.

#     Strategy:
#     - Send chunks as fast as client requests (UNSEQUENCED for speed)
#     - Client controls rate via BITMAP_ACK feedback
#     - Stay alive until client explicitly disconnects
#     """
#     try:
#         logger.info(f"Worker {worker_id}: Starting initialization...")

#         file_path = Path(filepath)
#         if not file_path.exists():
#             logger.error(f"Worker {worker_id}: File not found: {filepath}")
#             return

#         logger.info(f"Worker {worker_id}: Opening file {file_path.name}")
#         f = open(file_path, 'rb')
#         file_size = file_path.stat().st_size
#         m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

#         logger.info(f"Worker {worker_id}: File opened, size: {file_size / (1024**2):.1f} MB")

#         # ENet host with 2 channels: data (0) and control (1)
#         logger.info(f"Worker {worker_id}: Binding to 0.0.0.0:{port + worker_id}")
#         address = enet.Address(b'0.0.0.0', port + worker_id)

#         try:
#             host = enet.Host(
#                 address,
#                 1,  # One peer
#                 2,  # Two channels
#                 0,  # Unlimited incoming
#                 0   # Unlimited outgoing (let network handle it)
#             )
#             logger.info(f"Worker {worker_id}: Successfully bound to port {port + worker_id}")
#         except Exception as bind_error:
#             logger.error(f"Worker {worker_id}: FAILED to bind port {port + worker_id}")
#             logger.error(f"Worker {worker_id}: Error: {bind_error}")
#             logger.error(f"Worker {worker_id}: Port may be in use. Check with: netstat -an | findstr :{port + worker_id}")
#             m.close()
#             f.close()
#             return

#         logger.info(f"Worker {worker_id}: READY and listening on 0.0.0.0:{port + worker_id}")

#         peer = None
#         chunks_sent = 0
#         last_stats = time.time()
#         bytes_sent_interval = 0

#         # Track client's received status (updated via bitmap ACKs)
#         client_received = set()
#         last_request_time = time.time()

#         while True:
#             # CRITICAL: Stay responsive but don't timeout too fast
#             event = host.service(100)  # 100ms timeout for better responsiveness

#             if event.type == enet.EVENT_TYPE_CONNECT:
#                 peer = event.peer
#                 last_request_time = time.time()
#                 logger.info(f"Worker {worker_id}: Client connected")

#             elif event.type == enet.EVENT_TYPE_RECEIVE:
#                 if not peer:
#                     continue

#                 last_request_time = time.time()  # Reset timeout on any activity
#                 packet_type = event.packet.data[0]

#                 if packet_type == PacketType.CHUNK_REQUEST:
#                     # Client requests specific chunk
#                     chunk_index, = struct.unpack('!I', event.packet.data[1:5])

#                     offset = chunk_index * chunk_size
#                     if offset >= file_size:
#                         logger.warning(f"Worker {worker_id}: Invalid chunk {chunk_index}")
#                         continue

#                     end = min(offset + chunk_size, file_size)
#                     chunk_actual_size = end - offset

#                     # Zero-copy read
#                     chunk_data = m[offset:end]

#                     # Fast hash
#                     data_array = np.frombuffer(chunk_data, dtype=np.uint8)
#                     checksum = fast_hash(data_array)

#                     # Send UNSEQUENCED for maximum speed
#                     packet_data = struct.pack('!BIII', PacketType.CHUNK_DATA, chunk_index,
#                                             chunk_actual_size, checksum) + chunk_data
#                     packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#                     peer.send(0, packet)  # Data channel

#                     chunks_sent += 1
#                     bytes_sent_interval += chunk_actual_size

#                     # Flush periodically to prevent buffer buildup
#                     if chunks_sent % 10 == 0:
#                         host.flush()

#                 elif packet_type == PacketType.BITMAP_ACK:
#                     # Client sends bitmap of received chunks
#                     num_received, = struct.unpack('!I', event.packet.data[1:5])
#                     if num_received > 0 and len(event.packet.data) >= 5 + num_received * 4:
#                         received_chunks = struct.unpack(f'!{num_received}I',
#                                                        event.packet.data[5:5 + num_received * 4])
#                         client_received.update(received_chunks)
#                         logger.debug(f"Worker {worker_id}: Client has {len(client_received)} chunks")

#                 # Stats
#                 current_time = time.time()
#                 if current_time - last_stats >= 2.0:
#                     elapsed = current_time - last_stats
#                     if elapsed > 0:
#                         speed_mbps = (bytes_sent_interval / elapsed) / (1024 * 1024)
#                         logger.info(f"Worker {worker_id}: {chunks_sent} sent, {speed_mbps:.1f} MB/s, "
#                                   f"client has {len(client_received)} chunks")
#                         bytes_sent_interval = 0
#                         last_stats = current_time

#             elif event.type == enet.EVENT_TYPE_DISCONNECT:
#                 logger.info(f"Worker {worker_id} client disconnected: {chunks_sent} chunks sent")
#                 break

#             # Check for client timeout (no activity for 60 seconds)
#             if peer and (time.time() - last_request_time) > 60:
#                 logger.warning(f"Worker {worker_id}: Client timeout, closing")
#                 break

#         m.close()
#         f.close()

#     except Exception as e:
#         logger.error(f"Worker {worker_id} error: {e}")
#         import traceback
#         traceback.print_exc()


# def client_worker_windows(worker_id: int, server_host: str, port: int, chunk_indices: list,
#                           output_path: str, file_size: int, chunk_size: int, window_size: int,
#                           result_queue: mp.Queue):
#     """
#     Client worker with UNSEQUENCED + application-level retransmission.

#     Strategy (like RBUDP):
#     1. Request chunks in batches (window-based)
#     2. Receive UNSEQUENCED for low latency
#     3. Track received chunks in bitmap
#     4. Periodically send bitmap ACK to server
#     5. Retransmit missing chunks at end
#     """
#     if platform.system() != 'Windows':
#         logger.error(f"Worker {worker_id}: Windows required")
#         return

#     import msvcrt

#     try:
#         fd, handle = open_shared_file_windows(output_path, file_size)
#         logger.info(f"Worker {worker_id}: File opened")

#         # Connect
#         client_host = enet.Host(None, 1, 2, 0, 0)
#         address = enet.Address(server_host.encode('utf-8'), port + worker_id)
#         peer = client_host.connect(address, 2)

#         # Wait for connection with retry
#         logger.info(f"Worker {worker_id}: Connecting to port {port + worker_id}...")

#         connected = False
#         for attempt in range(3):
#             event = client_host.service(2000)  # 2 second timeout per attempt
#             if event.type == enet.EVENT_TYPE_CONNECT:
#                 connected = True
#                 break

#             if attempt < 2:
#                 logger.warning(f"Worker {worker_id}: Connection attempt {attempt + 1} failed, retrying...")
#                 time.sleep(1)

#         if not connected:
#             logger.error(f"Worker {worker_id}: Connection failed after 3 attempts to port {port + worker_id}")
#             kernel32.CloseHandle(handle)
#             result_queue.put(('worker_done', worker_id, 0))
#             return

#         logger.info(f"Worker {worker_id}: Connected to port {port + worker_id}")

#         # State
#         received = set()
#         requested = set()

#         # Request initial window - use smaller window to start
#         initial_window = min(window_size // 2, 32, len(chunk_indices))
#         logger.info(f"Worker {worker_id}: Requesting initial window of {initial_window} chunks")

#         for i in range(initial_window):
#             chunk_idx = chunk_indices[i]
#             packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#             packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#             peer.send(0, packet)
#             requested.add(chunk_idx)

#         # Flush to ensure requests sent
#         client_host.flush()

#         next_to_request = initial_window
#         last_progress = time.time()
#         last_ack_sent = time.time()

#         # Diagnostic counters
#         packets_received = 0
#         hash_failures = 0
#         write_failures = 0
#         duplicates = 0

#         # Main receive loop
#         while len(received) < len(chunk_indices):
#             # Frequent service for fast packet processing
#             event = client_host.service(5)

#             if event.type == enet.EVENT_TYPE_RECEIVE:
#                 packets_received += 1
#                 packet_type = event.packet.data[0]

#                 if packet_type == PacketType.CHUNK_DATA:
#                     if len(event.packet.data) < 13:
#                         continue

#                     chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
#                                                                             event.packet.data[1:13])
#                     chunk_data = event.packet.data[13:13 + received_chunk_size]

#                     if chunk_idx not in chunk_indices:
#                         continue

#                     if chunk_idx in received:
#                         duplicates += 1
#                         continue

#                     if len(chunk_data) != received_chunk_size:
#                         logger.warning(f"Worker {worker_id}: Chunk {chunk_idx} size mismatch")
#                         continue

#                     # Verify hash
#                     data_array = np.frombuffer(chunk_data, dtype=np.uint8)
#                     if not verify_hash(data_array, np.uint32(checksum)):
#                         hash_failures += 1
#                         logger.warning(f"Worker {worker_id}: Chunk {chunk_idx} hash failed")
#                         continue

#                     # Write to disk
#                     try:
#                         offset = chunk_idx * chunk_size
#                         bytes_written = write_at_offset_windows(handle, offset, chunk_data)

#                         if bytes_written != received_chunk_size:
#                             write_failures += 1
#                             logger.error(f"Worker {worker_id}: Write failed for {chunk_idx}")
#                             continue
#                     except Exception as e:
#                         write_failures += 1
#                         logger.error(f"Worker {worker_id}: Write error {chunk_idx}: {e}")
#                         continue

#                     # Success
#                     received.add(chunk_idx)

#                     # Request more chunks to maintain window
#                     while next_to_request < len(chunk_indices) and len(requested - received) < window_size:
#                         new_chunk_idx = chunk_indices[next_to_request]
#                         if new_chunk_idx not in requested:
#                             packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, new_chunk_idx)
#                             packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#                             peer.send(0, packet)
#                             requested.add(new_chunk_idx)
#                         next_to_request += 1

#                     # Progress with diagnostics
#                     current_time = time.time()
#                     if current_time - last_progress >= 2.0:
#                         progress = len(received) / len(chunk_indices) * 100
#                         in_flight = len(requested - received)
#                         logger.info(f"Worker {worker_id}: {len(received)}/{len(chunk_indices)} "
#                                   f"({progress:.1f}%), in-flight: {in_flight}, "
#                                   f"pkts: {packets_received}, hash_fail: {hash_failures}, "
#                                   f"write_fail: {write_failures}, dup: {duplicates}")
#                         last_progress = current_time

#             elif event.type == enet.EVENT_TYPE_DISCONNECT:
#                 logger.warning(f"Worker {worker_id}: Server disconnected unexpectedly")
#                 logger.warning(f"Worker {worker_id}: Diagnostics - received: {len(received)}, "
#                              f"requested: {len(requested)}, missing: {len(requested - received)}")
#                 break

#             # Send periodic bitmap ACK (like RBUDP)
#             current_time = time.time()
#             if current_time - last_ack_sent >= 1.0 and len(received) > 0:
#                 received_list = sorted(list(received))
#                 if received_list:
#                     # Send up to 1000 chunk IDs at a time
#                     batch_size = min(1000, len(received_list))
#                     ack_data = struct.pack(f'!BI{batch_size}I',
#                                           PacketType.BITMAP_ACK,
#                                           batch_size,
#                                           *received_list[:batch_size])
#                     packet = enet.Packet(ack_data, enet.PACKET_FLAG_RELIABLE)
#                     peer.send(1, packet)  # Control channel
#                     last_ack_sent = current_time

#             # Check for stalled transfer - re-request missing chunks
#             if current_time - last_progress > 5.0:
#                 in_flight_chunks = requested - received
#                 if len(in_flight_chunks) > 0:
#                     # Re-request first 10 in-flight chunks
#                     retry_chunks = sorted(list(in_flight_chunks))[:10]
#                     logger.info(f"Worker {worker_id}: Stalled, retrying {len(retry_chunks)} chunks")

#                     for chunk_idx in retry_chunks:
#                         packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#                         packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#                         peer.send(0, packet)

#                     client_host.flush()
#                     last_progress = current_time

#         # Retransmission phase for missing chunks
#         missing = set(chunk_indices) - received
#         if missing:
#             logger.info(f"Worker {worker_id}: Retransmitting {len(missing)} missing chunks")

#             # Send all missing chunk requests with RELIABLE
#             for chunk_idx in sorted(missing):
#                 packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#                 packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
#                 peer.send(0, packet)

#             # Flush to ensure all requests sent
#             client_host.flush()

#             # Wait for retransmitted chunks with longer timeout
#             retrans_start = time.time()
#             retrans_received = 0

#             while len(received) < len(chunk_indices) and (time.time() - retrans_start) < 60:
#                 event = client_host.service(100)  # Longer service time for retransmit

#                 if event.type == enet.EVENT_TYPE_RECEIVE:
#                     packet_type = event.packet.data[0]

#                     if packet_type == PacketType.CHUNK_DATA:
#                         if len(event.packet.data) < 13:
#                             continue

#                         chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
#                                                                                 event.packet.data[1:13])
#                         chunk_data = event.packet.data[13:13 + received_chunk_size]

#                         if chunk_idx in received or chunk_idx not in chunk_indices:
#                             continue

#                         if len(chunk_data) != received_chunk_size:
#                             logger.warning(f"Worker {worker_id}: Retransmit chunk {chunk_idx} size mismatch")
#                             continue

#                         # Verify hash
#                         data_array = np.frombuffer(chunk_data, dtype=np.uint8)
#                         if not verify_hash(data_array, np.uint32(checksum)):
#                             logger.warning(f"Worker {worker_id}: Retransmit chunk {chunk_idx} hash failed")
#                             continue

#                         # Write to disk
#                         try:
#                             offset = chunk_idx * chunk_size
#                             bytes_written = write_at_offset_windows(handle, offset, chunk_data)

#                             if bytes_written == received_chunk_size:
#                                 received.add(chunk_idx)
#                                 retrans_received += 1

#                                 if retrans_received % 10 == 0:
#                                     logger.info(f"Worker {worker_id}: Retransmit progress {retrans_received}/{len(missing)}")
#                         except Exception as e:
#                             logger.error(f"Worker {worker_id}: Retransmit write error {chunk_idx}: {e}")
#                             continue

#                 elif event.type == enet.EVENT_TYPE_DISCONNECT:
#                     logger.warning(f"Worker {worker_id}: Server disconnected during retransmit")
#                     break

#             if len(received) < len(chunk_indices):
#                 still_missing = len(chunk_indices) - len(received)
#                 logger.warning(f"Worker {worker_id}: Still missing {still_missing} chunks after retransmit")

#         # Final flush and close
#         kernel32.FlushFileBuffers(handle)
#         kernel32.CloseHandle(handle)

#         # Graceful disconnect
#         if peer:
#             peer.disconnect_later()

#             # Wait for disconnect event
#             timeout = time.time() + 2
#             while time.time() < timeout:
#                 event = client_host.service(100)
#                 if event.type == enet.EVENT_TYPE_DISCONNECT:
#                     break

#         result_queue.put(('worker_done', worker_id, len(received)))
#         logger.info(f"Worker {worker_id} complete: {len(received)}/{len(chunk_indices)}")

#     except Exception as e:
#         logger.error(f"Worker {worker_id} error: {e}")
#         import traceback
#         traceback.print_exc()
#         result_queue.put(('worker_done', worker_id, 0))


# def client_worker_unix(worker_id: int, server_host: str, port: int, chunk_indices: list,
#                        output_path: str, file_size: int, chunk_size: int, window_size: int,
#                        result_queue: mp.Queue):
#     """Unix/Linux client worker using pwrite"""
#     try:
#         fd = os.open(output_path, os.O_RDWR)
#         logger.info(f"Worker {worker_id}: File opened (fd: {fd})")

#         client_host = enet.Host(None, 1, 2, 0, 0)
#         address = enet.Address(server_host.encode('utf-8'), port + worker_id)
#         peer = client_host.connect(address, 2)

#         event = client_host.service(5000)
#         if event.type != enet.EVENT_TYPE_CONNECT:
#             logger.error(f"Worker {worker_id}: Connection failed")
#             os.close(fd)
#             return

#         logger.info(f"Worker {worker_id}: Connected")

#         received = set()
#         requested = set()

#         # Request initial window - smaller to start
#         initial_window = min(window_size // 2, 32, len(chunk_indices))
#         logger.info(f"Worker {worker_id}: Requesting initial {initial_window} chunks")

#         for i in range(initial_window):
#             chunk_idx = chunk_indices[i]
#             packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#             packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#             peer.send(0, packet)
#             requested.add(chunk_idx)

#         client_host.flush()

#         next_to_request = initial_window
#         last_progress = time.time()
#         last_ack_sent = time.time()

#         # Diagnostics
#         packets_received = 0
#         hash_failures = 0

#         while len(received) < len(chunk_indices):
#             event = client_host.service(5)

#             if event.type == enet.EVENT_TYPE_RECEIVE:
#                 packets_received += 1
#                 packet_type = event.packet.data[0]

#                 if packet_type == PacketType.CHUNK_DATA:
#                     if len(event.packet.data) < 13:
#                         continue

#                     chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
#                                                                             event.packet.data[1:13])
#                     chunk_data = event.packet.data[13:13 + received_chunk_size]

#                     if chunk_idx not in chunk_indices or chunk_idx in received:
#                         continue

#                     data_array = np.frombuffer(chunk_data, dtype=np.uint8)
#                     if not verify_hash(data_array, np.uint32(checksum)):
#                         hash_failures += 1
#                         continue

#                     try:
#                         offset = chunk_idx * chunk_size
#                         bytes_written = os.pwrite(fd, chunk_data, offset)

#                         if bytes_written == received_chunk_size:
#                             received.add(chunk_idx)
#                     except Exception as e:
#                         logger.error(f"Worker {worker_id}: Write error: {e}")
#                         continue

#                     # Maintain window
#                     while next_to_request < len(chunk_indices) and len(requested - received) < window_size:
#                         new_chunk_idx = chunk_indices[next_to_request]
#                         if new_chunk_idx not in requested:
#                             packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, new_chunk_idx)
#                             packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#                             peer.send(0, packet)
#                             requested.add(new_chunk_idx)
#                         next_to_request += 1

#                     # Progress
#                     current_time = time.time()
#                     if current_time - last_progress >= 2.0:
#                         progress = len(received) / len(chunk_indices) * 100
#                         logger.info(f"Worker {worker_id}: {len(received)}/{len(chunk_indices)} ({progress:.1f}%), "
#                                   f"pkts: {packets_received}, hash_fail: {hash_failures}")
#                         last_progress = current_time

#             elif event.type == enet.EVENT_TYPE_DISCONNECT:
#                 logger.warning(f"Worker {worker_id}: Server disconnected")
#                 break

#             # Periodic ACK
#             current_time = time.time()
#             if current_time - last_ack_sent >= 1.0 and len(received) > 0:
#                 received_list = sorted(list(received))[:1000]
#                 if received_list:
#                     ack_data = struct.pack(f'!BI{len(received_list)}I',
#                                           PacketType.BITMAP_ACK,
#                                           len(received_list),
#                                           *received_list)
#                     packet = enet.Packet(ack_data, enet.PACKET_FLAG_RELIABLE)
#                     peer.send(1, packet)
#                     last_ack_sent = current_time

#             # Stall detection and retry
#             if current_time - last_progress > 5.0:
#                 in_flight = requested - received
#                 if len(in_flight) > 0:
#                     retry = sorted(list(in_flight))[:10]
#                     logger.info(f"Worker {worker_id}: Stalled, retrying {len(retry)} chunks")
#                     for chunk_idx in retry:
#                         packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#                         packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
#                         peer.send(0, packet)
#                     client_host.flush()
#                     last_progress = current_time

#         # Retransmit missing
#         missing = set(chunk_indices) - received
#         if missing:
#             logger.info(f"Worker {worker_id}: Retransmitting {len(missing)} chunks")

#             for chunk_idx in sorted(missing):
#                 packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
#                 packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
#                 peer.send(0, packet)

#             client_host.flush()

#             retrans_start = time.time()
#             retrans_received = 0

#             while len(received) < len(chunk_indices) and (time.time() - retrans_start) < 60:
#                 event = client_host.service(100)

#                 if event.type == enet.EVENT_TYPE_RECEIVE:
#                     packet_type = event.packet.data[0]

#                     if packet_type == PacketType.CHUNK_DATA:
#                         if len(event.packet.data) < 13:
#                             continue

#                         chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
#                                                                                 event.packet.data[1:13])
#                         chunk_data = event.packet.data[13:13 + received_chunk_size]

#                         if chunk_idx in received or chunk_idx not in chunk_indices:
#                             continue

#                         data_array = np.frombuffer(chunk_data, dtype=np.uint8)
#                         if not verify_hash(data_array, np.uint32(checksum)):
#                             continue

#                         try:
#                             offset = chunk_idx * chunk_size
#                             bytes_written = os.pwrite(fd, chunk_data, offset)

#                             if bytes_written == received_chunk_size:
#                                 received.add(chunk_idx)
#                                 retrans_received += 1

#                                 if retrans_received % 10 == 0:
#                                     logger.info(f"Worker {worker_id}: Retransmit {retrans_received}/{len(missing)}")
#                         except Exception as e:
#                             logger.error(f"Worker {worker_id}: Retransmit write error: {e}")
#                             continue

#                 elif event.type == enet.EVENT_TYPE_DISCONNECT:
#                     logger.warning(f"Worker {worker_id}: Server disconnected during retransmit")
#                     break

#             if len(received) < len(chunk_indices):
#                 logger.warning(f"Worker {worker_id}: Still missing {len(chunk_indices) - len(received)} chunks")

#         os.fsync(fd)
#         os.close(fd)

#         if peer:
#             peer.disconnect_later()
#             timeout = time.time() + 2
#             while time.time() < timeout:
#                 event = client_host.service(100)
#                 if event.type == enet.EVENT_TYPE_DISCONNECT:
#                     break

#         result_queue.put(('worker_done', worker_id, len(received)))
#         logger.info(f"Worker {worker_id} complete: {len(received)}/{len(chunk_indices)}")

#     except Exception as e:
#         logger.error(f"Worker {worker_id} error: {e}")
#         import traceback
#         traceback.print_exc()
#         result_queue.put(('worker_done', worker_id, 0))


# def metadata_server_process(base_port: int, filepath: str, file_size: int, total_chunks: int, chunk_size: int):
#     """
#     Standalone metadata server process (must be top-level function for multiprocessing).
#     Sends file metadata to clients.
#     """
#     try:
#         # Convert string back to Path inside the process
#         file_path = Path(filepath)

#         address = enet.Address(b'0.0.0.0', base_port)
#         host = enet.Host(address, 8, 1, 0, 0)

#         logger.info(f"Metadata server listening on port {base_port}")

#         while True:
#             event = host.service(100)

#             if event.type == enet.EVENT_TYPE_CONNECT:
#                 logger.info("Client connected to metadata server")

#             elif event.type == enet.EVENT_TYPE_RECEIVE:
#                 if event.packet.data[0] == PacketType.WORKER_READY:
#                     filename_bytes = file_path.name.encode('utf-8')

#                     # Calculate file hash
#                     hasher = xxhash.xxh64()
#                     with open(file_path, 'rb') as f:
#                         while chunk := f.read(chunk_size * 4):
#                             hasher.update(chunk)
#                     file_hash = hasher.hexdigest()

#                     # Send metadata packet
#                     packet_data = struct.pack(
#                         '!BIQQI',
#                         PacketType.FILE_METADATA,
#                         len(filename_bytes),
#                         file_size,
#                         total_chunks,
#                         chunk_size
#                     ) + filename_bytes + file_hash.encode('utf-8')

#                     packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
#                     event.peer.send(0, packet)

#                     logger.info(f"Sent metadata: {file_path.name}, {total_chunks} chunks, {chunk_size} bytes/chunk")

#     except Exception as e:
#         logger.error(f"Metadata server error: {e}")
#         import traceback
#         traceback.print_exc()


# class ParallelServer:
#     """Server with multiple workers"""

#     def __init__(self, host: str, base_port: int, num_workers: int = DEFAULT_WORKERS,
#                  chunk_size: int = DEFAULT_CHUNK_SIZE, window_size: int = DEFAULT_WINDOW_SIZE):
#         self.host = host
#         self.base_port = base_port
#         self.num_workers = num_workers
#         self.chunk_size = chunk_size
#         self.window_size = window_size
#         self.workers = []

#     def serve_file(self, filepath: Path):
#         """Start parallel workers"""
#         if not filepath.exists():
#             logger.error(f"File not found: {filepath}")
#             return

#         file_size = filepath.stat().st_size
#         total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size

#         logger.info("=" * 70)
#         logger.info(f"SERVER STARTING")
#         logger.info("=" * 70)
#         logger.info(f"File: {filepath.name}, {file_size / (1024**2):.1f} MB, {total_chunks} chunks")
#         logger.info(f"Workers: {self.num_workers}, Chunk size: {self.chunk_size / (1024**2):.1f} MB")
#         logger.info(f"Window: {self.window_size} chunks/worker")
#         logger.info(f"Metadata port: {self.base_port}")
#         logger.info(f"Worker ports: {self.base_port + 1} to {self.base_port + self.num_workers}")
#         logger.info("=" * 70)

#         # Start workers FIRST (they need to be listening before client connects)
#         logger.info(f"Starting {self.num_workers} worker processes...")
#         for i in range(self.num_workers):
#             p = mp.Process(
#                 target=server_worker,
#                 args=(i, self.base_port + 1, str(filepath), self.chunk_size, self.window_size)
#             )
#             p.start()
#             self.workers.append(p)
#             logger.info(f"Started worker {i} (PID: {p.pid}) on port {self.base_port + 1 + i}")

#         # Give workers time to bind to ports
#         logger.info("Waiting for workers to initialize and bind ports...")
#         time.sleep(3)  # Increased from 2 to 3 seconds

#         # Check if workers are still alive
#         alive_workers = sum(1 for p in self.workers if p.is_alive())
#         logger.info(f"Workers alive: {alive_workers}/{self.num_workers}")

#         if alive_workers < self.num_workers:
#             logger.error(f"WARNING: Only {alive_workers}/{self.num_workers} workers are running!")
#             logger.error("Check firewall settings or port availability")

#         # Start metadata server LAST (use standalone function, not method)
#         logger.info(f"Starting metadata server on port {self.base_port}...")
#         metadata_proc = mp.Process(
#             target=metadata_server_process,
#             args=(self.base_port, str(filepath), file_size, total_chunks, self.chunk_size)
#         )
#         metadata_proc.start()
#         logger.info(f"Metadata server started (PID: {metadata_proc.pid})")

#         time.sleep(1)

#         # Final check
#         if metadata_proc.is_alive():
#             logger.info("Metadata server: RUNNING")
#         else:
#             logger.error("Metadata server: FAILED TO START")

#         logger.info("=" * 70)
#         logger.info("SERVER READY - Waiting for clients...")
#         logger.info("Check these ports are listening:")
#         logger.info(f"  netstat -an | findstr :{self.base_port}")
#         for i in range(self.num_workers):
#             logger.info(f"  netstat -an | findstr :{self.base_port + 1 + i}")
#         logger.info("=" * 70)

#         try:
#             # Monitor worker health
#             last_check = time.time()
#             while any(p.is_alive() for p in self.workers) or metadata_proc.is_alive():
#                 time.sleep(1)

#                 # Check worker health every 10 seconds
#                 if time.time() - last_check >= 10:
#                     alive = sum(1 for p in self.workers if p.is_alive())
#                     if alive < self.num_workers:
#                         logger.warning(f"Worker health check: {alive}/{self.num_workers} alive")
#                     last_check = time.time()

#         except KeyboardInterrupt:
#             logger.info("Stopping server...")
#         finally:
#             metadata_proc.terminate()
#             for p in self.workers:
#                 p.terminate()
#             logger.info("Server stopped")


# class ParallelClient:
#     """Client with multiple workers"""

#     def __init__(self, num_workers: int = DEFAULT_WORKERS, window_size: int = DEFAULT_WINDOW_SIZE):
#         self.num_workers = num_workers
#         self.window_size = window_size

#     def receive_file(self, server_host: str, base_port: int) -> bool:
#         """Receive file from server"""
#         try:
#             logger.info(f"Connecting to {server_host}:{base_port}")

#             client_host = enet.Host(None, 1, 1, 0, 0)
#             address = enet.Address(server_host.encode('utf-8'), base_port)
#             peer = client_host.connect(address, 1)

#             event = client_host.service(5000)
#             if event.type != enet.EVENT_TYPE_CONNECT:
#                 logger.error("Connection failed")
#                 return False

#             logger.info("Connected - requesting metadata")

#             config_packet = struct.pack('!B', PacketType.WORKER_READY)
#             packet = enet.Packet(config_packet, enet.PACKET_FLAG_RELIABLE)
#             peer.send(0, packet)

#             # Wait for metadata
#             file_info = None
#             start = time.time()

#             while time.time() - start < 10:
#                 event = client_host.service(100)

#                 if event.type == enet.EVENT_TYPE_RECEIVE:
#                     if event.packet.data[0] == PacketType.FILE_METADATA:
#                         offset = 1
#                         fname_len, = struct.unpack('!I', event.packet.data[offset:offset+4])
#                         offset += 4

#                         file_size, total_chunks, chunk_size = struct.unpack('!QQI',
#                                                                             event.packet.data[offset:offset+20])
#                         offset += 20

#                         filename = event.packet.data[offset:offset+fname_len].decode('utf-8')
#                         offset += fname_len
#                         file_hash = event.packet.data[offset:offset+16].decode('utf-8')

#                         file_info = {
#                             'filename': filename,
#                             'size': file_size,
#                             'total': total_chunks,
#                             'chunk_size': chunk_size,
#                             'hash': file_hash
#                         }

#                         logger.info(f"File: {filename}, {file_size/(1024**2):.1f} MB, {total_chunks} chunks")
#                         break

#             peer.disconnect()

#             if not file_info:
#                 logger.error("No metadata received")
#                 return False

#             output_path = TRANSFER_DIR / file_info['filename']

#             # Pre-allocate file
#             with open(output_path, 'wb') as f:
#                 f.truncate(file_info['size'])

#             logger.info(f"Pre-allocated file: {output_path}")
#             logger.info(f"Waiting for server workers to be ready...")
#             time.sleep(3)  # Give server workers time to bind ports

#             # Distribute chunks among workers
#             chunks_per_worker_base = file_info['total'] // self.num_workers
#             worker_chunks = []

#             for i in range(self.num_workers):
#                 start_chunk = i * chunks_per_worker_base
#                 end_chunk = (start_chunk + chunks_per_worker_base
#                            if i < self.num_workers - 1 else file_info['total'])
#                 worker_chunks.append(list(range(start_chunk, end_chunk)))

#             # Start workers
#             result_queue = mp.Queue()
#             workers = []
#             start_time = time.time()

#             is_windows = platform.system() == 'Windows'
#             worker_func = client_worker_windows if is_windows else client_worker_unix

#             logger.info("=" * 70)
#             logger.info(f"CLIENT STARTING")
#             logger.info("=" * 70)
#             logger.info(f"Platform: {platform.system()}")
#             logger.info(f"Workers: {self.num_workers}")
#             logger.info(f"Window size: {self.window_size}")
#             logger.info(f"Server: {server_host}")
#             logger.info(f"Worker ports: {base_port + 1} to {base_port + 1 + self.num_workers - 1}")
#             logger.info("=" * 70)
#             logger.info(f"Starting {self.num_workers} workers...")

#             for i in range(self.num_workers):
#                 p = mp.Process(
#                     target=worker_func,
#                     args=(i, server_host, base_port + 1, worker_chunks[i], str(output_path),
#                           file_info['size'], file_info['chunk_size'], self.window_size, result_queue)
#                 )
#                 p.start()
#                 workers.append(p)

#             # Wait for completion
#             completed = 0
#             total_received = 0

#             while completed < self.num_workers:
#                 try:
#                     msg = result_queue.get(timeout=2)
#                     if msg[0] == 'worker_done':
#                         completed += 1
#                         total_received += msg[2]
#                         logger.info(f"Worker {msg[1]} done: {msg[2]} chunks")
#                 except:
#                     if not any(p.is_alive() for p in workers):
#                         break

#             for p in workers:
#                 p.join(timeout=2)

#             elapsed = time.time() - start_time
#             speed = (file_info['size'] / elapsed) / (1024 * 1024) if elapsed > 0 else 0

#             logger.info("=" * 70)
#             logger.info(f"TRANSFER COMPLETE")
#             logger.info(f"Time: {elapsed:.2f}s")
#             logger.info(f"Speed: {speed:.1f} MB/s")
#             logger.info(f"Received: {total_received}/{file_info['total']} chunks")
#             logger.info(f"Saved: {output_path}")

#             # Verify
#             actual_size = output_path.stat().st_size
#             if actual_size == file_info['size']:
#                 logger.info("Size: VERIFIED")

#                 hasher = xxhash.xxh64()
#                 with open(output_path, 'rb') as f:
#                     while chunk := f.read(file_info['chunk_size'] * 4):
#                         hasher.update(chunk)

#                 if hasher.hexdigest() == file_info['hash']:
#                     logger.info("Hash: VERIFIED - Transfer successful!")
#                 else:
#                     logger.warning("Hash: MISMATCH - File may be corrupted")
#             else:
#                 logger.warning(f"Size: MISMATCH ({actual_size} vs {file_info['size']})")

#             logger.info("=" * 70)

#             return total_received == file_info['total']

#         except Exception as e:
#             logger.error(f"Error: {e}")
#             import traceback
#             traceback.print_exc()
#             return False


# def run_server(filepath: str, host: str = "0.0.0.0", port: int = 9000,
#                workers: int = None, chunk_size: int = None, window: int = None):
#     """Start server with optional config"""
#     num_workers = workers or DEFAULT_WORKERS
#     chunk_sz = chunk_size or DEFAULT_CHUNK_SIZE
#     window_sz = window or DEFAULT_WINDOW_SIZE

#     server = ParallelServer(host, port, num_workers, chunk_sz, window_sz)
#     server.serve_file(Path(filepath))


# def run_client(server_host: str, port: int = 9000, workers: int = None, window: int = None):
#     """Start client with optional config"""
#     num_workers = workers or DEFAULT_WORKERS
#     window_sz = window or DEFAULT_WINDOW_SIZE

#     client = ParallelClient(num_workers, window_sz)
#     client.receive_file(server_host, port)


# if __name__ == "__main__":
#     import sys

#     mp.set_start_method('spawn', force=True)

#     if not ENET_AVAILABLE:
#         print("ERROR: pip install pyenet-updated numba numpy xxhash psutil")
#         sys.exit(1)

#     print("\n" + "=" * 70)
#     print("ULTRA-FAST UDP FILE TRANSFER")
#     print("=" * 70)
#     print("Strategy: UNSEQUENCED packets + Application-level retransmission")
#     print("Protocol: Receiver-driven flow control (like RBUDP/FASP)")
#     print()
#     print("CRITICAL: FIREWALL MUST ALLOW UDP PORTS")
#     print("=" * 70)

#     if platform.system() == 'Windows':
#         print("Windows Firewall:")
#         print("  1. Run as Administrator OR")
#         print("  2. Add firewall rule:")
#         print('     netsh advfirewall firewall add rule name="UDP File Transfer"')
#         print('     dir=in action=allow protocol=UDP localport=9000-9020')
#         print()
#         print("  3. Check if ports are open:")
#         print("     netstat -an | findstr :9000")
#     else:
#         print("Linux Firewall:")
#         print("  sudo ufw allow 9000:9020/udp")
#         print("  OR: sudo iptables -A INPUT -p udp --dport 9000:9020 -j ACCEPT")
#         print()
#         print("  Check: sudo netstat -uln | grep 9000")

#     print()
#     print("TROUBLESHOOTING CONNECTION FAILURES:")
#     print("  1. Check server is running FIRST")
#     print("  2. Wait 5 seconds after server starts")
#     print("  3. Verify ports with netstat command above")
#     print("  4. Temporarily disable firewall to test")
#     print("  5. Check antivirus isn't blocking UDP")
#     print("=" * 70)

#     if len(sys.argv) < 2:
#         print("\nUSAGE:")
#         print("  Server: python script.py server <file> [host] [port] [workers] [chunk_mb] [window]")
#         print("  Client: python script.py client <host> [port] [workers] [window]")
#         print()
#         print("DEFAULTS:")
#         print(f"  Workers: {DEFAULT_WORKERS}")
#         print(f"  Chunk size: {DEFAULT_CHUNK_SIZE / (1024**2):.0f} MB")
#         print(f"  Window: {DEFAULT_WINDOW_SIZE} chunks per worker")
#         print()
#         print("EXAMPLES:")
#         print("  Server: python script.py server bigfile.bin 0.0.0.0 9000 8 1 64")
#         print("  Client: python script.py client 192.168.1.100 9000 8 64")
#         print()
#         print("TUNING:")
#         print("  - More workers = higher throughput (but more CPU/memory)")
#         print("  - Larger chunks = fewer syscalls (1-4 MB recommended)")
#         print("  - Larger window = more in-flight data (32-128 recommended)")
#         print("  - For LAN: 8 workers, 1MB chunks, 64 window")
#         print("  - For WAN: 16 workers, 2MB chunks, 128 window")
#         sys.exit(1)

#     mode = sys.argv[1]

#     if mode == "server":
#         if len(sys.argv) < 3:
#             print("Error: Specify file to serve")
#             sys.exit(1)

#         filepath = sys.argv[2]
#         host = sys.argv[3] if len(sys.argv) > 3 else "0.0.0.0"
#         port = int(sys.argv[4]) if len(sys.argv) > 4 else 9000
#         workers = int(sys.argv[5]) if len(sys.argv) > 5 else None
#         chunk_mb = int(sys.argv[6]) if len(sys.argv) > 6 else None
#         window = int(sys.argv[7]) if len(sys.argv) > 7 else None

#         chunk_size = chunk_mb * 1024 * 1024 if chunk_mb else None

#         run_server(filepath, host, port, workers, chunk_size, window)

#     elif mode == "client":
#         if len(sys.argv) < 3:
#             print("Error: Specify server host")
#             sys.exit(1)

#         server_host = sys.argv[2]
#         port = int(sys.argv[3]) if len(sys.argv) > 3 else 9000
#         workers = int(sys.argv[4]) if len(sys.argv) > 4 else None
#         window = int(sys.argv[5]) if len(sys.argv) > 5 else None

#         run_client(server_host, port, workers, window)

#     else:
#         print(f"Unknown mode: {mode}")
#         sys.exit(1)


"""
Ultra-fast UDP file transfer with proper flow control
Strategy: UNSEQUENCED for speed + application-level selective retransmission
Research: Aspera FASP, RBUDP, UDT protocols

CRITICAL FIXES from analysis:
1. UNSEQUENCED for maximum speed (no head-of-line blocking)
2. Application-level bitmap ACK (like RBUDP)
3. Receiver-driven flow control (prevents server flooding)
4. Separate control channel for ACKs
5. Rate limiting based on RTT feedback

Windows: pip install pyenet-updated numba numpy xxhash psutil
"""

import ctypes
import logging
import mmap
import multiprocessing as mp
import os
import platform
import socket
import struct
import sys
import time
from collections import deque
from pathlib import Path
from typing import Set, Tuple, Dict

import numpy as np
import xxhash
from numba import jit

try:
    import enet
    ENET_AVAILABLE = True
except ImportError:
    ENET_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Optimized for speed + stability
DEFAULT_WORKERS = 8
DEFAULT_CHUNK_SIZE = 2097152  # 2MB (increased from 1MB for better throughput)
DEFAULT_WINDOW_SIZE = 128  # Increased from 64 for deeper pipeline

TRANSFER_DIR = Path("./transfers")
TRANSFER_DIR.mkdir(exist_ok=True)

# Performance tuning constants
PACKET_BATCH_SIZE = 16  # Send packets in batches
SERVICE_TIMEOUT_MS = 1  # Very low latency (1ms instead of 5ms)
FLUSH_INTERVAL = 32  # Flush every N packets

# Windows API for concurrent file access
if platform.system() == 'Windows':
    import msvcrt

    GENERIC_READ = 0x80000000
    GENERIC_WRITE = 0x40000000
    FILE_SHARE_READ = 0x00000001
    FILE_SHARE_WRITE = 0x00000002
    OPEN_EXISTING = 3
    FILE_ATTRIBUTE_NORMAL = 0x80
    FILE_FLAG_RANDOM_ACCESS = 0x10000000
    FILE_FLAG_WRITE_THROUGH = 0x80000000

    kernel32 = ctypes.windll.kernel32


@jit(nopython=True, cache=True, fastmath=True, inline='always')
def fast_hash(data: np.ndarray) -> np.uint32:
    """Optimized hash"""
    h = np.uint32(2166136261)
    prime = np.uint32(16777619)

    for i in range(0, len(data), 16):
        h = np.uint32((h ^ data[i]) * prime)

    return h


@jit(nopython=True, cache=True, fastmath=True)
def verify_hash(data: np.ndarray, expected: np.uint32) -> bool:
    """Fast verify"""
    return fast_hash(data) == expected


class PacketType:
    CHUNK_DATA = 1
    CHUNK_REQUEST = 2
    BITMAP_ACK = 3  # Receiver sends bitmap of received chunks
    WORKER_READY = 4
    FILE_METADATA = 5
    RATE_FEEDBACK = 6  # Client sends rate info to server


def open_shared_file_windows(filepath: str, file_size: int):
    """Open file with Windows API for concurrent access"""
    if platform.system() != 'Windows':
        raise OSError("Windows-only function")

    import msvcrt

    try:
        filepath_wide = ctypes.create_unicode_buffer(filepath)

        handle = kernel32.CreateFileW(
            filepath_wide,
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_WRITE_THROUGH,
            None
        )

        if handle == -1 or handle == 0:
            error_code = kernel32.GetLastError()
            raise OSError(f"CreateFile failed: {error_code}")

        fd = msvcrt.open_osfhandle(handle, os.O_RDWR | os.O_BINARY)
        return fd, handle

    except Exception as e:
        logger.error(f"Failed to open file: {e}")
        raise


def write_at_offset_windows(handle, offset: int, data: bytes):
    """Direct write at offset using Windows API"""
    if platform.system() != 'Windows':
        raise OSError("Windows-only function")

    offset_low = ctypes.c_ulong(offset & 0xFFFFFFFF)
    offset_high = ctypes.c_long(offset >> 32)

    result = kernel32.SetFilePointer(handle, offset_low, ctypes.byref(offset_high), 0)

    if result == 0xFFFFFFFF:
        error = kernel32.GetLastError()
        if error != 0:
            raise OSError(f"SetFilePointer failed: {error}")

    bytes_written = ctypes.c_ulong()
    success = kernel32.WriteFile(handle, data, len(data), ctypes.byref(bytes_written), None)

    if not success:
        raise OSError(f"WriteFile failed: {kernel32.GetLastError()}")

    return bytes_written.value


def server_worker(worker_id: int, port: int, filepath: str, chunk_size: int, window_size: int):
    """
    Server worker with UNSEQUENCED + receiver-driven flow control.

    Strategy:
    - Send chunks as fast as client requests (UNSEQUENCED for speed)
    - Client controls rate via BITMAP_ACK feedback
    - Stay alive until client explicitly disconnects
    """
    try:
        logger.info(f"Worker {worker_id}: Starting initialization...")

        file_path = Path(filepath)
        if not file_path.exists():
            logger.error(f"Worker {worker_id}: File not found: {filepath}")
            return

        logger.info(f"Worker {worker_id}: Opening file {file_path.name}")
        f = open(file_path, 'rb')
        file_size = file_path.stat().st_size
        m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        logger.info(f"Worker {worker_id}: File opened, size: {file_size / (1024**2):.1f} MB")

        # ENet host with 2 channels: data (0) and control (1)
        logger.info(f"Worker {worker_id}: Binding to 0.0.0.0:{port + worker_id}")
        address = enet.Address(b'0.0.0.0', port + worker_id)

        try:
            host = enet.Host(
                address,
                1,  # One peer
                2,  # Two channels
                0,  # Unlimited incoming
                0   # Unlimited outgoing (let network handle it)
            )
            logger.info(f"Worker {worker_id}: Successfully bound to port {port + worker_id}")
        except Exception as bind_error:
            logger.error(f"Worker {worker_id}: FAILED to bind port {port + worker_id}")
            logger.error(f"Worker {worker_id}: Error: {bind_error}")
            logger.error(f"Worker {worker_id}: Port may be in use. Check with: netstat -an | findstr :{port + worker_id}")
            m.close()
            f.close()
            return

        logger.info(f"Worker {worker_id}: READY and listening on 0.0.0.0:{port + worker_id}")

        peer = None
        chunks_sent = 0
        last_stats = time.time()
        bytes_sent_interval = 0

        # Track client's received status (updated via bitmap ACKs)
        client_received = set()
        last_request_time = time.time()

        while True:
            # CRITICAL: Very low timeout for maximum responsiveness
            event = host.service(10)  # 10ms - balance between CPU and responsiveness

            if event.type == enet.EVENT_TYPE_CONNECT:
                peer = event.peer
                last_request_time = time.time()
                logger.info(f"Worker {worker_id}: Client connected")

            elif event.type == enet.EVENT_TYPE_RECEIVE:
                if not peer:
                    continue

                last_request_time = time.time()  # Reset timeout on any activity
                packet_type = event.packet.data[0]

                if packet_type == PacketType.CHUNK_REQUEST:
                    # Client requests specific chunk
                    chunk_index, = struct.unpack('!I', event.packet.data[1:5])

                    offset = chunk_index * chunk_size
                    if offset >= file_size:
                        logger.warning(f"Worker {worker_id}: Invalid chunk {chunk_index}")
                        continue

                    end = min(offset + chunk_size, file_size)
                    chunk_actual_size = end - offset

                    # Zero-copy read
                    chunk_data = m[offset:end]

                    # Fast hash
                    data_array = np.frombuffer(chunk_data, dtype=np.uint8)
                    checksum = fast_hash(data_array)

                    # Send UNSEQUENCED for maximum speed
                    packet_data = struct.pack('!BIII', PacketType.CHUNK_DATA, chunk_index,
                                            chunk_actual_size, checksum) + chunk_data
                    packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
                    peer.send(0, packet)  # Data channel

                    chunks_sent += 1
                    bytes_sent_interval += chunk_actual_size

                    # Flush less frequently for better batching
                    if chunks_sent % FLUSH_INTERVAL == 0:
                        host.flush()

                elif packet_type == PacketType.BITMAP_ACK:
                    # Client sends bitmap of received chunks
                    num_received, = struct.unpack('!I', event.packet.data[1:5])
                    if num_received > 0 and len(event.packet.data) >= 5 + num_received * 4:
                        received_chunks = struct.unpack(f'!{num_received}I',
                                                       event.packet.data[5:5 + num_received * 4])
                        client_received.update(received_chunks)
                        logger.debug(f"Worker {worker_id}: Client has {len(client_received)} chunks")

                # Stats reporting
                current_time = time.time()
                if current_time - last_stats >= 2.0:
                    elapsed = current_time - last_stats
                    if elapsed > 0:
                        speed_mbps = (bytes_sent_interval / elapsed) / (1024 * 1024)
                        logger.info(f"Worker {worker_id}: {chunks_sent} sent, {speed_mbps:.1f} MB/s, "
                                  f"client has {len(client_received)} chunks")
                        bytes_sent_interval = 0
                        last_stats = current_time

            elif event.type == enet.EVENT_TYPE_DISCONNECT:
                logger.info(f"Worker {worker_id} client disconnected: {chunks_sent} chunks sent")
                break

            # Check for client timeout (no activity for 60 seconds)
            if peer and (time.time() - last_request_time) > 60:
                logger.warning(f"Worker {worker_id}: Client timeout, closing")
                break

        m.close()
        f.close()

    except Exception as e:
        logger.error(f"Worker {worker_id} error: {e}")
        import traceback
        traceback.print_exc()


def client_worker_windows(worker_id: int, server_host: str, port: int, chunk_indices: list,
                          output_path: str, file_size: int, chunk_size: int, window_size: int,
                          result_queue: mp.Queue):
    """
    Client worker with UNSEQUENCED + application-level retransmission.

    Strategy (like RBUDP):
    1. Request chunks in batches (window-based)
    2. Receive UNSEQUENCED for low latency
    3. Track received chunks in bitmap
    4. Periodically send bitmap ACK to server
    5. Retransmit missing chunks at end
    """
    if platform.system() != 'Windows':
        logger.error(f"Worker {worker_id}: Windows required")
        return

    import msvcrt

    try:
        fd, handle = open_shared_file_windows(output_path, file_size)
        logger.info(f"Worker {worker_id}: File opened")

        # Connect
        client_host = enet.Host(None, 1, 2, 0, 0)
        address = enet.Address(server_host.encode('utf-8'), port + worker_id)
        peer = client_host.connect(address, 2)

        # Wait for connection with retry
        logger.info(f"Worker {worker_id}: Connecting to port {port + worker_id}...")

        connected = False
        for attempt in range(3):
            event = client_host.service(2000)  # 2 second timeout per attempt
            if event.type == enet.EVENT_TYPE_CONNECT:
                connected = True
                break

            if attempt < 2:
                logger.warning(f"Worker {worker_id}: Connection attempt {attempt + 1} failed, retrying...")
                time.sleep(1)

        if not connected:
            logger.error(f"Worker {worker_id}: Connection failed after 3 attempts to port {port + worker_id}")
            kernel32.CloseHandle(handle)
            result_queue.put(('worker_done', worker_id, 0))
            return

        logger.info(f"Worker {worker_id}: Connected to port {port + worker_id}")

        # State
        received = set()
        requested = set()

        # Request initial window - use smaller window to start
        initial_window = min(window_size // 2, 32, len(chunk_indices))
        logger.info(f"Worker {worker_id}: Requesting initial window of {initial_window} chunks")

        for i in range(initial_window):
            chunk_idx = chunk_indices[i]
            packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
            packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
            peer.send(0, packet)
            requested.add(chunk_idx)

        # Flush to ensure requests sent
        client_host.flush()

        next_to_request = initial_window
        last_progress = time.time()
        last_ack_sent = time.time()

        # Diagnostic counters
        packets_received = 0
        hash_failures = 0
        write_failures = 0
        duplicates = 0
        prev_received = 0  # For speed calculation

        # Main receive loop
        while len(received) < len(chunk_indices):
            # Very fast polling for low latency
            event = client_host.service(SERVICE_TIMEOUT_MS)

            if event.type == enet.EVENT_TYPE_RECEIVE:
                packets_received += 1
                packet_type = event.packet.data[0]

                if packet_type == PacketType.CHUNK_DATA:
                    if len(event.packet.data) < 13:
                        continue

                    chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
                                                                            event.packet.data[1:13])
                    chunk_data = event.packet.data[13:13 + received_chunk_size]

                    if chunk_idx not in chunk_indices:
                        continue

                    if chunk_idx in received:
                        duplicates += 1
                        continue

                    if len(chunk_data) != received_chunk_size:
                        logger.warning(f"Worker {worker_id}: Chunk {chunk_idx} size mismatch")
                        continue

                    # Verify hash
                    data_array = np.frombuffer(chunk_data, dtype=np.uint8)
                    if not verify_hash(data_array, np.uint32(checksum)):
                        hash_failures += 1
                        logger.warning(f"Worker {worker_id}: Chunk {chunk_idx} hash failed")
                        continue

                    # Write to disk
                    try:
                        offset = chunk_idx * chunk_size
                        bytes_written = write_at_offset_windows(handle, offset, chunk_data)

                        if bytes_written != received_chunk_size:
                            write_failures += 1
                            logger.error(f"Worker {worker_id}: Write failed for {chunk_idx}")
                            continue
                    except Exception as e:
                        write_failures += 1
                        logger.error(f"Worker {worker_id}: Write error {chunk_idx}: {e}")
                        continue

                    # Success
                    received.add(chunk_idx)

                    # Aggressive window filling - request multiple chunks at once
                    batch_requests = []
                    while next_to_request < len(chunk_indices) and len(requested - received) < window_size:
                        new_chunk_idx = chunk_indices[next_to_request]
                        if new_chunk_idx not in requested:
                            batch_requests.append(new_chunk_idx)
                            requested.add(new_chunk_idx)
                            if len(batch_requests) >= PACKET_BATCH_SIZE:
                                break
                        next_to_request += 1

                    # Send batch requests
                    for new_chunk_idx in batch_requests:
                        packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, new_chunk_idx)
                        packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
                        peer.send(0, packet)

                    if batch_requests:
                        client_host.flush()

                    # Progress with diagnostics
                    current_time = time.time()
                    if current_time - last_progress >= 2.0:
                        progress = len(received) / len(chunk_indices) * 100
                        in_flight = len(requested - received)
                        elapsed = current_time - last_progress
                        if elapsed > 0:
                            chunks_per_sec = (len(received) - prev_received) / elapsed
                            speed_mbps = (chunks_per_sec * chunk_size) / (1024 * 1024)
                            prev_received = len(received)

                            logger.info(f"Worker {worker_id}: {len(received)}/{len(chunk_indices)} "
                                      f"({progress:.1f}%), {speed_mbps:.1f} MB/s, in-flight: {in_flight}, "
                                      f"pkts: {packets_received}, hash_fail: {hash_failures}, "
                                      f"write_fail: {write_failures}, dup: {duplicates}")
                        last_progress = current_time

            elif event.type == enet.EVENT_TYPE_DISCONNECT:
                logger.warning(f"Worker {worker_id}: Server disconnected unexpectedly")
                logger.warning(f"Worker {worker_id}: Diagnostics - received: {len(received)}, "
                             f"requested: {len(requested)}, missing: {len(requested - received)}")
                break

            # Send periodic bitmap ACK (like RBUDP)
            current_time = time.time()
            if current_time - last_ack_sent >= 1.0 and len(received) > 0:
                received_list = sorted(list(received))
                if received_list:
                    # Send up to 1000 chunk IDs at a time
                    batch_size = min(1000, len(received_list))
                    ack_data = struct.pack(f'!BI{batch_size}I',
                                          PacketType.BITMAP_ACK,
                                          batch_size,
                                          *received_list[:batch_size])
                    packet = enet.Packet(ack_data, enet.PACKET_FLAG_RELIABLE)
                    peer.send(1, packet)  # Control channel
                    last_ack_sent = current_time

            # Check for stalled transfer - re-request missing chunks
            if current_time - last_progress > 5.0:
                in_flight_chunks = requested - received
                if len(in_flight_chunks) > 0:
                    # Re-request first 10 in-flight chunks
                    retry_chunks = sorted(list(in_flight_chunks))[:10]
                    logger.info(f"Worker {worker_id}: Stalled, retrying {len(retry_chunks)} chunks")

                    for chunk_idx in retry_chunks:
                        packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
                        packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
                        peer.send(0, packet)

                    client_host.flush()
                    last_progress = current_time

        # Retransmission phase for missing chunks
        missing = set(chunk_indices) - received
        if missing:
            logger.info(f"Worker {worker_id}: Retransmitting {len(missing)} missing chunks")

            # Send all missing chunk requests with RELIABLE
            for chunk_idx in sorted(missing):
                packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
                packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
                peer.send(0, packet)

            # Flush to ensure all requests sent
            client_host.flush()

            # Wait for retransmitted chunks with longer timeout
            retrans_start = time.time()
            retrans_received = 0

            while len(received) < len(chunk_indices) and (time.time() - retrans_start) < 60:
                event = client_host.service(100)  # Longer service time for retransmit

                if event.type == enet.EVENT_TYPE_RECEIVE:
                    packet_type = event.packet.data[0]

                    if packet_type == PacketType.CHUNK_DATA:
                        if len(event.packet.data) < 13:
                            continue

                        chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
                                                                                event.packet.data[1:13])
                        chunk_data = event.packet.data[13:13 + received_chunk_size]

                        if chunk_idx in received or chunk_idx not in chunk_indices:
                            continue

                        if len(chunk_data) != received_chunk_size:
                            logger.warning(f"Worker {worker_id}: Retransmit chunk {chunk_idx} size mismatch")
                            continue

                        # Verify hash
                        data_array = np.frombuffer(chunk_data, dtype=np.uint8)
                        if not verify_hash(data_array, np.uint32(checksum)):
                            logger.warning(f"Worker {worker_id}: Retransmit chunk {chunk_idx} hash failed")
                            continue

                        # Write to disk
                        try:
                            offset = chunk_idx * chunk_size
                            bytes_written = write_at_offset_windows(handle, offset, chunk_data)

                            if bytes_written == received_chunk_size:
                                received.add(chunk_idx)
                                retrans_received += 1

                                if retrans_received % 10 == 0:
                                    logger.info(f"Worker {worker_id}: Retransmit progress {retrans_received}/{len(missing)}")
                        except Exception as e:
                            logger.error(f"Worker {worker_id}: Retransmit write error {chunk_idx}: {e}")
                            continue

                elif event.type == enet.EVENT_TYPE_DISCONNECT:
                    logger.warning(f"Worker {worker_id}: Server disconnected during retransmit")
                    break

            if len(received) < len(chunk_indices):
                still_missing = len(chunk_indices) - len(received)
                logger.warning(f"Worker {worker_id}: Still missing {still_missing} chunks after retransmit")

        # Final flush and close
        kernel32.FlushFileBuffers(handle)
        kernel32.CloseHandle(handle)

        # Graceful disconnect
        if peer:
            peer.disconnect_later()

            # Wait for disconnect event
            timeout = time.time() + 2
            while time.time() < timeout:
                event = client_host.service(100)
                if event.type == enet.EVENT_TYPE_DISCONNECT:
                    break

        result_queue.put(('worker_done', worker_id, len(received)))
        logger.info(f"Worker {worker_id} complete: {len(received)}/{len(chunk_indices)}")

    except Exception as e:
        logger.error(f"Worker {worker_id} error: {e}")
        import traceback
        traceback.print_exc()
        result_queue.put(('worker_done', worker_id, 0))


def client_worker_unix(worker_id: int, server_host: str, port: int, chunk_indices: list,
                       output_path: str, file_size: int, chunk_size: int, window_size: int,
                       result_queue: mp.Queue):
    """Unix/Linux client worker using pwrite"""
    try:
        fd = os.open(output_path, os.O_RDWR)
        logger.info(f"Worker {worker_id}: File opened (fd: {fd})")

        client_host = enet.Host(None, 1, 2, 0, 0)
        address = enet.Address(server_host.encode('utf-8'), port + worker_id)
        peer = client_host.connect(address, 2)

        event = client_host.service(5000)
        if event.type != enet.EVENT_TYPE_CONNECT:
            logger.error(f"Worker {worker_id}: Connection failed")
            os.close(fd)
            return

        logger.info(f"Worker {worker_id}: Connected")

        received = set()
        requested = set()

        # Request initial window - smaller to start
        initial_window = min(window_size // 2, 32, len(chunk_indices))
        logger.info(f"Worker {worker_id}: Requesting initial {initial_window} chunks")

        for i in range(initial_window):
            chunk_idx = chunk_indices[i]
            packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
            packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
            peer.send(0, packet)
            requested.add(chunk_idx)

        client_host.flush()

        next_to_request = initial_window
        last_progress = time.time()
        last_ack_sent = time.time()

        # Diagnostics
        packets_received = 0
        hash_failures = 0
        prev_received = 0

        while len(received) < len(chunk_indices):
            event = client_host.service(SERVICE_TIMEOUT_MS)

            if event.type == enet.EVENT_TYPE_RECEIVE:
                packets_received += 1
                packet_type = event.packet.data[0]

                if packet_type == PacketType.CHUNK_DATA:
                    if len(event.packet.data) < 13:
                        continue

                    chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
                                                                            event.packet.data[1:13])
                    chunk_data = event.packet.data[13:13 + received_chunk_size]

                    if chunk_idx not in chunk_indices or chunk_idx in received:
                        continue

                    data_array = np.frombuffer(chunk_data, dtype=np.uint8)
                    if not verify_hash(data_array, np.uint32(checksum)):
                        hash_failures += 1
                        continue

                    try:
                        offset = chunk_idx * chunk_size
                        bytes_written = os.pwrite(fd, chunk_data, offset)

                        if bytes_written == received_chunk_size:
                            received.add(chunk_idx)
                    except Exception as e:
                        logger.error(f"Worker {worker_id}: Write error: {e}")
                        continue

                    # Aggressive batch requesting
                    batch_requests = []
                    while next_to_request < len(chunk_indices) and len(requested - received) < window_size:
                        new_chunk_idx = chunk_indices[next_to_request]
                        if new_chunk_idx not in requested:
                            batch_requests.append(new_chunk_idx)
                            requested.add(new_chunk_idx)
                            if len(batch_requests) >= PACKET_BATCH_SIZE:
                                break
                        next_to_request += 1

                    for new_chunk_idx in batch_requests:
                        packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, new_chunk_idx)
                        packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
                        peer.send(0, packet)

                    if batch_requests:
                        client_host.flush()

                    # Progress with speed
                    current_time = time.time()
                    if current_time - last_progress >= 2.0:
                        progress = len(received) / len(chunk_indices) * 100
                        elapsed = current_time - last_progress
                        if elapsed > 0:
                            chunks_per_sec = (len(received) - prev_received) / elapsed
                            speed_mbps = (chunks_per_sec * chunk_size) / (1024 * 1024)
                            prev_received = len(received)

                            logger.info(f"Worker {worker_id}: {len(received)}/{len(chunk_indices)} ({progress:.1f}%), "
                                      f"{speed_mbps:.1f} MB/s, pkts: {packets_received}, hash_fail: {hash_failures}")
                        last_progress = current_time

            elif event.type == enet.EVENT_TYPE_DISCONNECT:
                logger.warning(f"Worker {worker_id}: Server disconnected")
                break

            elif event.type == enet.EVENT_TYPE_DISCONNECT:
                logger.warning(f"Worker {worker_id}: Server disconnected")
                break

            # Periodic ACK
            current_time = time.time()
            if current_time - last_ack_sent >= 1.0 and len(received) > 0:
                received_list = sorted(list(received))[:1000]
                if received_list:
                    ack_data = struct.pack(f'!BI{len(received_list)}I',
                                          PacketType.BITMAP_ACK,
                                          len(received_list),
                                          *received_list)
                    packet = enet.Packet(ack_data, enet.PACKET_FLAG_RELIABLE)
                    peer.send(1, packet)
                    last_ack_sent = current_time

            # Stall detection and retry
            if current_time - last_progress > 5.0:
                in_flight = requested - received
                if len(in_flight) > 0:
                    retry = sorted(list(in_flight))[:10]
                    logger.info(f"Worker {worker_id}: Stalled, retrying {len(retry)} chunks")
                    for chunk_idx in retry:
                        packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
                        packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
                        peer.send(0, packet)
                    client_host.flush()
                    last_progress = current_time

        # Retransmit missing
        missing = set(chunk_indices) - received
        if missing:
            logger.info(f"Worker {worker_id}: Retransmitting {len(missing)} chunks")

            for chunk_idx in sorted(missing):
                packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
                packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
                peer.send(0, packet)

            client_host.flush()

            retrans_start = time.time()
            retrans_received = 0

            while len(received) < len(chunk_indices) and (time.time() - retrans_start) < 60:
                event = client_host.service(100)

                if event.type == enet.EVENT_TYPE_RECEIVE:
                    packet_type = event.packet.data[0]

                    if packet_type == PacketType.CHUNK_DATA:
                        if len(event.packet.data) < 13:
                            continue

                        chunk_idx, received_chunk_size, checksum = struct.unpack('!III',
                                                                                event.packet.data[1:13])
                        chunk_data = event.packet.data[13:13 + received_chunk_size]

                        if chunk_idx in received or chunk_idx not in chunk_indices:
                            continue

                        data_array = np.frombuffer(chunk_data, dtype=np.uint8)
                        if not verify_hash(data_array, np.uint32(checksum)):
                            continue

                        try:
                            offset = chunk_idx * chunk_size
                            bytes_written = os.pwrite(fd, chunk_data, offset)

                            if bytes_written == received_chunk_size:
                                received.add(chunk_idx)
                                retrans_received += 1

                                if retrans_received % 10 == 0:
                                    logger.info(f"Worker {worker_id}: Retransmit {retrans_received}/{len(missing)}")
                        except Exception as e:
                            logger.error(f"Worker {worker_id}: Retransmit write error: {e}")
                            continue

                elif event.type == enet.EVENT_TYPE_DISCONNECT:
                    logger.warning(f"Worker {worker_id}: Server disconnected during retransmit")
                    break

            if len(received) < len(chunk_indices):
                logger.warning(f"Worker {worker_id}: Still missing {len(chunk_indices) - len(received)} chunks")

        os.fsync(fd)
        os.close(fd)

        if peer:
            peer.disconnect_later()
            timeout = time.time() + 2
            while time.time() < timeout:
                event = client_host.service(100)
                if event.type == enet.EVENT_TYPE_DISCONNECT:
                    break

        result_queue.put(('worker_done', worker_id, len(received)))
        logger.info(f"Worker {worker_id} complete: {len(received)}/{len(chunk_indices)}")

    except Exception as e:
        logger.error(f"Worker {worker_id} error: {e}")
        import traceback
        traceback.print_exc()
        result_queue.put(('worker_done', worker_id, 0))


def metadata_server_process(base_port: int, filepath: str, file_size: int, total_chunks: int, chunk_size: int):
    """
    Standalone metadata server process (must be top-level function for multiprocessing).
    Sends file metadata to clients.
    """
    try:
        # Convert string back to Path inside the process
        file_path = Path(filepath)

        address = enet.Address(b'0.0.0.0', base_port)
        host = enet.Host(address, 8, 1, 0, 0)

        logger.info(f"Metadata server listening on port {base_port}")

        while True:
            event = host.service(100)

            if event.type == enet.EVENT_TYPE_CONNECT:
                logger.info("Client connected to metadata server")

            elif event.type == enet.EVENT_TYPE_RECEIVE:
                if event.packet.data[0] == PacketType.WORKER_READY:
                    filename_bytes = file_path.name.encode('utf-8')

                    # Calculate file hash
                    hasher = xxhash.xxh64()
                    with open(file_path, 'rb') as f:
                        while chunk := f.read(chunk_size * 4):
                            hasher.update(chunk)
                    file_hash = hasher.hexdigest()

                    # Send metadata packet
                    packet_data = struct.pack(
                        '!BIQQI',
                        PacketType.FILE_METADATA,
                        len(filename_bytes),
                        file_size,
                        total_chunks,
                        chunk_size
                    ) + filename_bytes + file_hash.encode('utf-8')

                    packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
                    event.peer.send(0, packet)

                    logger.info(f"Sent metadata: {file_path.name}, {total_chunks} chunks, {chunk_size} bytes/chunk")

    except Exception as e:
        logger.error(f"Metadata server error: {e}")
        import traceback
        traceback.print_exc()


class ParallelServer:
    """Server with multiple workers"""

    def __init__(self, host: str, base_port: int, num_workers: int = DEFAULT_WORKERS,
                 chunk_size: int = DEFAULT_CHUNK_SIZE, window_size: int = DEFAULT_WINDOW_SIZE):
        self.host = host
        self.base_port = base_port
        self.num_workers = num_workers
        self.chunk_size = chunk_size
        self.window_size = window_size
        self.workers = []

    def serve_file(self, filepath: Path):
        """Start parallel workers"""
        if not filepath.exists():
            logger.error(f"File not found: {filepath}")
            return

        file_size = filepath.stat().st_size
        total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size

        logger.info("=" * 70)
        logger.info(f"SERVER STARTING")
        logger.info("=" * 70)
        logger.info(f"File: {filepath.name}, {file_size / (1024**2):.1f} MB, {total_chunks} chunks")
        logger.info(f"Workers: {self.num_workers}, Chunk size: {self.chunk_size / (1024**2):.1f} MB")
        logger.info(f"Window: {self.window_size} chunks/worker")
        logger.info(f"Metadata port: {self.base_port}")
        logger.info(f"Worker ports: {self.base_port + 1} to {self.base_port + self.num_workers}")
        logger.info("=" * 70)

        # Start workers FIRST (they need to be listening before client connects)
        logger.info(f"Starting {self.num_workers} worker processes...")
        for i in range(self.num_workers):
            p = mp.Process(
                target=server_worker,
                args=(i, self.base_port + 1, str(filepath), self.chunk_size, self.window_size)
            )
            p.start()
            self.workers.append(p)
            logger.info(f"Started worker {i} (PID: {p.pid}) on port {self.base_port + 1 + i}")

        # Give workers time to bind to ports
        logger.info("Waiting for workers to initialize and bind ports...")
        time.sleep(3)  # Increased from 2 to 3 seconds

        # Check if workers are still alive
        alive_workers = sum(1 for p in self.workers if p.is_alive())
        logger.info(f"Workers alive: {alive_workers}/{self.num_workers}")

        if alive_workers < self.num_workers:
            logger.error(f"WARNING: Only {alive_workers}/{self.num_workers} workers are running!")
            logger.error("Check firewall settings or port availability")

        # Start metadata server LAST (use standalone function, not method)
        logger.info(f"Starting metadata server on port {self.base_port}...")
        metadata_proc = mp.Process(
            target=metadata_server_process,
            args=(self.base_port, str(filepath), file_size, total_chunks, self.chunk_size)
        )
        metadata_proc.start()
        logger.info(f"Metadata server started (PID: {metadata_proc.pid})")

        time.sleep(1)

        # Final check
        if metadata_proc.is_alive():
            logger.info("Metadata server: RUNNING")
        else:
            logger.error("Metadata server: FAILED TO START")

        logger.info("=" * 70)
        logger.info("SERVER READY - Waiting for clients...")
        logger.info("Check these ports are listening:")
        logger.info(f"  netstat -an | findstr :{self.base_port}")
        for i in range(self.num_workers):
            logger.info(f"  netstat -an | findstr :{self.base_port + 1 + i}")
        logger.info("=" * 70)

        try:
            # Monitor worker health
            last_check = time.time()
            while any(p.is_alive() for p in self.workers) or metadata_proc.is_alive():
                time.sleep(1)

                # Check worker health every 10 seconds
                if time.time() - last_check >= 10:
                    alive = sum(1 for p in self.workers if p.is_alive())
                    if alive < self.num_workers:
                        logger.warning(f"Worker health check: {alive}/{self.num_workers} alive")
                    last_check = time.time()

        except KeyboardInterrupt:
            logger.info("Stopping server...")
        finally:
            metadata_proc.terminate()
            for p in self.workers:
                p.terminate()
            logger.info("Server stopped")


class ParallelClient:
    """Client with multiple workers"""

    def __init__(self, num_workers: int = DEFAULT_WORKERS, window_size: int = DEFAULT_WINDOW_SIZE):
        self.num_workers = num_workers
        self.window_size = window_size

    def receive_file(self, server_host: str, base_port: int) -> bool:
        """Receive file from server"""
        try:
            logger.info(f"Connecting to {server_host}:{base_port}")

            client_host = enet.Host(None, 1, 1, 0, 0)
            address = enet.Address(server_host.encode('utf-8'), base_port)
            peer = client_host.connect(address, 1)

            event = client_host.service(5000)
            if event.type != enet.EVENT_TYPE_CONNECT:
                logger.error("Connection failed")
                return False

            logger.info("Connected - requesting metadata")

            config_packet = struct.pack('!B', PacketType.WORKER_READY)
            packet = enet.Packet(config_packet, enet.PACKET_FLAG_RELIABLE)
            peer.send(0, packet)

            # Wait for metadata
            file_info = None
            start = time.time()

            while time.time() - start < 10:
                event = client_host.service(100)

                if event.type == enet.EVENT_TYPE_RECEIVE:
                    if event.packet.data[0] == PacketType.FILE_METADATA:
                        offset = 1
                        fname_len, = struct.unpack('!I', event.packet.data[offset:offset+4])
                        offset += 4

                        file_size, total_chunks, chunk_size = struct.unpack('!QQI',
                                                                            event.packet.data[offset:offset+20])
                        offset += 20

                        filename = event.packet.data[offset:offset+fname_len].decode('utf-8')
                        offset += fname_len
                        file_hash = event.packet.data[offset:offset+16].decode('utf-8')

                        file_info = {
                            'filename': filename,
                            'size': file_size,
                            'total': total_chunks,
                            'chunk_size': chunk_size,
                            'hash': file_hash
                        }

                        logger.info(f"File: {filename}, {file_size/(1024**2):.1f} MB, {total_chunks} chunks")
                        break

            peer.disconnect()

            if not file_info:
                logger.error("No metadata received")
                return False

            output_path = TRANSFER_DIR / file_info['filename']

            # Pre-allocate file
            with open(output_path, 'wb') as f:
                f.truncate(file_info['size'])

            logger.info(f"Pre-allocated file: {output_path}")
            logger.info(f"Waiting for server workers to be ready...")
            time.sleep(3)  # Give server workers time to bind ports

            # Distribute chunks among workers
            chunks_per_worker_base = file_info['total'] // self.num_workers
            worker_chunks = []

            for i in range(self.num_workers):
                start_chunk = i * chunks_per_worker_base
                end_chunk = (start_chunk + chunks_per_worker_base
                           if i < self.num_workers - 1 else file_info['total'])
                worker_chunks.append(list(range(start_chunk, end_chunk)))

            # Start workers
            result_queue = mp.Queue()
            workers = []
            start_time = time.time()

            is_windows = platform.system() == 'Windows'
            worker_func = client_worker_windows if is_windows else client_worker_unix

            logger.info("=" * 70)
            logger.info(f"CLIENT STARTING")
            logger.info("=" * 70)
            logger.info(f"Platform: {platform.system()}")
            logger.info(f"Workers: {self.num_workers}")
            logger.info(f"Window size: {self.window_size}")
            logger.info(f"Server: {server_host}")
            logger.info(f"Worker ports: {base_port + 1} to {base_port + 1 + self.num_workers - 1}")
            logger.info("=" * 70)
            logger.info(f"Starting {self.num_workers} workers...")

            for i in range(self.num_workers):
                p = mp.Process(
                    target=worker_func,
                    args=(i, server_host, base_port + 1, worker_chunks[i], str(output_path),
                          file_info['size'], file_info['chunk_size'], self.window_size, result_queue)
                )
                p.start()
                workers.append(p)

            # Wait for completion
            completed = 0
            total_received = 0

            while completed < self.num_workers:
                try:
                    msg = result_queue.get(timeout=2)
                    if msg[0] == 'worker_done':
                        completed += 1
                        total_received += msg[2]
                        logger.info(f"Worker {msg[1]} done: {msg[2]} chunks")
                except:
                    if not any(p.is_alive() for p in workers):
                        break

            for p in workers:
                p.join(timeout=2)

            elapsed = time.time() - start_time
            speed = (file_info['size'] / elapsed) / (1024 * 1024) if elapsed > 0 else 0

            logger.info("=" * 70)
            logger.info(f"TRANSFER COMPLETE")
            logger.info(f"Time: {elapsed:.2f}s")
            logger.info(f"Speed: {speed:.1f} MB/s")
            logger.info(f"Received: {total_received}/{file_info['total']} chunks")
            logger.info(f"Saved: {output_path}")

            # Verify
            actual_size = output_path.stat().st_size
            if actual_size == file_info['size']:
                logger.info("Size: VERIFIED")

                hasher = xxhash.xxh64()
                with open(output_path, 'rb') as f:
                    while chunk := f.read(file_info['chunk_size'] * 4):
                        hasher.update(chunk)

                if hasher.hexdigest() == file_info['hash']:
                    logger.info("Hash: VERIFIED - Transfer successful!")
                else:
                    logger.warning("Hash: MISMATCH - File may be corrupted")
            else:
                logger.warning(f"Size: MISMATCH ({actual_size} vs {file_info['size']})")

            logger.info("=" * 70)

            return total_received == file_info['total']

        except Exception as e:
            logger.error(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return False


def run_server(filepath: str, host: str = "0.0.0.0", port: int = 9000,
               workers: int = None, chunk_size: int = None, window: int = None):
    """Start server with optional config"""
    num_workers = workers or DEFAULT_WORKERS
    chunk_sz = chunk_size or DEFAULT_CHUNK_SIZE
    window_sz = window or DEFAULT_WINDOW_SIZE

    server = ParallelServer(host, port, num_workers, chunk_sz, window_sz)
    server.serve_file(Path(filepath))


def run_client(server_host: str, port: int = 9000, workers: int = None, window: int = None):
    """Start client with optional config"""
    num_workers = workers or DEFAULT_WORKERS
    window_sz = window or DEFAULT_WINDOW_SIZE

    client = ParallelClient(num_workers, window_sz)
    client.receive_file(server_host, port)


if __name__ == "__main__":
    import sys

    mp.set_start_method('spawn', force=True)

    if not ENET_AVAILABLE:
        print("ERROR: pip install pyenet-updated numba numpy xxhash psutil")
        sys.exit(1)

    print("\n" + "=" * 70)
    print("ULTRA-FAST UDP FILE TRANSFER")
    print("=" * 70)
    print("Strategy: UNSEQUENCED packets + Application-level retransmission")
    print("Protocol: Receiver-driven flow control (like RBUDP/FASP)")
    print()
    print("CRITICAL: FIREWALL MUST ALLOW UDP PORTS")
    print("=" * 70)

    if platform.system() == 'Windows':
        print("Windows Firewall:")
        print("  1. Run as Administrator OR")
        print("  2. Add firewall rule:")
        print('     netsh advfirewall firewall add rule name="UDP File Transfer"')
        print('     dir=in action=allow protocol=UDP localport=9000-9020')
        print()
        print("  3. Check if ports are open:")
        print("     netstat -an | findstr :9000")
    else:
        print("Linux Firewall:")
        print("  sudo ufw allow 9000:9020/udp")
        print("  OR: sudo iptables -A INPUT -p udp --dport 9000:9020 -j ACCEPT")
        print()
        print("  Check: sudo netstat -uln | grep 9000")

    print()
    print("TROUBLESHOOTING CONNECTION FAILURES:")
    print("  1. Check server is running FIRST")
    print("  2. Wait 5 seconds after server starts")
    print("  3. Verify ports with netstat command above")
    print("  4. Temporarily disable firewall to test")
    print("  5. Check antivirus isn't blocking UDP")
    print("=" * 70)

    if len(sys.argv) < 2:
        print("\nUSAGE:")
        print("  Server: python script.py server <file> [host] [port] [workers] [chunk_mb] [window]")
        print("  Client: python script.py client <host> [port] [workers] [window]")
        print()
        print("DEFAULTS:")
        print(f"  Workers: {DEFAULT_WORKERS}")
        print(f"  Chunk size: {DEFAULT_CHUNK_SIZE / (1024**2):.0f} MB")
        print(f"  Window: {DEFAULT_WINDOW_SIZE} chunks per worker")
        print()
        print("EXAMPLES:")
        print("  LAN (Gigabit):")
        print("    Server: python script.py server bigfile.bin 0.0.0.0 9000 8 2 128")
        print("    Client: python script.py client 192.168.1.100 9000 8 128")
        print()
        print("  WAN (High latency):")
        print("    Server: python script.py server bigfile.bin 0.0.0.0 9000 16 4 256")
        print("    Client: python script.py client remote.server.com 9000 16 256")
        print()
        print("TUNING:")
        print("  - More workers = higher throughput (but more CPU/memory)")
        print("  - Larger chunks = fewer syscalls (2-4 MB recommended)")
        print("  - Larger window = more in-flight data (128-256 recommended)")
        print("  - Expected speed on Gigabit LAN: 100-300 MB/s")
        print("  - Expected speed on 100Mbps: 10-12 MB/s")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "server":
        if len(sys.argv) < 3:
            print("Error: Specify file to serve")
            sys.exit(1)

        filepath = sys.argv[2]
        host = sys.argv[3] if len(sys.argv) > 3 else "0.0.0.0"
        port = int(sys.argv[4]) if len(sys.argv) > 4 else 9000
        workers = int(sys.argv[5]) if len(sys.argv) > 5 else None
        chunk_mb = int(sys.argv[6]) if len(sys.argv) > 6 else None
        window = int(sys.argv[7]) if len(sys.argv) > 7 else None

        chunk_size = chunk_mb * 1024 * 1024 if chunk_mb else None

        run_server(filepath, host, port, workers, chunk_size, window)

    elif mode == "client":
        if len(sys.argv) < 3:
            print("Error: Specify server host")
            sys.exit(1)

        server_host = sys.argv[2]
        port = int(sys.argv[3]) if len(sys.argv) > 3 else 9000
        workers = int(sys.argv[4]) if len(sys.argv) > 4 else None
        window = int(sys.argv[5]) if len(sys.argv) > 5 else None

        run_client(server_host, port, workers, window)

    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
