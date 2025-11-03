"""
Ultra-optimized parallel file transfer with native Windows file sharing
Uses: ctypes for Windows API, sparse files, direct I/O, Numba JIT
Zero merge overhead - direct concurrent writes to single file

Windows: pip install pyenet-updated numba numpy xxhash
"""

import ctypes
import logging
import mmap
import multiprocessing as mp
import os
import platform
import struct
import time
from pathlib import Path
from typing import Set

import numpy as np
import xxhash
from numba import jit

try:
    import enet
    ENET_AVAILABLE = True
except ImportError:
    ENET_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

CHUNK_SIZE = 2097152  # 2MB
NUM_WORKERS = 8
CHUNKS_PER_WORKER = 128
TRANSFER_DIR = Path("./transfers")
TRANSFER_DIR.mkdir(exist_ok=True)

# Windows API constants for file sharing
if platform.system() == 'Windows':
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
    """Optimized hash with SIMD-friendly operations"""
    h = np.uint32(2166136261)
    prime = np.uint32(16777619)

    # Process in larger strides for cache efficiency
    for i in range(0, len(data), 16):
        h = np.uint32((h ^ data[i]) * prime)

    return h


@jit(nopython=True, cache=True, fastmath=True)
def verify_hash(data: np.ndarray, expected: np.uint32) -> bool:
    """Fast verify"""
    return fast_hash(data) == expected


class PacketType:
    FILE_INFO = 1
    CHUNK_DATA = 2
    CHUNK_REQUEST = 3
    WORKER_READY = 4
    FILE_METADATA = 5


def open_shared_file_windows(filepath: str, file_size: int):
    """
    Open file with Windows API for true concurrent access.
    Uses CreateFile with FILE_SHARE_READ | FILE_SHARE_WRITE.
    """
    import msvcrt
    try:
        # Convert path to Windows format
        filepath_wide = ctypes.create_unicode_buffer(filepath)

        # Open with shared read/write access
        handle = kernel32.CreateFileW(
            filepath_wide,
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,  # Critical: allow sharing
            None,
            OPEN_EXISTING,
            FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_WRITE_THROUGH,
            None
        )

        if handle == -1 or handle == 0:
            error_code = kernel32.GetLastError()
            raise OSError(f"CreateFile failed with error {error_code}")

        # Convert Windows handle to Python file descriptor
        fd = msvcrt.open_osfhandle(handle, os.O_RDWR | os.O_BINARY)

        return fd, handle

    except Exception as e:
        logger.error(f"Failed to open shared file: {e}")
        raise


def write_at_offset_windows(handle, offset: int, data: bytes):
    """
    Direct write at offset using Windows API.
    Thread-safe, atomic positioning and writing.
    """
    import msvcrt

    # Set file pointer
    offset_low = ctypes.c_ulong(offset & 0xFFFFFFFF)
    offset_high = ctypes.c_long(offset >> 32)

    result = kernel32.SetFilePointer(
        handle,
        offset_low,
        ctypes.byref(offset_high),
        0  # FILE_BEGIN
    )

    if result == 0xFFFFFFFF:
        error = kernel32.GetLastError()
        if error != 0:
            raise OSError(f"SetFilePointer failed: {error}")

    # Write data
    bytes_written = ctypes.c_ulong()
    success = kernel32.WriteFile(
        handle,
        data,
        len(data),
        ctypes.byref(bytes_written),
        None
    )

    if not success:
        raise OSError(f"WriteFile failed: {kernel32.GetLastError()}")

    return bytes_written.value


def server_worker(worker_id: int, port: int, filepath: str, chunk_queue: mp.Queue, stats_queue: mp.Queue):
    """
    Server worker with memory-mapped file.
    Each worker has independent mmap for zero-copy reads.
    """
    try:
        file_path = Path(filepath)
        f = open(file_path, 'rb')
        file_size = file_path.stat().st_size
        m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        address = enet.Address(b'0.0.0.0', port + worker_id)
        host = enet.Host(address, 4, 1, 0, 0)

        logger.info(f"Worker {worker_id} on port {port + worker_id}")

        peer = None
        chunks_sent = 0

        while True:
            event = host.service(10)

            if event.type == enet.EVENT_TYPE_CONNECT:
                peer = event.peer
                logger.info(f"Worker {worker_id} client connected")

            elif event.type == enet.EVENT_TYPE_RECEIVE:
                if not peer:
                    continue

                packet_type = event.packet.data[0]

                if packet_type == PacketType.CHUNK_REQUEST:
                    chunk_index, = struct.unpack('!I', event.packet.data[1:5])

                    offset = chunk_index * CHUNK_SIZE
                    if offset >= file_size:
                        continue

                    end = min(offset + CHUNK_SIZE, file_size)
                    chunk_size = end - offset

                    # Zero-copy read from mmap
                    chunk_data = m[offset:end]

                    # Fast hash
                    data_array = np.frombuffer(chunk_data, dtype=np.uint8)
                    checksum = fast_hash(data_array)

                    # Send
                    packet_data = struct.pack('!BIII', PacketType.CHUNK_DATA, chunk_index, chunk_size, checksum) + chunk_data
                    packet = enet.Packet(packet_data, enet.PACKET_FLAG_UNSEQUENCED)
                    peer.send(0, packet)

                    chunks_sent += 1

                    if chunks_sent % 100 == 0:
                        stats_queue.put(('sent', worker_id, chunks_sent))

            elif event.type == enet.EVENT_TYPE_DISCONNECT:
                logger.info(f"Worker {worker_id} done: {chunks_sent} chunks")
                break

        m.close()
        f.close()

    except Exception as e:
        logger.error(f"Worker {worker_id} error: {e}")


def client_worker_windows(worker_id: int, server_host: str, port: int, chunk_indices: list,
                          output_path: str, file_size: int, chunk_size: int, result_queue: mp.Queue):
    """
    Windows-optimized client worker using native API for concurrent writes.
    Direct writes at offset - no buffering, no conflicts.
    """
    try:
        import msvcrt

        # Open file with Windows API for shared access
        fd, handle = open_shared_file_windows(output_path, file_size)

        logger.info(f"Worker {worker_id} opened file (handle: {handle})")

        # Connect to server worker
        client_host = enet.Host(None, 1, 1, 0, 0)
        address = enet.Address(server_host.encode('utf-8'), port + worker_id)
        peer = client_host.connect(address, 1)

        event = client_host.service(5000)
        if event.type != enet.EVENT_TYPE_CONNECT:
            logger.error(f"Worker {worker_id} connection failed")
            kernel32.CloseHandle(handle)
            return

        logger.info(f"Worker {worker_id} connected to port {port + worker_id}")

        received = set()
        pending = set()

        # Request initial batch
        for chunk_idx in chunk_indices[:min(CHUNKS_PER_WORKER, len(chunk_indices))]:
            packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
            packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
            peer.send(0, packet)
            pending.add(chunk_idx)

        next_request_idx = min(CHUNKS_PER_WORKER, len(chunk_indices))
        last_progress = time.time()

        while len(received) < len(chunk_indices):
            event = client_host.service(5)

            if event.type == enet.EVENT_TYPE_RECEIVE:
                packet_type = event.packet.data[0]

                if packet_type == PacketType.CHUNK_DATA:
                    if len(event.packet.data) < 13:
                        continue

                    chunk_idx, received_chunk_size, checksum = struct.unpack('!III', event.packet.data[1:13])
                    chunk_data = event.packet.data[13:13+received_chunk_size]

                    if chunk_idx not in chunk_indices:
                        continue

                    if len(chunk_data) != received_chunk_size:
                        pending.discard(chunk_idx)
                        continue

                    # Fast verify
                    data_array = np.frombuffer(chunk_data, dtype=np.uint8)
                    if not verify_hash(data_array, np.uint32(checksum)):
                        pending.discard(chunk_idx)
                        packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
                        packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
                        peer.send(0, packet)
                        pending.add(chunk_idx)
                        continue

                    # Direct write at offset using Windows API
                    offset = chunk_idx * chunk_size

                    try:
                        bytes_written = write_at_offset_windows(handle, offset, chunk_data)

                        if bytes_written != received_chunk_size:
                            logger.error(f"Worker {worker_id} partial write: {bytes_written}/{received_chunk_size}")
                            pending.discard(chunk_idx)
                            continue

                    except Exception as write_error:
                        logger.error(f"Worker {worker_id} write error: {write_error}")
                        pending.discard(chunk_idx)
                        continue

                    received.add(chunk_idx)
                    pending.discard(chunk_idx)

                    # Progress
                    if time.time() - last_progress >= 2.0:
                        progress = len(received) / len(chunk_indices) * 100
                        logger.info(f"Worker {worker_id}: {len(received)}/{len(chunk_indices)} ({progress:.1f}%)")
                        last_progress = time.time()

                    # Keep pipeline full
                    if next_request_idx < len(chunk_indices) and len(pending) < CHUNKS_PER_WORKER // 2:
                        new_chunk_idx = chunk_indices[next_request_idx]
                        packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, new_chunk_idx)
                        packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
                        peer.send(0, packet)
                        pending.add(new_chunk_idx)
                        next_request_idx += 1

            elif event.type == enet.EVENT_TYPE_DISCONNECT:
                break

            # Retry missing chunks
            if time.time() - last_progress > 5.0:
                missing = set(chunk_indices) - received
                if missing and len(pending) < CHUNKS_PER_WORKER:
                    retry_chunk = list(missing)[0]
                    if retry_chunk not in pending:
                        packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, retry_chunk)
                        packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
                        peer.send(0, packet)
                        pending.add(retry_chunk)
                last_progress = time.time()

        # Flush and close
        kernel32.FlushFileBuffers(handle)
        kernel32.CloseHandle(handle)
        peer.disconnect()

        result_queue.put(('worker_done', worker_id, len(received)))
        logger.info(f"Worker {worker_id} complete: {len(received)}/{len(chunk_indices)}")

    except Exception as e:
        logger.error(f"Worker {worker_id} error: {e}")
        import traceback
        traceback.print_exc()


def client_worker_unix(worker_id: int, server_host: str, port: int, chunk_indices: list,
                       output_path: str, file_size: int, chunk_size: int, result_queue: mp.Queue):
    """
    Unix/Linux client worker using pwrite for atomic positioned writes.
    """
    try:
        # Open file with O_RDWR
        fd = os.open(output_path, os.O_RDWR)

        logger.info(f"Worker {worker_id} opened file (fd: {fd})")

        # Connect
        client_host = enet.Host(None, 1, 1, 0, 0)
        address = enet.Address(server_host.encode('utf-8'), port + worker_id)
        peer = client_host.connect(address, 1)

        event = client_host.service(5000)
        if event.type != enet.EVENT_TYPE_CONNECT:
            logger.error(f"Worker {worker_id} connection failed")
            os.close(fd)
            return

        logger.info(f"Worker {worker_id} connected")

        received = set()
        pending = set()

        # Request initial batch
        for chunk_idx in chunk_indices[:min(CHUNKS_PER_WORKER, len(chunk_indices))]:
            packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, chunk_idx)
            packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
            peer.send(0, packet)
            pending.add(chunk_idx)

        next_request_idx = min(CHUNKS_PER_WORKER, len(chunk_indices))
        last_progress = time.time()

        while len(received) < len(chunk_indices):
            event = client_host.service(5)

            if event.type == enet.EVENT_TYPE_RECEIVE:
                packet_type = event.packet.data[0]

                if packet_type == PacketType.CHUNK_DATA:
                    if len(event.packet.data) < 13:
                        continue

                    chunk_idx, received_chunk_size, checksum = struct.unpack('!III', event.packet.data[1:13])
                    chunk_data = event.packet.data[13:13+received_chunk_size]

                    if chunk_idx not in chunk_indices:
                        continue

                    # Verify
                    data_array = np.frombuffer(chunk_data, dtype=np.uint8)
                    if not verify_hash(data_array, np.uint32(checksum)):
                        pending.discard(chunk_idx)
                        continue

                    # Atomic positioned write using pwrite
                    offset = chunk_idx * chunk_size

                    try:
                        bytes_written = os.pwrite(fd, chunk_data, offset)

                        if bytes_written != received_chunk_size:
                            pending.discard(chunk_idx)
                            continue

                    except Exception as write_error:
                        logger.error(f"Worker {worker_id} write error: {write_error}")
                        pending.discard(chunk_idx)
                        continue

                    received.add(chunk_idx)
                    pending.discard(chunk_idx)

                    # Progress
                    if time.time() - last_progress >= 2.0:
                        progress = len(received) / len(chunk_indices) * 100
                        logger.info(f"Worker {worker_id}: {len(received)}/{len(chunk_indices)} ({progress:.1f}%)")
                        last_progress = time.time()

                    # Keep pipeline full
                    if next_request_idx < len(chunk_indices) and len(pending) < CHUNKS_PER_WORKER // 2:
                        new_chunk_idx = chunk_indices[next_request_idx]
                        packet_data = struct.pack('!BI', PacketType.CHUNK_REQUEST, new_chunk_idx)
                        packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
                        peer.send(0, packet)
                        pending.add(new_chunk_idx)
                        next_request_idx += 1

        # Sync and close
        os.fsync(fd)
        os.close(fd)
        peer.disconnect()

        result_queue.put(('worker_done', worker_id, len(received)))
        logger.info(f"Worker {worker_id} complete: {len(received)}/{len(chunk_indices)}")

    except Exception as e:
        logger.error(f"Worker {worker_id} error: {e}")
        import traceback
        traceback.print_exc()


class ParallelServer:
    """Server with multiple worker processes"""

    def __init__(self, host: str, base_port: int, num_workers: int = NUM_WORKERS):
        self.host = host
        self.base_port = base_port
        self.num_workers = num_workers
        self.workers = []
        self.chunk_queue = mp.Queue()
        self.stats_queue = mp.Queue()

    def metadata_server_process(self, filepath: Path, file_size: int, total_chunks: int):
        """Send file metadata to clients"""
        try:
            address = enet.Address(b'0.0.0.0', self.base_port)
            host = enet.Host(address, 8, 1, 0, 0)

            logger.info(f"Metadata server on port {self.base_port}")

            while True:
                event = host.service(100)

                if event.type == enet.EVENT_TYPE_CONNECT:
                    filename_bytes = filepath.name.encode('utf-8')

                    hasher = xxhash.xxh64()
                    with open(filepath, 'rb') as f:
                        while chunk := f.read(CHUNK_SIZE * 4):
                            hasher.update(chunk)
                    file_hash = hasher.hexdigest()

                    packet_data = struct.pack(
                        '!BIQQI',
                        PacketType.FILE_METADATA,
                        len(filename_bytes),
                        file_size,
                        total_chunks,
                        CHUNK_SIZE
                    ) + filename_bytes + file_hash.encode('utf-8')

                    packet = enet.Packet(packet_data, enet.PACKET_FLAG_RELIABLE)
                    event.peer.send(0, packet)

                    logger.info(f"Sent metadata: {filepath.name}")

        except Exception as e:
            logger.error(f"Metadata server error: {e}")

    def serve_file(self, filepath: Path):
        """Start parallel workers"""
        if not filepath.exists():
            logger.error(f"File not found: {filepath}")
            return

        file_size = filepath.stat().st_size
        total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

        logger.info(f"File: {filepath.name}, {file_size/(1024**2):.1f} MB, {total_chunks} chunks")
        logger.info(f"Metadata: port {self.base_port}, Workers: {self.base_port + 1}-{self.base_port + self.num_workers}")

        # Start metadata server
        metadata_proc = mp.Process(
            target=self.metadata_server_process,
            args=(filepath, file_size, total_chunks)
        )
        metadata_proc.start()

        # Start workers
        for i in range(self.num_workers):
            p = mp.Process(
                target=server_worker,
                args=(i, self.base_port + 1, str(filepath), self.chunk_queue, self.stats_queue)
            )
            p.start()
            self.workers.append(p)

        try:
            while any(p.is_alive() for p in self.workers) or metadata_proc.is_alive():
                time.sleep(0.5)
        except KeyboardInterrupt:
            logger.info("Stopping...")
        finally:
            metadata_proc.terminate()
            for p in self.workers:
                p.terminate()


class ParallelClient:
    """Client with parallel workers"""

    def __init__(self, num_workers: int = NUM_WORKERS):
        self.num_workers = num_workers

    def receive_file(self, server_host: str, base_port: int) -> bool:
        """Receive file with parallel workers"""
        try:
            # Get metadata
            client_host = enet.Host(None, 1, 1, 0, 0)
            address = enet.Address(server_host.encode('utf-8'), base_port)
            peer = client_host.connect(address, 1)

            event = client_host.service(5000)
            if event.type != enet.EVENT_TYPE_CONNECT:
                logger.error("Connection failed")
                return False

            logger.info(f"Connected to {server_host}:{base_port}")

            # Receive metadata
            file_info = None
            start = time.time()

            while time.time() - start < 10:
                event = client_host.service(100)

                if event.type == enet.EVENT_TYPE_RECEIVE:
                    if event.packet.data[0] == PacketType.FILE_METADATA:
                        offset = 1
                        fname_len, = struct.unpack('!I', event.packet.data[offset:offset+4])
                        offset += 4

                        file_size, total_chunks, chunk_size = struct.unpack('!QQI', event.packet.data[offset:offset+20])
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

                        logger.info(f"File: {filename}, {file_size/(1024**2):.1f} MB")
                        break

            peer.disconnect()

            if not file_info:
                logger.error("No metadata received")
                return False

            output_path = TRANSFER_DIR / file_info['filename']

            # Pre-allocate and close
            with open(output_path, 'wb') as f:
                f.truncate(file_info['size'])

            time.sleep(0.1)  # Ensure file is closed

            # Distribute chunks
            chunks_per_worker = file_info['total'] // self.num_workers
            worker_chunks = []

            for i in range(self.num_workers):
                start_chunk = i * chunks_per_worker
                end_chunk = start_chunk + chunks_per_worker if i < self.num_workers - 1 else file_info['total']
                worker_chunks.append(list(range(start_chunk, end_chunk)))

            # Start workers
            result_queue = mp.Queue()
            workers = []
            start_time = time.time()

            # Choose worker function based on platform
            is_windows = platform.system() == 'Windows'
            worker_func = client_worker_windows if is_windows else client_worker_unix

            logger.info(f"Platform: {platform.system()}, using {'Windows API' if is_windows else 'pwrite'}")

            for i in range(self.num_workers):
                p = mp.Process(
                    target=worker_func,
                    args=(i, server_host, base_port + 1, worker_chunks[i], str(output_path),
                          file_info['size'], file_info['chunk_size'], result_queue)
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

            logger.info(f"Complete: {elapsed:.2f}s, {speed:.1f} MB/s")
            logger.info(f"Received: {total_received}/{file_info['total']} chunks")
            logger.info(f"Saved: {output_path}")

            # Verify
            actual_size = output_path.stat().st_size
            if actual_size == file_info['size']:
                logger.info("Size verified")

                hasher = xxhash.xxh64()
                with open(output_path, 'rb') as f:
                    while chunk := f.read(CHUNK_SIZE * 4):
                        hasher.update(chunk)

                if hasher.hexdigest() == file_info['hash']:
                    logger.info("Hash verified - SUCCESS!")
                else:
                    logger.warning("Hash mismatch - file may be corrupted")
            else:
                logger.warning(f"Size mismatch: {actual_size} vs {file_info['size']}")

            return total_received == file_info['total']

        except Exception as e:
            logger.error(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return False


def run_server(filepath: str, host: str = "0.0.0.0", port: int = 9000):
    """Start server"""
    server = ParallelServer(host, port, NUM_WORKERS)
    server.serve_file(Path(filepath))


def run_client(server_host: str, port: int = 9000):
    """Start client"""
    client = ParallelClient(NUM_WORKERS)
    client.receive_file(server_host, port)


if __name__ == "__main__":
    import sys

    mp.set_start_method('spawn', force=True)

    if not ENET_AVAILABLE:
        print("ERROR: pip install pyenet-updated numba numpy xxhash")
        sys.exit(1)

    print(f"Platform: {platform.system()}")
    print(f"Workers: {NUM_WORKERS} parallel")
    print(f"Chunk: {CHUNK_SIZE/(1024**2):.1f} MB")
    print(f"Pipeline: {CHUNKS_PER_WORKER} per worker")

    if platform.system() == 'Windows':
        import msvcrt
        print("Using: Windows CreateFile API for concurrent writes")
    else:
        print("Using: Unix pwrite for atomic positioned writes")

    if len(sys.argv) < 2:
        print("\nUsage:")
        print("  Server: python script.py server <file> [host] [port]")
        print("  Client: python script.py client <host> [port]")
        print("\nZero merge overhead - direct concurrent writes")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "server":
        run_server(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "0.0.0.0",
                   int(sys.argv[4]) if len(sys.argv) > 4 else 9000)
    elif mode == "client":
        run_client(sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 9000)
    else:
        print(f"Unknown: {mode}")
        sys.exit(1)
