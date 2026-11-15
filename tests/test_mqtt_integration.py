#!/usr/bin/env python3
"""通过真实 TCP + libmosquitto 验证 QoS 1 状态机；不连接真实设备。
测试 Broker 仅实现本用例所需的 MQTT 3.1.1 子集，不替代正式 Broker 兼容性验收。
用法：python3 tests/test_mqtt_integration.py /绝对路径/mqtt_test_driver
"""
import json
import pathlib
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import time


def wait_until(predicate, seconds=10):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.025)
    raise AssertionError("等待条件超时")


def read_exact(sock, size):
    out = b""
    while len(out) < size:
        data = sock.recv(size - len(out))
        if not data:
            raise EOFError()
        out += data
    return out


class Broker:
    """可控制是否发送 PUBACK、何时发送，以及模拟网络断开。"""
    def __init__(self, port=0, ack=True, gate=None):
        self.ack = ack
        self.gate = gate
        self.messages = []
        self.errors = []
        self.clients = []
        self.threads = []
        self.stopped = threading.Event()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", port))
        self.port = self.listener.getsockname()[1]
        self.listener.listen()
        self.listener.settimeout(0.2)
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stopped.is_set():
            try:
                sock, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self.clients.append(sock)
            t = threading.Thread(target=self.handle, args=(sock,), daemon=True)
            self.threads.append(t)
            t.start()

    def handle(self, sock):
        try:
            while not self.stopped.is_set():
                first = read_exact(sock, 1)[0]
                remaining, multiplier = 0, 1
                while True:
                    digit = read_exact(sock, 1)[0]
                    remaining += (digit & 127) * multiplier
                    if digit < 128:
                        break
                    multiplier *= 128
                    assert multiplier <= 128 ** 3
                data = read_exact(sock, remaining)
                kind = first >> 4
                if kind == 1:  # CONNECT -> 成功的 CONNACK
                    sock.sendall(b"\x20\x02\x00\x00")
                elif kind == 3:
                    topic_len = struct.unpack("!H", data[:2])[0]
                    topic = data[2:2 + topic_len].decode()
                    offset = 2 + topic_len
                    assert (first >> 1) & 3 == 1, "必须使用 QoS 1"
                    assert not first & 1, "遥测默认不能 retain"
                    mid = struct.unpack("!H", data[offset:offset + 2])[0]
                    message = json.loads(data[offset + 2:])
                    assert topic == "factory/gateway/test-gateway/telemetry"
                    assert message["gateway_id"] == "test-gateway"
                    self.messages.append(message)
                    if self.gate is not None:
                        while not self.gate.wait(0.05):
                            if self.stopped.is_set():
                                return
                    if self.ack:
                        sock.sendall(b"\x40\x02" + struct.pack("!H", mid))
                elif kind == 12:
                    sock.sendall(b"\xd0\x00")  # PINGRESP
                elif kind == 14:
                    return
                else:
                    raise AssertionError("未预期的报文类型 " + str(kind))
        except (OSError, EOFError):
            pass
        except Exception as error:
            self.errors.append(str(error))
        finally:
            sock.close()

    def close(self):
        self.stopped.set()
        self.listener.close()
        for sock in self.clients:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.thread.join(2)
        for thread in self.threads:
            thread.join(2)
        assert not self.errors, self.errors


DRIVER = str(pathlib.Path(sys.argv[1]).resolve())
ROOT = pathlib.Path(tempfile.mkdtemp(prefix="rk3566-mqtt-tests-"))
PROCESSES = []
BROKERS = []


def broker(*args, **kwargs):
    b = Broker(*args, **kwargs)
    BROKERS.append(b)
    return b


def seed(name, count):
    db = ROOT / (name + ".db")
    subprocess.run([DRIVER, "seed", str(db), str(count)], check=True,
                   capture_output=True, timeout=10)
    return db


def launch(db, port, seconds=12, ack_timeout=30):
    # 使用文件接收日志，避免进程输出较多时堵满 PIPE。
    log = open(ROOT / (db.stem + "-" + str(len(PROCESSES)) + ".log"), "w+b")
    p = subprocess.Popen([DRIVER, "run", str(db), "127.0.0.1", str(port),
                          str(seconds), str(ack_timeout)], stdout=log, stderr=log)
    p.logfile = log
    PROCESSES.append(p)
    return p


def finish(p, seconds=18):
    p.wait(timeout=seconds)
    p.logfile.seek(0)
    text = p.logfile.read().decode("utf-8", errors="replace")
    assert p.returncode == 0, text
    lines = [line for line in text.splitlines() if line.startswith("{")]
    assert lines, text
    return json.loads(lines[-1])


def pending(db):
    with sqlite3.connect(db, timeout=5) as conn:
        return conn.execute("SELECT COUNT(*) FROM sensor_history WHERE reported=0").fetchone()[0]


def case(name, action):
    action()
    print("PASS", name, flush=True)


def normal():
    db = seed("normal", 100)
    b = broker()
    result = finish(launch(db, b.port))
    assert result["reported"] == 100 and result["pubacks"] == 100 and pending(db) == 0
    assert {m["record_id"] for m in b.messages} == set(range(1, 101))
    for message in b.messages:
        i = message["record_id"] - 1
        assert message["timestamp_ms"] == 1794500000000 + i
        assert message["raw_value"] == 1000 + i
        assert message["slave_id"] == 1 and message["register"] == 0
        assert abs(message["value"] - (1000 + i) * 0.1) < 1e-9
    b.close()


def no_ack():
    db = seed("no_ack", 3)
    b = broker(ack=False)
    result = finish(launch(db, b.port, seconds=2))
    assert result["submitted"] == 1 and result["reported"] == 0 and pending(db) == 3
    b.close()
    b2 = broker()
    result = finish(launch(db, b2.port))
    assert result["reported"] == 3 and pending(db) == 0
    assert b.messages[0]["record_id"] == b2.messages[0]["record_id"] == 1
    b2.close()


def startup_offline():
    db = seed("startup_offline", 5)
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    p = launch(db, port)
    time.sleep(1.2)
    assert p.poll() is None and pending(db) == 5
    b = broker(port)
    result = finish(p)
    assert result["reported"] == 5 and pending(db) == 0
    b.close()


def ack_timeout():
    db = seed("ack_timeout", 3)
    b = broker(ack=False)
    p = launch(db, b.port, seconds=12, ack_timeout=2)
    wait_until(lambda: len(b.messages) >= 2, 8)
    assert pending(db) == 3  # 提交/超时均不能标记成功。
    b.ack = True
    result = finish(p)
    assert pending(db) == 0 and result["failures"] >= 1
    assert sum(m["record_id"] == 1 for m in b.messages) >= 2
    b.close()


def ack_before_busy_update():
    db = seed("ack_then_busy", 1)
    gate = threading.Event()
    b = broker(gate=gate)
    p = launch(db, b.port)
    wait_until(lambda: len(b.messages) == 1)
    conn = sqlite3.connect(db, timeout=5)
    conn.execute("BEGIN IMMEDIATE")
    gate.set()  # 在数据库已被锁住后才发 PUBACK。
    time.sleep(1.8)
    assert p.poll() is None and len(b.messages) == 1
    assert conn.execute("SELECT reported FROM sensor_history").fetchone()[0] == 0
    conn.rollback()
    conn.close()
    result = finish(p)
    assert result["db_errors"] >= 1 and result["pubacks"] == 1
    assert result["reported"] == 1 and len(b.messages) == 1 and pending(db) == 0
    b.close()


def outage_and_new_rows():
    db = seed("outage", 40)
    b = broker()
    p = launch(db, b.port, seconds=16)
    wait_until(lambda: len(b.messages) >= 5)
    port = b.port
    b.close()
    time.sleep(0.4)
    assert p.poll() is None
    # 模拟存储线程在断网期间继续提交采样。
    with sqlite3.connect(db, timeout=5) as conn:
        conn.execute("INSERT INTO sensor_history "
                     "(timestamp_ms,slave_id,reg_addr,raw_value,scaled_value,reported) "
                     "VALUES(1794500009999,1,0,2222,222.2,0)")
    assert pending(db) > 0
    b2 = broker(port)
    result = finish(p, seconds=20)
    assert pending(db) == 0 and result["reported"] == 41
    all_ids = {m["record_id"] for m in b.messages + b2.messages}
    assert all_ids == set(range(1, 42))
    b2.close()


def crash_recovery():
    db = seed("crash", 3)
    b = broker(ack=False)
    p = launch(db, b.port, seconds=20)
    wait_until(lambda: len(b.messages) == 1)
    p.kill()  # 只终止本测试创建并持有的进程。
    p.wait(5)
    assert pending(db) == 3
    b.close()
    b2 = broker()
    result = finish(launch(db, b2.port))
    assert result["reported"] == 3 and pending(db) == 0
    assert b2.messages[0]["record_id"] == b.messages[0]["record_id"] == 1
    b2.close()


try:
    case("100条 JSON、QoS1、PUBACK 后标记", normal)
    case("无 PUBACK 不标记，退出后重启补传", no_ack)
    case("启动时 Broker 离线，恢复后补传", startup_offline)
    case("PUBACK 超时保留记录并允许重复补传", ack_timeout)
    case("确认已到但数据库忙，只重试标记不重新发布", ack_before_busy_update)
    case("断网期间继续入库，恢复后补传全部 ID", outage_and_new_rows)
    case("强制终止后的未确认数据恢复", crash_recovery)
    print("ALL MQTT TESTS PASSED; artifacts:", ROOT, flush=True)
finally:
    for p in PROCESSES:
        if p.poll() is None:
            p.kill()
            p.wait(5)
        p.logfile.close()
    for b in BROKERS:
        if not b.stopped.is_set():
            b.close()
