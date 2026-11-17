#!/usr/bin/env python3
"""引擎级配置切换自测（纯 asyncio，无第三方依赖，直接运行）：

  python3 tests/test_engine_switch.py

覆盖 tests/selftest.py（HTTP 端到端）难以构造的引擎语义：
* 新端口被其他进程占用 -> 保存失败，原监听与原配置保留，在传连接不中断
* 0.0.0.0 <-> 127.0.0.1 同端口监听地址切换成功
* 切换中回退也失败 -> 明确停服并记录原因；之后保存有效配置可恢复
* 成功切换后新连接走新监听，已有连接沿用旧快照完成回执
* 通配监听地址的本机连接地址推导（self_connect_host）
"""
from __future__ import annotations

import asyncio
import os
import socket
import sys
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "proxy"))

from app.engine import (  # noqa: E402
    DelayProxy,
    ProxyConfig,
    self_connect_host,
)

failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    print(f"[{'ok ' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not cond:
        failures.append(name)


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def target_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """模拟接收端：欢迎行 + 读到 EOF 后回执字节数。"""
    try:
        writer.write(b"WELCOME test-target\n")
        await writer.drain()
        total = 0
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break
            total += len(chunk)
        writer.write(f"RECEIPT bytes={total}\n".encode())
        await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def roundtrip(host: str, port: int, data: bytes, timeout: float = 10.0):
    """经代理发 data 后半关闭，返回 (welcome, receipt)；失败抛异常。"""
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
    welcome = await asyncio.wait_for(reader.readline(), timeout)
    writer.write(data)
    await writer.drain()
    writer.write_eof()
    receipt = await asyncio.wait_for(reader.read(), timeout)
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    return welcome, receipt


async def main():
    target_port = free_port()
    target_server = await asyncio.start_server(target_handler, "127.0.0.1", target_port)

    port_a, port_b, port_c = free_port(), free_port(), free_port()
    cfg0 = ProxyConfig(listen_host="127.0.0.1", listen_port=port_a,
                       target_host="127.0.0.1", target_port=target_port,
                       up_delay_ms=50, down_delay_ms=50)
    proxy = DelayProxy(cfg0)
    blocker = None
    try:
        # 0. 初始监听
        r = await proxy.apply_config(proxy.cfg)
        check("initial listen ok", r.ok and proxy.listening, str(r))

        # 1. 新端口被其他进程占用 -> 保存失败，原监听与原配置保留
        blocker = await asyncio.start_server(lambda r_, w_: None, "127.0.0.1", port_b)
        r = await proxy.apply_config(ProxyConfig(
            listen_host="127.0.0.1", listen_port=port_b,
            target_host="127.0.0.1", target_port=target_port,
            up_delay_ms=50, down_delay_ms=50))
        check("occupied port rejected (ok=False)", not r.ok and r.error, str(r.error))
        check("proxy still listening after failed switch",
              r.listening and proxy.listening)
        check("original config kept",
              proxy.cfg.listen_port == port_a and proxy.cfg.listen_host == "127.0.0.1",
              f"{proxy.cfg.listen_host}:{proxy.cfg.listen_port}")
        welcome, receipt = await roundtrip("127.0.0.1", port_a, b"kept-listener")
        check("old listener still serves after failed switch",
              receipt.strip() == f"RECEIPT bytes={len(b'kept-listener')}".encode(),
              receipt.decode(errors="replace"))

        # 2. 同端口换地址：127.0.0.1 -> 0.0.0.0 -> 127.0.0.1（重叠地址切换）
        r = await proxy.apply_config(ProxyConfig(
            listen_host="0.0.0.0", listen_port=port_a,
            target_host="127.0.0.1", target_port=target_port,
            up_delay_ms=50, down_delay_ms=50))
        check("same-port switch 127.0.0.1 -> 0.0.0.0 ok", r.ok and proxy.listening,
              str(r.error))
        check("config updated to 0.0.0.0", proxy.cfg.listen_host == "0.0.0.0")
        r = await proxy.apply_config(ProxyConfig(
            listen_host="127.0.0.1", listen_port=port_a,
            target_host="127.0.0.1", target_port=target_port,
            up_delay_ms=50, down_delay_ms=50))
        check("same-port switch 0.0.0.0 -> 127.0.0.1 ok", r.ok and proxy.listening,
              str(r.error))
        welcome, receipt = await roundtrip("127.0.0.1", port_a, b"after-addr-switch")
        check("listener works after addr switch", receipt.startswith(b"RECEIPT"),
              receipt.decode(errors="replace"))

        # 3. 回退也失败 -> 明确停服并记录原因；之后保存有效配置可恢复
        #    当前监听 0.0.0.0:port_a（上一步切回 127.0.0.1 了，先切到 0.0.0.0）
        await proxy.apply_config(ProxyConfig(
            listen_host="0.0.0.0", listen_port=port_a,
            target_host="127.0.0.1", target_port=target_port,
            up_delay_ms=50, down_delay_ms=50))
        boom = OSError(98, "Address already in use")
        with mock.patch("asyncio.start_server", side_effect=boom):
            r = await proxy.apply_config(ProxyConfig(
                listen_host="127.0.0.1", listen_port=port_a,  # 与 0.0.0.0 重叠 -> 走回退路径
                target_host="127.0.0.1", target_port=target_port,
                up_delay_ms=50, down_delay_ms=50))
        check("rollback failure -> not ok", not r.ok and r.error, str(r.error))
        check("rollback failure -> stopped (not listening)",
              not r.listening and not proxy.listening)
        check("stop reason recorded in listen_error",
              bool(proxy.listen_error) and "停止" in proxy.listen_error,
              str(proxy.listen_error))
        check("config unchanged after stop",
              proxy.cfg.listen_host == "0.0.0.0" and proxy.cfg.listen_port == port_a)
        snap = proxy.snapshot()
        check("snapshot reports stopped + reason",
              snap["listening"] is False and bool(snap["listen_error"]))
        # 停服后保存有效配置可恢复
        r = await proxy.apply_config(ProxyConfig(
            listen_host="127.0.0.1", listen_port=port_a,
            target_host="127.0.0.1", target_port=target_port,
            up_delay_ms=50, down_delay_ms=50))
        check("save valid config recovers listener", r.ok and proxy.listening,
              str(r.error))
        check("listen_error cleared after recovery", proxy.listen_error is None)

        # 4. 成功切换端口：已有连接沿用旧快照完成，新连接走新监听
        #    旧连接：切换前建立，数据进延迟线后挂起（不 EOF）
        reader, writer = await asyncio.open_connection("127.0.0.1", port_a)
        welcome = await asyncio.wait_for(reader.readline(), 10)
        check("old conn got welcome", b"WELCOME" in welcome, welcome.decode())
        old_data = b"old-conn-" + os.urandom(4096)
        writer.write(old_data)
        await writer.drain()
        await asyncio.sleep(0.1)  # 让数据进入延迟线
        r = await proxy.apply_config(ProxyConfig(
            listen_host="127.0.0.1", listen_port=port_c,
            target_host="127.0.0.1", target_port=target_port,
            up_delay_ms=50, down_delay_ms=50))
        check("port switch ok", r.ok and r.restarted, str(r.error))
        check("config switched to new port", proxy.cfg.listen_port == port_c)
        # 在途旧连接完成回执（旧快照）
        writer.write_eof()
        receipt = await asyncio.wait_for(reader.read(), 15)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        check("in-flight conn completes on old snapshot",
              receipt.strip() == f"RECEIPT bytes={len(old_data)}".encode(),
              receipt.decode(errors="replace"))
        # 新连接走新监听
        welcome, receipt = await roundtrip("127.0.0.1", port_c, b"new-listener")
        check("new listener accepts connections", b"WELCOME" in welcome)
        # 旧端口不再服务
        old_serving = True
        try:
            r_, w_ = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port_a), 3)
            w_.close()
            await w_.wait_closed()
        except Exception:
            old_serving = False
        check("old port stopped serving", not old_serving)
        # 新连接快照用的是新配置（listen_port=port_c）
        await asyncio.sleep(0.2)
        infos = list(proxy.history) + [c.info for c in proxy.active.values()]
        new_conn = next((i for i in infos if i.up.bytes == len(b"new-listener")), None)
        check("new conn snapshot uses new config",
              new_conn is not None and new_conn.cfg.listen_port == port_c,
              f"snapshot port={new_conn and new_conn.cfg.listen_port}")
        old_conn = next((i for i in infos if i.up.bytes == len(old_data)), None)
        check("old conn snapshot kept old config",
              old_conn is not None and old_conn.cfg.listen_port == port_a,
              f"snapshot port={old_conn and old_conn.cfg.listen_port}")

        # 5. 非监听字段修改不触发切换
        r = await proxy.apply_config(ProxyConfig(
            listen_host="127.0.0.1", listen_port=port_c,
            target_host="127.0.0.1", target_port=target_port,
            up_delay_ms=500, down_delay_ms=500))
        check("delay-only change ok without restart", r.ok and not r.restarted)
        check("delay applied", proxy.cfg.up_delay_ms == 500)
    finally:
        await proxy.shutdown()
        if blocker is not None:
            blocker.close()
            await blocker.wait_closed()
        target_server.close()
        await target_server.wait_closed()

    # 6. self_connect_host：通配 -> 回环，具体地址原样
    check("self_connect_host 0.0.0.0 -> 127.0.0.1",
          self_connect_host("0.0.0.0") == "127.0.0.1")
    check("self_connect_host :: -> ::1", self_connect_host("::") == "::1")
    check("self_connect_host 127.0.0.1 unchanged",
          self_connect_host("127.0.0.1") == "127.0.0.1")
    check("self_connect_host hostname unchanged",
          self_connect_host("example-host") == "example-host")

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        sys.exit(1)
    print("ALL ENGINE SWITCH CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
