# MQTT 集成验证记录

日期：2026-11-14，Asia/Hong_Kong。

## 实际执行

- Windows 本机 MinGW GCC/G++ 13.1.0，C++17，CMake/Ninja。
- 官方 Mosquitto 2.0.22 客户端源码，TCP、线程支持开启、TLS 关闭；
  本机静态测试构建使用 MinGW Winpthreads，不安装系统服务。
- 使用项目自带 SQLite 和 nlohmann/json 3.12.0。
- gateway_core、database_test、mqtt_test_driver 编译通过。
- main.cpp、Acquisition_worker.cpp 的独立编译检查通过。
- 最终 CTest 统一入口：2/2 测试组通过，总用时 31.54 秒。
- 数据库/存储组通过，TCP 集成组中的 7 个场景全部通过。

## 数据库与存储

通过：缺少 reported 的旧表迁移、64 位时间戳与无符号字段读取、空结果、
非法 limit、重复标记、不存在的 ID、批量事务、写锁冲突、关闭连接错误、
停止时刷入不足一批的数据、注入写入失败后内存限额与未落库/丢弃计数。

写入错误通过 SQLite 触发器注入；这不是板端真实磁盘满或掉电实验。

## TCP 集成

1. 100 条 JSON：逐条校验 record_id、采集时间、原始值、工程值、Topic、QoS 和 retain，
   最终 100 条都获得确认并标记。
2. 不发 PUBACK：数据库始终未标记；正常退出并重启后恢复补传。
3. 启动时 Broker 离线：待上报记录保留，Broker 恢复后全部上报。
4. PUBACK 超时：保持 reported=0，重建会话后同一业务 ID 重发，验证重复是允许的。
5. 先锁数据库再发送 PUBACK：不重复发布，只重试标记，解锁后成功。
6. 断开连接期间追加数据库记录：恢复后数据库全部记录 ID 均被接收。
7. 强制终止测试进程：重启后从 SQLite 补传未确认数据。

Broker 是绑定本机回环随机端口的可控 MQTT 3.1.1 测试实现，
测试使用真实 TCP 和真实 libmosquitto；不是正式 PC Mosquitto Broker 验收。

## 尚未验证

- 虚拟机 SSH 免密登录不可用，未执行 Ubuntu 中的完整 Linux 主程序链接。
- 未取得实际 Buildroot SDK/sysroot，未完成本次修改后的 ARM64 交叉编译。
- 未在 RK3566 上运行，未操作真实串口、STM32、网线或 Wi-Fi。
- 未做真实满盘、板端掉电、TLS、认证、长时间性能和内存稳定性测试。

这些限制不影响已通过的本机核心测试，但不能据此宣称已经完成上板验收。
请按 README_MQTT.md 中的交叉编译与上板步骤继续验证。
