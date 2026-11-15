# RK3566 工业网关：Modbus → SQLite → MQTT

本版本保留原来的采集、队列和存储架构，补齐了 MQTT 上报与 main 集成。
默认采用 MQTT 3.1.1、普通 TCP、QoS 1、非 retained 遥测消息。
适合先在可信局域网开发；本版本尚未实现 MQTT 用户名密码、TLS、配置文件或开机服务。

## 1. 数据流和关键约定

STM32 → AcquisitionWorker → SensorDataQueue → StorageWorker → sensor_history
→ MqttWorker → MqttClient → Broker → PUBACK → MqttWorker → reported=1。

- 只发布已经提交到 SQLite 的记录；采集线程不调用 MQTT。
- StorageWorker 和 MqttWorker 各自拥有一个 DatabaseManager 和连接。
- reported=0 表示尚未获得并完成本地登记的确认，reported=1 表示 PUBACK 后数据库标记成功。
- 每次最多一条在途记录。先保存数据库 id 和 MQTT mid 的对应关系，再处理确认队列。
  即使确认在 publish 返回之前到达，也先保留在队列，业务线程之后再取。
- PUBACK 已到但标记失败时，只重试 UPDATE，不重新发布。
- PUBACK 超时属于“结果未知”。重建会话后保留原业务 ID 补传，因此可能重复。
- 接收端必须用 gateway_id + record_id 去重。PUBACK 不是业务订阅端的持久化确认。
- gateway_id 在同一个数据库生命周期内保持稳定。若删除/重建数据库导致 ID 从头开始，
  应调整网关的数据身份或在接收端处理新的数据代次，避免与旧记录冲突。
- 程序退出不等待所有历史传完；尚未标记的数据下次自动恢复。
- 数据库放在 RK3566 本地磁盘/文件系统上，不能放在 Samba、NFS 等网络共享上运行 WAL。

## 2. 编译依赖

需要 C++17 编译器、CMake >= 3.16、带线程支持的 libmosquitto。
SQLite 和 nlohmann/json 已放在 third_party 中，随本工程编译。

Ubuntu 本机验证可以安装：
    
    sudo apt-get install build-essential cmake libmosquitto-dev python3

然后在 Ubuntu 的项目根目录执行：

    cmake -S . -B build-host -DBUILD_TESTING=ON -DSTATIC_CXX_RUNTIME=OFF
    cmake --build build-host -j4
    ctest --test-dir build-host --output-on-failure

没有 Python 时仍会构建数据库测试；TCP 集成测试可稍后单独运行：

    python3 tests/test_mqtt_integration.py ./build-host/mqtt_test_driver

TCP 测试只绑定本机随机端口，使用临时测试数据库和可控制 PUBACK 的简化 Broker，
不连接真实设备，也不替代与正式 Mosquitto Broker 的兼容性和上板验收。

## 3. RK3566 / Buildroot 交叉编译

本次没有访问到你的实际 Buildroot SDK，下面路径是模板，需要换成真实 SDK。
优先让 Buildroot 自带的 mosquitto 软件包提供目标库，不要链接 Ubuntu 的 x86 库。

在实际 SDK 使用的配置/输出环境中搜索 BR2_PACKAGE_MOSQUITTO：
启用 mosquitto 和线程支持；板子只是客户端时可以关闭 Broker；普通 TCP 不需要 WebSocket。
具体依赖以厂商 SDK 中 package/mosquitto/Config.in 和 mosquitto.mk 为准。

示例工具链配置：

    export RK3566_CROSS_COMPILE=/实际SDK/output/host/bin/aarch64-buildroot-linux-gnu-
    export RK3566_SYSROOT=/实际SDK/output/host/aarch64-buildroot-linux-gnu/sysroot

    cmake -S . -B build-rk3566 \
      -DCMAKE_TOOLCHAIN_FILE=cmake/aarch64-buildroot.cmake \
      -DBUILD_TESTING=OFF
    cmake --build build-rk3566 -j4

应确认目标 sysroot 内同时有 mosquitto.h 和 ARM64 的 libmosquitto，
板端有 libmosquitto.so.1 及其实际依赖。若启用了 TLS，依赖可能包含目标 OpenSSL 库。
nlohmann/json 是头文件库，不需要额外链接 JSON 动态库。

不要复用之前采用其他工具链生成的 build 缓存；使用新的 build-rk3566。
编译后检查：

    file build-rk3566/modbus_app
    readelf -d build-rk3566/modbus_app

输出必须是目标系统兼容的 ELF；CPU 架构、C 库和动态链接器路径都要匹配。
STATIC_CXX_RUNTIME 仅控制 libstdc++/libgcc，不代表整个程序完全静态。

## 4. PC 上运行 Broker

Windows 安装 Mosquitto 后，新建 mosquitto-test.conf：

    listener 1883 0.0.0.0
    allow_anonymous true
    log_dest stdout

以上匿名监听只用于可信局域网开发。确认没有另一个 Broker 占用 1883，
并允许开发板通过 Windows 防火墙访问该 TCP 端口。

在配置文件所在目录，用 PowerShell 启动：

    & 'C:\Program Files\mosquitto\mosquitto.exe' -c .\mosquitto-test.conf -v

另开终端订阅：

    & 'C:\Program Files\mosquitto\mosquitto_sub.exe' -h 127.0.0.1 -t 'factory/gateway/rk3566-01/telemetry' -q 1 -v

开发板连接的是 PC 的局域网 IP，不是 127.0.0.1，也不是开发板自己的 IP。

## 5. 主程序运行

保留原有前两个位置参数，后面新增 Broker 和网关参数：

    ./modbus_app /dev/ttyS3 sensor_edge.db 192.168.1.100 1883 rk3566-01

上面的 IP 是示例，请替换。默认采集从站 1、寄存器 0，每 500ms 采集；
每批 10 条或最长 3 秒存储一次。网关 ID 允许字母、数字、下划线、点和短横线。

程序每 2 秒输出统计；数据库总数和待上报数量为上报线程约每秒更新的缓存快照，
-1 表示暂时未得到有效查询结果。确认计数、已标记计数分别统计。

不接 STM32，只补传已有数据库：

    ./modbus_app --upload-only sensor_edge.db 192.168.1.100 1883 rk3566-01

--upload-only 不产生新采样，会持续运行，按 Ctrl+C 退出。
两个示例都不能同时使用同一个网关 ID 连接同一个 Broker。

退出顺序：
停止采集 → 停止队列并刷完存储 → 停止上报线程 → 销毁 MQTT 客户端 → 清理库。
正常退出不会删除历史，也不会把未确认数据标记成成功。
libmosquitto 的正常线程回收需要等待网络循环退出，不承诺严格毫秒级退出时限。

## 6. 统计字段怎么读

- 提交：客户端库接受的发布次数，可能包含对同一业务记录的重试。
- PUBACK：本次进程收到并匹配到在途记录的协议确认数。
- 已标记：本次进程完成 reported=1 更新的次数。
- 发送/连接失败事件：上报线程遇到的提交失败、启动失败、超时/异常事件，
  不等于丢失的数据条数；普通断网可能由库自动恢复而不增加这个计数。
- MQTT重连：成功连接次数减首次成功连接，启动失败次数不算成功重连。
- 数据库错误：上报线程打开/查询/标记失败的次数。
- 队列丢弃：沿用原队列“满时丢最旧”的策略，并补上可观察计数。
- 未落库：退出时因持续写入故障而仍停留在批次/队列中的数量。

内存有上限：采集队列 1000 条、存储批次最多 10 条、MQTT 在途最多 1 条。
磁盘故障或队列溢出仍可能丢失尚未提交的采样，不能宣称无条件零丢失。
已提交记录不会因 MQTT 断网而从数据库删除。
磁盘满时保存当前批次并退避重试；退出时只做有限的最后处理并明确报告未落库数。
没有自动历史删除策略，请定期监控数据库与 WAL 占用。

## 7. 上板验收

1. 先确认 PC Broker 可订阅，再运行网关，检查 CONNACK=0。
2. 保持联网，核对 JSON 的采集时间、原始值和数据库 id。
3. 断开开发板网络，检查采集/落库继续、reported=0 数量累积。
4. 恢复网络，检查待上报数下降；以数据库 ID 与接收端集合核对。
5. 在有待上报记录时结束并重启程序，检查自动补传。
6. 再做强制终止、长时间离线、满盘和长时间运行测试，分别记录计数。
7. 接收端按业务 ID 去重；收到重复消息是 QoS 1 允许的行为。

只在停止程序或采用 SQLite 正式备份机制后复制数据库；运行中不要只复制 .db 而忽略 WAL。
磁盘故障、数据库损坏、系统时间未校准和真实掉电测试需要在板端另行验证。

## 8. 参考

- [libmosquitto API](https://mosquitto.org/api/files/mosquitto-h.html)
- [Mosquitto 认证说明](https://mosquitto.org/documentation/authentication-methods/)
- [SQLite WAL](https://www.sqlite.org/wal.html)
- [SQLite busy_timeout](https://www.sqlite.org/pragma.html#pragma_busy_timeout)
- [nlohmann/json 集成](https://json.nlohmann.me/integration/)

本次实际验证结果见 TEST_REPORT_MQTT.md
