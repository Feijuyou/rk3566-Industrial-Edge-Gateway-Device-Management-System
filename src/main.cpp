#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include "Acquisition_worker.hpp"
#include "Mqtt_worker.hpp"
#include "Storage_worker.hpp"

namespace {
volatile std::sig_atomic_t running = 1;
// 信号函数只设置标记，不在其中加锁、输出日志或访问数据库。
void signalHandler(int) { running = 0; }
int runApplication(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "用法: " << argv[0]
                  << " <串口路径|--upload-only> [数据库] [Broker IP] [端口] [网关ID]\n";
        return 1;
    }
    const bool uploadOnly = std::string(argv[1]) == "--upload-only";
    const std::string path = argc > 2 ? argv[2] : "sensor_edge.db";
    MqttConfig config;
    config.host = argc > 3 ? argv[3] : "127.0.0.1";
    if (argc > 4) {
        std::size_t end = 0;
        config.port = std::stoi(argv[4], &end);
        if (argv[4][end] != '\0' || config.port < 1 || config.port > 65535)
            throw std::invalid_argument("Broker 端口必须为 1～65535");
    }
    const std::string gateway = argc > 5 ? argv[5] : "rk3566-01";
    if (gateway.empty() || gateway.find_first_not_of(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")
        != std::string::npos)
        throw std::invalid_argument("网关 ID 仅使用字母、数字、下划线、点和短横线");
    config.clientId = gateway;
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    SensorDataQueue queue(1000);
    StorageWorker storage(queue, path, 10, std::chrono::milliseconds(3000));
    std::unique_ptr<AcquisitionWorker> acquisition;
    MqttWorker mqtt(config, path, gateway);
    std::cout << "模式: " << (uploadOnly ? "仅补传" : "采集 + 落库 + MQTT")
              << " | 数据库: " << path << " | Broker: " << config.host << ':'
              << config.port << "\nTopic: factory/gateway/" << gateway << "/telemetry\n";
    if (!storage.start()) return 1;
    if (!uploadOnly) {
        acquisition = std::make_unique<AcquisitionWorker>(
            argv[1], 1, std::chrono::milliseconds(500), queue);
        if (!acquisition->start()) return 1;
    }
    if (!mqtt.start()) {
        if (acquisition) acquisition->stop();
        queue.stop();
        storage.stop();
        return 1;
    }
    while (running) {
        const auto s = mqtt.stats();
        const auto a = acquisition ? acquisition->stats() : CollectorStats{};
        std::cout << "[心跳] 采集成功=" << a.success_count
                  << " 失败=" << a.fail_count << " 串口重连=" << a.reconnect_count
                  << " 队列=" << queue.size() << " 队列丢弃=" << queue.dropped_count()
                  << " 本次落库=" << storage.total_saved_count()
                  << " MQTT连接=" << s.connected << " 等待确认=" << s.waitingAck
                  << " 提交=" << s.submitted << " PUBACK=" << s.pubacks
                  << " 已标记=" << s.reported << " 发送/连接失败事件=" << s.failures
                  << " 数据库错误=" << s.dbErrors << " 待上报=" << s.pending
                  << " 总记录=" << s.total << " MQTT重连=" << s.reconnects
                  << " 上报线程异常=" << s.faulted << '\n';
        for (int i = 0; i < 20 && running; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // 停止采集 -> 排空并落库 -> 停上报；未上报的历史留给下次启动补传。
    if (acquisition) acquisition->stop();
    queue.stop();
    storage.stop();
    mqtt.stop();
    const auto s = mqtt.stats();
    std::cout << "[退出] 本次落库=" << storage.total_saved_count()
              << " 总记录=" << storage.db_record_count()
              << " 待上报=" << s.pending << " 已确认并标记=" << s.reported
              << " 队列丢弃=" << queue.dropped_count()
              << " 未落库=" << storage.unpersisted_count() << '\n';
    return storage.unpersisted_count() == 0 && !s.faulted ? 0 : 1;
}
}
int main(int argc, char* argv[]) {
    try {
        // 先初始化库，最后才清理；runApplication 内的线程和客户端先销毁。
        MqttRuntime runtime;
        return runApplication(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "[MAIN] " << e.what() << '\n';
        return 1;
    }
}
