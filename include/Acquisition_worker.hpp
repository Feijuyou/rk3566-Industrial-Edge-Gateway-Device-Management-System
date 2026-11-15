#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "Modbus_collector.hpp"
#include "SensorDataQueue.hpp"

// 架构图中的 ① 采集线程：负责周期调度 Modbus 采集，并将数据灌入 SensorDataQueue
class AcquisitionWorker {
public:
    // 构造函数：传入串口路径、从站地址、采集周期（默认 500ms）以及绑定的目标队列
    AcquisitionWorker(std::string device_path,
                      uint8_t slave_id,
                      std::chrono::milliseconds interval_ms,
                      SensorDataQueue& queue);

    // 析构函数：自动安全退出后台线程
    ~AcquisitionWorker();

    AcquisitionWorker(const AcquisitionWorker&) = delete;
    AcquisitionWorker& operator=(const AcquisitionWorker&) = delete;

    // 启动后台采集线程（非阻塞，立即返回）
    bool start();

    // 停止后台采集线程并等待其完全退出
    void stop();

    // 查看底层的采集统计（成功、失败、重连）
    CollectorStats stats() const { return collector_.stats(); }

private:
    // 线程实际运行的工作函数
    void run();

    std::string                device_path_;
    uint8_t                    slave_id_;
    std::chrono::milliseconds interval_;
    SensorDataQueue&           queue_;     // 引用绑定的线程安全队列
    ModbusCollector            collector_; // 底层 Modbus 采集器
    std::atomic<bool>          running_{false};
    std::thread                thread_;
};