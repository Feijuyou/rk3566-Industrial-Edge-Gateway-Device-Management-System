#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include "Database_manager.hpp"
#include "Mqtt_work.hpp"

// 计数快照：main 只读取原子值，不跨线程使用数据库连接。
struct MqttStats {
    bool running{false}, connected{false}, faulted{false}, waitingAck{false};
    uint64_t submitted{0}, pubacks{0}, reported{0}, failures{0}, dbErrors{0}, reconnects{0};
    int64_t pending{-1}, total{-1};
};

class MqttWorker {
public:
    MqttWorker(const MqttConfig& config, const std::string& dbPath,
               const std::string& gatewayId);
    ~MqttWorker();
    MqttWorker(const MqttWorker&) = delete;
    MqttWorker& operator=(const MqttWorker&) = delete;
    bool start();
    void stop();
    MqttStats stats() const noexcept;
private:
    void run();
    std::string toJson(const PendingRecord& record) const;
    bool waitFor(std::chrono::milliseconds delay);
    void refreshCounts();
    MqttClient client_;
    DatabaseManager db_; // 上报线程独占连接，与存储连接分离。
    std::string gatewayId_, topic_;
    std::chrono::seconds ackTimeout_;
    unsigned int retryMaxSeconds_;
    std::atomic<bool> running_{false}, faulted_{false}, waitingAck_{false};
    std::atomic<uint64_t> submitted_{0}, pubacks_{0}, reported_{0}, failures_{0}, dbErrors_{0};
    std::atomic<int64_t> pending_{-1}, total_{-1};
    std::thread thread_;
    std::mutex waitMutex_;
    std::condition_variable wake_;
};
