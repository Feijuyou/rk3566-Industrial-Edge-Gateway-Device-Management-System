#pragma once

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "Database_manager.hpp"
#include "SensorDataQueue.hpp"

// 架构图中的 ③ 存储线程：消费队列数据，批量打包写入 SQLite
class StorageWorker {
public:
    // 构造函数：指定绑定的队列、数据库文件路径、单批最大条数（默认10条）、最长超时刷新（默认3秒）
    StorageWorker(SensorDataQueue& queue,
                  std::string db_path,
                  size_t batch_size = 10,
                  std::chrono::milliseconds max_wait_ms = std::chrono::milliseconds(3000));
    ~StorageWorker();

    StorageWorker(const StorageWorker&) = delete;
    StorageWorker& operator=(const StorageWorker&) = delete;

    bool start();
    void stop();

    uint64_t total_saved_count() const { return total_saved_count_.load(); }
    int64_t db_record_count() const { return total_records_.load(); }
    uint64_t failed_flushes() const { return failed_flushes_.load(); }
    uint64_t unpersisted_count() const { return unpersisted_.load(); }

private:
    void run();
    bool flush_batch(std::vector<SensorData>& batch);

    SensorDataQueue&           queue_;
    std::string                db_path_;
    const size_t               batch_size_;
    std::chrono::milliseconds  max_wait_ms_;

    DatabaseManager            db_;
    std::atomic<bool>          running_{false};
    std::atomic<uint64_t>      total_saved_count_{0};
    std::atomic<uint64_t> failed_flushes_{0}, unpersisted_{0};
    std::atomic<int64_t> total_records_{-1};
    std::thread                thread_;
};