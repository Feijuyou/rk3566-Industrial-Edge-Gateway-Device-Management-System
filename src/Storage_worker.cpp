#include "Storage_worker.hpp"
#include <iostream>
#include <utility>

using Clock = std::chrono::steady_clock;

StorageWorker::StorageWorker(SensorDataQueue& queue, std::string path,
                             size_t batch_size, std::chrono::milliseconds max_wait_ms)
    : queue_(queue), db_path_(std::move(path)), batch_size_(batch_size),
      max_wait_ms_(max_wait_ms), db_(db_path_) {}
StorageWorker::~StorageWorker() { stop(); }

bool StorageWorker::start() {
    if (thread_.joinable()) return running_.load();
    if (batch_size_ == 0 || !db_.open()) return false;
    total_records_.store(db_.get_total_records());
    running_.store(true);
    try { thread_ = std::thread(&StorageWorker::run, this); }
    catch (const std::exception& e) {
        running_.store(false);
        db_.close();
        std::cerr << "[StorageWorker] 创建线程失败: " << e.what() << '\n';
        return false;
    }
    return true;
}
void StorageWorker::stop() {
    running_.store(false);
    if (thread_.joinable()) thread_.join();
    db_.close();
}
bool StorageWorker::flush_batch(std::vector<SensorData>& batch) {
    if (batch.empty()) return true;
    if (!db_.insert_batch(batch)) {
        failed_flushes_.fetch_add(1);
        std::cerr << "[StorageWorker] 写入失败，保留本批 " << batch.size() << " 条等待重试\n";
        return false;
    }
    total_saved_count_.fetch_add(batch.size());
    total_records_.store(db_.get_total_records());
    batch.clear();
    return true;
}
void StorageWorker::run() {
    std::vector<SensorData> batch;
    batch.reserve(batch_size_);
    auto last_flush = Clock::now();
    bool retrying = false;
    while (running_.load() || queue_.size() > 0 || !batch.empty()) {
        // 写失败时停止从队列取数据，保证 batch 最多 batch_size_ 条。
        // 采集队列仍有限；满时使用原有丢最旧策略，同时计数报警。
        if (!retrying && batch.size() < batch_size_) {
            auto item = queue_.pop_timeout(std::chrono::milliseconds(200));
            if (item) batch.push_back(*item);
        }
        const bool due = Clock::now() - last_flush >= max_wait_ms_;
        if (!batch.empty() &&
            (retrying || batch.size() >= batch_size_ || due || !running_.load())) {
            if (!flush_batch(batch)) {
                retrying = true;
                if (!running_.load()) {
                    // 磁盘故障时不能无限阻塞退出，也不能宣称这些内存数据已持久化。
                    unpersisted_.store(batch.size() + queue_.size());
                    std::cerr << "[StorageWorker] 退出时仍有未落库数据: "
                              << unpersisted_.load() << '\n';
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }
            retrying = false;
            last_flush = Clock::now();
        }
        if (!running_.load() && queue_.size() == 0 && batch.empty()) break;
    }
}
