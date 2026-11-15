#include "Mqtt_worker.hpp"
#include <algorithm>
#include <cmath>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <vector>
#include <nlohmann/json.hpp>

using namespace std::chrono_literals;

MqttWorker::MqttWorker(const MqttConfig& config, const std::string& dbPath,
                       const std::string& gatewayId)
    : client_(config), db_(dbPath), gatewayId_(gatewayId),
      topic_("factory/gateway/" + gatewayId_ + "/telemetry"),
      ackTimeout_(config.ackTimeoutSeconds),
      retryMaxSeconds_(std::max(1U, config.reconnectMaxSeconds)) {
    if (config.ackTimeoutSeconds == 0) throw std::invalid_argument("PUBACK 超时必须大于 0");
}
MqttWorker::~MqttWorker() { stop(); }

bool MqttWorker::start() {
    if (thread_.joinable()) return running_.load();
    faulted_.store(false);
    running_.store(true);
    try { thread_ = std::thread(&MqttWorker::run, this); }
    catch (const std::exception& e) {
        running_.store(false);
        std::cerr << "[MqttWorker] 创建线程失败: " << e.what() << '\n';
        return false;
    }
    return true;
}
void MqttWorker::stop() {
    {
        // 和 waitFor 的谓词使用同一把锁，避免停止通知丢在进入等待之前。
        std::lock_guard<std::mutex> lock(waitMutex_);
        running_.store(false);
    }
    wake_.notify_all();
    if (thread_.joinable()) thread_.join();
}
bool MqttWorker::waitFor(std::chrono::milliseconds delay) {
    std::unique_lock<std::mutex> lock(waitMutex_);
    return wake_.wait_for(lock, delay, [this] { return !running_.load(); });
}
MqttStats MqttWorker::stats() const noexcept {
    MqttStats s;
    s.running = running_.load();
    s.connected = client_.isConnected();
    s.faulted = faulted_.load();
    s.waitingAck = waitingAck_.load();
    s.submitted = submitted_.load();
    s.pubacks = pubacks_.load();
    s.reported = reported_.load();
    s.failures = failures_.load();
    s.dbErrors = dbErrors_.load();
    const auto connections = client_.connectionCount();
    s.reconnects = connections > 0 ? connections - 1 : 0;
    s.pending = pending_.load();
    s.total = total_.load();
    return s;
}
void MqttWorker::refreshCounts() {
    pending_.store(db_.get_pending_count());
    total_.store(db_.get_total_records());
}
std::string MqttWorker::toJson(const PendingRecord& record) const {
    // 标准 JSON 不支持 NaN/无穷大；不要悄悄把非法工程值当有效数据发送。
    if (!std::isfinite(record.sample.scaled_value))
        throw std::runtime_error("采样工程值不是有限数值");
    nlohmann::json message = {
        {"gateway_id", gatewayId_},
        {"record_id", record.id},
        {"slave_id", record.sample.slave_id},
        {"register", record.sample.reg_addr},
        {"raw_value", record.sample.raw_value},
        {"value", record.sample.scaled_value},
        {"timestamp_ms", record.sample.timestamp_ms}
    };
    return message.dump();
}

void MqttWorker::run() {
    using Clock = std::chrono::steady_clock;
    bool dbReady = false;
    unsigned int retrySeconds = 1;
    auto nextCounts = Clock::now();
    auto lastConnected = Clock::now();

    // 一条在途记录同时保存两个 ID：业务主键 id 和协议编号 mid。
    std::optional<PendingRecord> active;
    int activeMid = 0;
    bool acknowledged = false;
    auto submittedAt = Clock::now();

    try {
        if (client_.resetSession() != MOSQ_ERR_SUCCESS)
            throw std::runtime_error("初始化 MQTT 会话失败");

        while (running_.load()) {
            if (!dbReady) {
                dbReady = db_.open();
                if (!dbReady) {
                    dbErrors_.fetch_add(1);
                    waitFor(std::chrono::seconds(retrySeconds));
                    retrySeconds = std::min(retrySeconds * 2, retryMaxSeconds_);
                    continue;
                }
                retrySeconds = 1;
            }
            const auto now = Clock::now();
            if (now >= nextCounts) {
                refreshCounts();
                nextCounts = now + 1s;
            }

            // 即使网络此刻断开，也先处理已经收到的确认。
            int mid;
            while (client_.takePublishedMid(mid)) {
                if (active && mid == activeMid && !acknowledged) {
                    acknowledged = true;
                    waitingAck_.store(false);
                    pubacks_.fetch_add(1);
                } else {
                    std::cerr << "[MqttWorker] 忽略无对应记录的确认 mid=" << mid << '\n';
                }
            }

            if (active) {
                if (acknowledged) {
                    // PUBACK 已到：数据库暂忙时只重试标记，绝不再次发布该记录。
                    if (db_.mark_reported(active->id)) {
                        reported_.fetch_add(1);
                        active.reset();
                        acknowledged = false;
                    } else {
                        dbErrors_.fetch_add(1);
                        std::cerr << "[MqttWorker] 确认已到，等待更新数据库\n";
                        waitFor(1s);
                    }
                    continue;
                }
                if (now - submittedAt >= ackTimeout_) {
                    // 结果未知：清除旧协议会话，用同一业务 ID 重发。
                    // Broker 可能已收到，所以接收端必须按 gateway_id + record_id 去重。
                    failures_.fetch_add(1);
                    std::cerr << "[MqttWorker] PUBACK 超时，重建会话，record_id="
                              << active->id << '\n';
                    if (client_.resetSession() != MOSQ_ERR_SUCCESS)
                        throw std::runtime_error("重建 MQTT 会话失败");
                    active.reset();
                    waitingAck_.store(false);
                    lastConnected = Clock::now();
                    waitFor(1s);
                    continue;
                }
                waitFor(50ms);
                continue;
            }

            if (!client_.isRunning()) {
                const int rc = client_.start();
                if (rc != MOSQ_ERR_SUCCESS) {
                    failures_.fetch_add(1);
                    std::cerr << "[MqttWorker] 启动连接失败: "
                              << mosquitto_strerror(rc) << '\n';
                    client_.resetSession();
                    waitFor(std::chrono::seconds(retrySeconds));
                    retrySeconds = std::min(retrySeconds * 2, retryMaxSeconds_);
                    continue;
                }
                lastConnected = now;
            }
            if (!client_.isConnected()) {
                // 正常断网由库自动重连；长时间不成功时清理异常/已退出的旧会话。
                if (now - lastConnected > std::chrono::seconds(retryMaxSeconds_ + 30)) {
                    failures_.fetch_add(1);
                    if (client_.resetSession() != MOSQ_ERR_SUCCESS)
                        throw std::runtime_error("重建连接失败");
                    lastConnected = Clock::now();
                }
                waitFor(250ms);
                continue;
            }
            lastConnected = now;
            retrySeconds = 1;

            std::vector<PendingRecord> records;
            if (!db_.get_pending_records(1, records)) {
                dbErrors_.fetch_add(1);
                waitFor(1s);
                continue;
            }
            if (records.empty()) { waitFor(250ms); continue; }

            std::string payload;
            try { payload = toJson(records.front()); }
            catch (const std::exception& e) {
                failures_.fetch_add(1);
                std::cerr << "[MqttWorker] JSON 编码失败，record_id="
                          << records.front().id << ": " << e.what() << '\n';
                // 保留问题记录并报警，不假装已经上报。
                waitFor(1s);
                continue;
            }
            const auto result = client_.publishQos1(topic_, payload);
            if (result.errorCode != MOSQ_ERR_SUCCESS) {
                failures_.fetch_add(1);
                if (client_.resetSession() != MOSQ_ERR_SUCCESS)
                    throw std::runtime_error("提交失败后重建会话失败");
                waitFor(1s);
                continue;
            }
            active = records.front();
            activeMid = result.mid;
            acknowledged = false;
            submittedAt = Clock::now();
            submitted_.fetch_add(1);
            waitingAck_.store(true);
            // 即使回调比 publish 返回还快，确认也已在队列中，下一轮再处理。
        }
    } catch (const std::exception& e) {
        faulted_.store(true);
        failures_.fetch_add(1);
        std::cerr << "[MqttWorker] 上报线程异常退出: " << e.what() << '\n';
    }
    // 退出不等待清空全部历史；尚未标记的记录下次启动继续补传。
    // main 先停止存储再停止上报，因此这里的数量包含最后一批落库。
    if (dbReady) refreshCounts();
    client_.stop();
    db_.close();
    waitingAck_.store(false);
    running_.store(false);
}
