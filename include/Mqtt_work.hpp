#pragma once
#include <atomic>
#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <mosquitto.h>

// 进程级资源：必须比所有 MqttClient 先构造、后析构。
class MqttRuntime {
public:
    MqttRuntime();
    ~MqttRuntime();
    MqttRuntime(const MqttRuntime&) = delete;
    MqttRuntime& operator=(const MqttRuntime&) = delete;
};

struct MqttConfig {
    std::string host;
    int port{1883};
    std::string clientId{"rk3566-01"};
    int keepAliveSeconds{15};
    unsigned int reconnectMinSeconds{1};
    unsigned int reconnectMaxSeconds{30};
    // 超时表示结果未知，重建客户端后仍用同一业务 ID 补传，允许重复。
    unsigned int ackTimeoutSeconds{30};
};

struct PublishResult {
    int errorCode{MOSQ_ERR_UNKNOWN};
    int mid{0};
};

class MqttClient {
public:
    explicit MqttClient(const MqttConfig& config);
    ~MqttClient();
    MqttClient(const MqttClient&) = delete;
    MqttClient& operator=(const MqttClient&) = delete;
    int start();
    void stop();
    // 清除旧会话中尚未完成的协议消息；SQLite 中的记录不变。
    int resetSession();
    bool isConnected() const noexcept { return connected_.load(); }
    bool isRunning() const noexcept { return loopStarted_; } // 仅拥有者线程读取。
    uint64_t connectionCount() const noexcept { return connectionCount_.load(); }
    uint64_t disconnectCount() const noexcept { return disconnectCount_.load(); }
    PublishResult publishQos1(const std::string& topic, const std::string& payload);
    bool takePublishedMid(int& mid);
private:
    static void onConnect(mosquitto*, void*, int) noexcept;
    static void onDisconnect(mosquitto*, void*, int) noexcept;
    static void onPublish(mosquitto*, void*, int) noexcept;
    MqttConfig config_;
    mosquitto* mosq_{nullptr};
    std::atomic<bool> connected_{false};
    std::atomic<uint64_t> connectionCount_{0}, disconnectCount_{0};
    bool loopStarted_{false};
    std::mutex ackMutex_;
    std::queue<int> ackedMids_;
    // 从提交到业务线程取走确认，只允许一条在途消息，限制内存队列增长。
    bool publishPending_{false};
};
