#include "Mqtt_work.hpp"
#include <cstdio>
#include <limits>
#include <stdexcept>

MqttRuntime::MqttRuntime() {
    const int rc = mosquitto_lib_init();
    if (rc != MOSQ_ERR_SUCCESS) throw std::runtime_error(mosquitto_strerror(rc));
}
MqttRuntime::~MqttRuntime() { mosquitto_lib_cleanup(); }

MqttClient::MqttClient(const MqttConfig& config) : config_(config) {
    mosq_ = mosquitto_new(config_.clientId.c_str(), true, this);
    if (!mosq_) throw std::runtime_error("创建 MQTT 客户端失败");
}
MqttClient::~MqttClient() {
    // 和你原来的资源顺序一致：先回收线程，再销毁客户端。
    stop();
    mosquitto_destroy(mosq_);
}

int MqttClient::start() {
    if (loopStarted_) return MOSQ_ERR_SUCCESS;
    mosquitto_connect_callback_set(mosq_, &MqttClient::onConnect);
    mosquitto_disconnect_callback_set(mosq_, &MqttClient::onDisconnect);
    mosquitto_publish_callback_set(mosq_, &MqttClient::onPublish);
    int rc = mosquitto_int_option(mosq_, MOSQ_OPT_PROTOCOL_VERSION, MQTT_PROTOCOL_V311);
    if (rc != MOSQ_ERR_SUCCESS) return rc;
    rc = mosquitto_reconnect_delay_set(mosq_, config_.reconnectMinSeconds,
                                      config_.reconnectMaxSeconds, true);
    if (rc != MOSQ_ERR_SUCCESS) return rc;
    rc = mosquitto_connect_async(mosq_, config_.host.c_str(),
                                config_.port, config_.keepAliveSeconds);
    if (rc != MOSQ_ERR_SUCCESS) return rc;
    rc = mosquitto_loop_start(mosq_);
    if (rc != MOSQ_ERR_SUCCESS) {
        mosquitto_disconnect(mosq_);
        return rc;
    }
    loopStarted_ = true;
    return MOSQ_ERR_SUCCESS; // 仅启动成功，真正连接结果由回调报告。
}

void MqttClient::stop() {
    if (!mosq_) return;
    if (loopStarted_) {
        mosquitto_disconnect(mosq_);
        const int rc = mosquitto_loop_stop(mosq_, false);
        // libmosquitto 2.x 在线程自行结束后可能返回 INVAL，此时没有活动的库线程。
        if (rc != MOSQ_ERR_SUCCESS && rc != MOSQ_ERR_INVAL)
            throw std::runtime_error(std::string("停止 MQTT 网络线程失败: ") +
                                     mosquitto_strerror(rc));
        loopStarted_ = false;
    }
    connected_.store(false);
}

int MqttClient::resetSession() {
    stop();
    const int rc = mosquitto_reinitialise(mosq_, config_.clientId.c_str(), true, this);
    std::lock_guard<std::mutex> lock(ackMutex_);
    std::queue<int> empty;
    ackedMids_.swap(empty);
    publishPending_ = false;
    return rc;
}

PublishResult MqttClient::publishQos1(const std::string& topic,
                                     const std::string& payload) {
    PublishResult result;
    if (!connected_.load()) { result.errorCode = MOSQ_ERR_NO_CONN; return result; }
    if (payload.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        result.errorCode = MOSQ_ERR_PAYLOAD_SIZE;
        return result;
    }
    {
        std::lock_guard<std::mutex> lock(ackMutex_);
        if (publishPending_) { result.errorCode = MOSQ_ERR_INVAL; return result; }
        publishPending_ = true;
    }
    // 不持有 ackMutex_ 调用库函数，避免和网络线程的回调互相等待。
    result.errorCode = mosquitto_publish(mosq_, &result.mid, topic.c_str(),
        static_cast<int>(payload.size()), payload.data(), 1, false);
    if (result.errorCode != MOSQ_ERR_SUCCESS) {
        std::lock_guard<std::mutex> lock(ackMutex_);
        publishPending_ = false;
    }
    return result;
}

bool MqttClient::takePublishedMid(int& mid) {
    std::lock_guard<std::mutex> lock(ackMutex_);
    if (ackedMids_.empty()) return false;
    mid = ackedMids_.front();
    ackedMids_.pop();
    publishPending_ = false;
    return true;
}
void MqttClient::onConnect(mosquitto*, void* data, int rc) noexcept {
    auto* self = static_cast<MqttClient*>(data);
    self->connected_.store(rc == 0);
    if (rc == 0) self->connectionCount_.fetch_add(1);
    std::fprintf(stderr, "[MQTT] CONNACK=%d\n", rc);
}
void MqttClient::onDisconnect(mosquitto*, void* data, int rc) noexcept {
    auto* self = static_cast<MqttClient*>(data);
    self->connected_.store(false);
    if (rc != 0) self->disconnectCount_.fetch_add(1);
    std::fprintf(stderr, "[MQTT] 连接断开，结果码=%d\n", rc);
}
void MqttClient::onPublish(mosquitto*, void* data, int mid) noexcept {
    auto* self = static_cast<MqttClient*>(data);
    // 固定 MQTT 3.1.1、QoS 1；此回调是 PUBACK，不是业务落库确认。
    // Worker 同时最多提交一条消息，且取走确认后才继续。
    std::lock_guard<std::mutex> lock(self->ackMutex_);
    self->ackedMids_.push(mid);
}
