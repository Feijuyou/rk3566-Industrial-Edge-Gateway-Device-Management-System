#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <utility>
#include <cstdint>

#include "SensorData.hpp"

// 架构图中的 ② SensorDataQueue：专为 SensorData 设计的线程安全阻塞队列
class SensorDataQueue {
public:
    // 构造函数：默认最大缓存 1000 条数据（超过后自动丢弃最旧的数据，保护嵌入式内存）
    explicit SensorDataQueue(size_t max_capacity = 1000)
        : max_capacity_(max_capacity), stop_requested_(false) {
        if (max_capacity == 0) throw std::invalid_argument("队列容量必须大于 0");
    }

    ~SensorDataQueue() {
        stop();
    }

    // 禁止拷贝
    SensorDataQueue(const SensorDataQueue&) = delete;
    SensorDataQueue& operator=(const SensorDataQueue&) = delete;

    // ==================== 采集线程 (生产者) 调用 ====================
    // 推入一条采集到的传感器数据
    void push(SensorData data) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stop_requested_) {
                return; // 已经收到停止信号，不再接收新数据
            }

            // 保护机制：如果队列满了，弹出最老的一条（工业数据更看重最新鲜的数据）
            if (queue_.size() >= max_capacity_) {
                queue_.pop();
                ++dropped_count_;
            }

            queue_.push(std::move(data));
        }
        // 唤醒一个正在休眠等待的消费线程
        cv_.notify_one();
    }

    // ==================== 存储/消费线程 (消费者) 调用 ====================
    // 从队列中取出一批/一条数据（带超时等待，避免死等）
    // - 成功取到：返回包含数据的 std::optional<SensorData>
    // - 超时或队列已停止且为空：返回 std::nullopt
    std::optional<SensorData> pop_timeout(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);

        // 等待条件：队列非空，或者队列被要求停止 (stop_requested_)
        const bool wait_ok = cv_.wait_for(lock, timeout, [this] {
            return !queue_.empty() || stop_requested_;
        });

        // 如果被通知退出且队列已经处理空了，返回空
        if (stop_requested_ && queue_.empty()) {
            return std::nullopt;
        }

        // 超时或者还是没有数据，返回空
        if (!wait_ok || queue_.empty()) {
            return std::nullopt;
        }

        SensorData item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

    // ==================== 控制与监控接口 ====================
    // 请求停止队列：唤醒所有挂起的等待线程，准备安全退出
    void stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_requested_ = true;
        }
        // 唤醒所有正在等待的线程，让它们检查退出条件
        cv_.notify_all();
    }

    // 当前队列中堆积的数据条数（供日志或监控查看是否有积压）
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    // 检查是否已经请求停止
    bool is_stopped() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return stop_requested_;
    }

    uint64_t dropped_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return dropped_count_;
    }

private:
    uint64_t dropped_count_{0};
    mutable std::mutex      mutex_;          // 保护 queue_ 和 stop_requested_ 的互斥锁
    std::condition_variable cv_;             // 用于休眠和唤醒消费者的条件变量
    std::queue<SensorData>  queue_;          // 底层 FIFO 队列
    const size_t            max_capacity_;   // 最大容量限制
    bool                    stop_requested_; // 停止标志
};