#include "Acquisition_worker.hpp"
#include <iostream>

using namespace std;
using Clock = chrono::steady_clock;

AcquisitionWorker::AcquisitionWorker(string device_path,
                                     uint8_t slave_id,
                                     chrono::milliseconds interval_ms,
                                     SensorDataQueue& queue)
    : device_path_(std::move(device_path)),
      slave_id_(slave_id),
      interval_(interval_ms),
      queue_(queue),
      collector_(device_path_, slave_id_) {}

AcquisitionWorker::~AcquisitionWorker() {
    stop();
}

bool AcquisitionWorker::start() {
    if (running_.load()) {
        return true; // 已经在运行
    }

    // 先初次连接串口
    if (!collector_.connect()) {
        cerr << "[AcquisitionWorker] 串口初次连接失败，但将在工作线程中持续重试\n";
    }

    running_.store(true); //启动开关
    // 创建并启动独立的工作线程
    thread_ = thread([this]{run();});
    cout << "[AcquisitionWorker] 采集线程已启动，周期: " << interval_.count() << "ms\n";
    return true;
}

void AcquisitionWorker::stop() {
    if (!running_.load()) {   //读取running这个值
        return;
    }

    running_.store(false);

    // 等待后台线程执行完当前这一轮并安全退出
    if (thread_.joinable()) {
        thread_.join();
    }
    cout << "[AcquisitionWorker] 采集线程已安全停止\n";
}

void AcquisitionWorker::run() {
    auto next_tick = Clock::now();

    while (running_.load()) {
        next_tick += interval_;

        SensorData data{};
        // 执行单次采集（内部已包含超时重试与断线重连）
        if (collector_.collect_once(data)) {
            // 【核心对接】：采集成功，推入线程安全队列！
            queue_.push(data);
        }

        // 周期节拍控制：睡眠到下一个节拍点，避免 CPU 空转
        if (running_.load()) {
            this_thread::sleep_until(next_tick);
            const auto now = Clock::now();
            if (now > next_tick) {
                next_tick = now;
            }
        }
    }
}