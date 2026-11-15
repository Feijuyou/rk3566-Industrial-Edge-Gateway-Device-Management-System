#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>
#include "SensorData.hpp"

// Modbus RTU 采集器类：负责串口生命周期管理、报文收发、CRC校验、断线重连与统计
class ModbusCollector {
public:
    using Bytes = std::vector<uint8_t>;
    using Clock = std::chrono::steady_clock;

    // 构造函数：传入串口设备路径（如 "/dev/ttyS3"）和从站地址（默认 0x01）
    explicit ModbusCollector(std::string device_path, uint8_t slave_id = 0x01);

    // 析构函数：自动关闭串口文件描述符（RAII）
    ~ModbusCollector();

    // 禁止拷贝（防止两个对象管理同一个串口 fd 导致重复 close）
    ModbusCollector(const ModbusCollector&) = delete;
    ModbusCollector& operator=(const ModbusCollector&) = delete;

    // 初次打开并配置串口（不计入重连次数）
    bool connect();

    // 断开后重新打开并配置串口（计入 reconnect_count）
    bool reconnect();

    // 执行一次寄存器采集：
    // - 成功：返回 true，并将解析结果填入 out_data，success_count++
    // - 失败：捕获超时/CRC/断开异常，返回 false（不崩溃），fail_count++
    bool collect_once(SensorData& out_data);

    // 获取当前的统计数据（成功次数、失败次数、重连次数）
    CollectorStats stats() const noexcept {
        return {success_count_.load(), fail_count_.load(), reconnect_count_.load()};
    }

private:
    std::string    device_path_;          // 串口设备节点路径
    uint8_t        slave_id_;             // Modbus 从站地址
    int            fd_ = -1;              // 串口文件描述符（-1 表示未打开）
    std::atomic<uint64_t> success_count_{0}, fail_count_{0}, reconnect_count_{0};                // 运行统计计数
    uint32_t       consecutive_failures_; // 连续采集失败次数（用于触发主动重连）

    // 内部串口开关辅助函数
    void open_port();
    void close_port() noexcept;
    bool is_open() const noexcept { return fd_ >= 0; }

    // 底层串口与协议辅助函数（均保留你原有的核心逻辑）
    static void     configure_serial(int fd);
    static uint16_t crc16(const uint8_t* data, size_t length);
    static void     append_crc(Bytes& frame);
    static void     wait_ready(int fd, short event, Clock::time_point deadline);
    static void     write_all(int fd, const Bytes& frame, Clock::time_point deadline);
    static void     read_until(int fd, Bytes& frame, size_t targetsize, Clock::time_point deadline);
};