#include "Modbus_collector.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <poll.h>
#include <stdexcept>
#include <termios.h>
#include <unistd.h>

using namespace std;

namespace {
// 内部辅助函数：抛出带有系统 errno 信息的异常
void systemError(const string& message) {
    throw runtime_error(message + ": " + strerror(errno));
}
} // namespace

ModbusCollector::ModbusCollector(string device_path, uint8_t slave_id)
    : device_path_(std::move(device_path)),
      slave_id_(slave_id),
      fd_(-1),
      consecutive_failures_(0) {}

ModbusCollector::~ModbusCollector() {
    close_port();
}

void ModbusCollector::open_port() {
    close_port();
    fd_ = open(device_path_.c_str(), O_RDWR | O_NOCTTY | O_NONBLOCK);
    if (fd_ < 0) {
        throw runtime_error("打开串口失败 (" + device_path_ + "): " + strerror(errno));
    }
}

void ModbusCollector::close_port() noexcept {
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

bool ModbusCollector::connect() {
    try {
        open_port();
        configure_serial(fd_);
        usleep(10000); // 等待 10ms 让串口电平稳定
        cout << "[INFO] 串口初始连接成功: " << device_path_ << "\n";
        return true;
    } catch (const exception& e) {
        cerr << "[WARN] 串口初始连接失败: " << e.what() << "\n";
        close_port();
        return false;
    }
}

bool ModbusCollector::reconnect() {
    reconnect_count_++;
    cout << "[RECONNECT] 正在尝试第 " << reconnect_count_
         << " 次重连 (" << device_path_ << ")...\n";

    close_port();
    usleep(50000); // 关闭后等待 50ms 再重新打开

    try {
        open_port();
        configure_serial(fd_);
        usleep(10000);
        consecutive_failures_ = 0; // 重连成功后清零连续失败计数
        cout << "[RECONNECT] 串口重连成功！\n";
        return true;
    } catch (const exception& e) {
        cerr << "[RECONNECT] 串口重连失败: " << e.what() << "\n";
        close_port();
        return false;
    }
}

bool ModbusCollector::collect_once(SensorData& out_data) {
    // 如果串口尚未打开，或连续通信失败满 3 次，主动触发一次串口重连
    if (!is_open() || consecutive_failures_ >= 3) {
        if (!reconnect()) {
            fail_count_++;
            return false;
        }
    }

    try {
        // 关键：每次发包前清空输入输出缓冲区！
        // 防止断线或插拔杜邦线期间残留的半截脏数据导致后续报文全部错位
        if (tcflush(fd_, TCIOFLUSH) < 0) {
            systemError("tcflush 失败");
        }

        // 1. 构造读保持寄存器请求帧 (01 03 00 00 00 01 84 0A)
        Bytes request{
            slave_id_, 0x03,
            0x00, 0x00, // 起始地址 0x0000
            0x00, 0x01  // 寄存器数量 1
        };
        append_crc(request);

        // 2. 发送请求（单次超时设为 300ms，适配 500ms 的采集周期）
        const auto timeout = chrono::milliseconds(300);
        write_all(fd_, request, Clock::now() + timeout);

        // 3. 接收应答（先读 3 字节判断是正常响应还是异常响应）
        const auto recv_deadline = Clock::now() + timeout;
        Bytes response;
        read_until(fd_, response, 3, recv_deadline);

        if (response[0] != slave_id_) {
            throw runtime_error("从站地址错误");
        }

        if (response[1] == 0x83) {
            // 异常应答总长 5 字节
            read_until(fd_, response, 5, recv_deadline);
        } else if (response[1] == 0x03 && response[2] == 0x02) {
            // 正常应答总长 7 字节
            read_until(fd_, response, 7, recv_deadline);
        } else {
            throw runtime_error("功能码或字节数错误");
        }

        // 4. 校验 CRC16（低字节在前，高字节在后）
        const size_t len = response.size();
        const uint16_t recv_crc = static_cast<uint16_t>(response[len - 2]) |
                                  (static_cast<uint16_t>(response[len - 1]) << 8);
        if (crc16(response.data(), len - 2) != recv_crc) {
            throw runtime_error("CRC 校验失败");
        }

        // 5. 检查是否为 Modbus 异常应答
        if (response[1] == 0x83) {
            throw runtime_error("Modbus 异常码: " + to_string(response[2]));
        }

        // 6. 解析寄存器数据（高字节在前，低字节在后）并填充 SensorData
        const uint16_t raw = static_cast<uint16_t>(
            (static_cast<uint16_t>(response[3]) << 8) | response[4]);

        const auto now_ms = chrono::duration_cast<chrono::milliseconds>(
            chrono::system_clock::now().time_since_epoch()).count();

        out_data.timestamp_ms = now_ms;
        out_data.slave_id     = slave_id_;
        out_data.reg_addr     = 0x0000;
        out_data.raw_value    = raw;
        out_data.scaled_value = raw * 0.1; // 示例：1234 -> 123.4

        // 采集成功：累加成功次数，清零连续失败次数
        success_count_++;
        consecutive_failures_ = 0;
        return true;

    } catch (const exception& e) {
        // 捕获所有异常，保证断线时程序绝不崩溃退出
        fail_count_++;
        consecutive_failures_++;

        cerr << "[ERROR] 采集失败: " << e.what()
             << " (连续失败: " << consecutive_failures_ << " 次)\n";

        // 如果底层串口设备节点发生物理错误，立即关闭 fd，下一轮自动触发 reconnect
        if (errno == EIO || errno == ENXIO || errno == EBADF || errno == ENODEV) {
            close_port();
        }
        return false;
    }
}

void ModbusCollector::configure_serial(int fd) {
    termios options{};
    if (tcgetattr(fd, &options) < 0) {
        systemError("获取串口属性失败");
    }

    cfmakeraw(&options);

    // 115200 8N1：无校验(~PARENB)，1位停止位(~CSTOPB)，8位数据位(CS8)，无硬件流控(~CRTSCTS)
    options.c_cflag &= ~(PARENB | PARODD | CSTOPB | CSIZE | CRTSCTS);
    options.c_cflag |= (CS8 | CREAD | CLOCAL);

    options.c_cc[VMIN]  = 0;
    options.c_cc[VTIME] = 0;

    if (cfsetispeed(&options, B115200) < 0 ||
        cfsetospeed(&options, B115200) < 0) {
        systemError("设置波特率失败");
    }

    if (tcsetattr(fd, TCSANOW, &options) < 0) {
        systemError("设置串口属性失败");
    }

    if (tcflush(fd, TCIOFLUSH) < 0) {
        systemError("清空串口缓冲区失败");
    }
}

uint16_t ModbusCollector::crc16(const uint8_t* data, size_t length) {
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < length; ++i) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; ++bit) {
            if (crc & 1) {
                crc = static_cast<uint16_t>((crc >> 1) ^ 0xA001);
            } else {
                crc >>= 1;
            }
        }
    }
    return crc;
}

void ModbusCollector::append_crc(Bytes& frame) {
    const uint16_t crc = crc16(frame.data(), frame.size());
    frame.push_back(static_cast<uint8_t>(crc & 0xFF));
    frame.push_back(static_cast<uint8_t>((crc >> 8) & 0xFF));
}

void ModbusCollector::wait_ready(int fd, short event, Clock::time_point deadline) {
    while (true) {
        const auto now = Clock::now();
        if (now >= deadline) {
            throw runtime_error("串口收发超时");
        }

        const int remaining_ms = static_cast<int>(
            chrono::duration_cast<chrono::milliseconds>(deadline - now).count()) + 1;

        pollfd item{};
        item.fd     = fd;
        item.events = event;

        const int result = poll(&item, 1, remaining_ms);
        if (result < 0) {
            if (errno == EINTR) continue;
            systemError("poll 失败");
        }
        if (result == 0) {
            continue;
        }
        if (item.revents & POLLNVAL) {
            throw runtime_error("串口文件描述符无效");
        }
        if (item.revents & event) {
            return;
        }
        if (item.revents & (POLLERR | POLLHUP)) {
            throw runtime_error("串口异常或连接断开");
        }
    }
}

void ModbusCollector::write_all(int fd, const Bytes& frame, Clock::time_point deadline) {
    size_t sent = 0;
    while (sent < frame.size()) {
        wait_ready(fd, POLLOUT, deadline);
        const ssize_t count = write(fd, frame.data() + sent, frame.size() - sent);
        if (count > 0) {
            sent += static_cast<size_t>(count);
        } else if (count < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            systemError("发送失败");
        } else if (count == 0) {
            throw runtime_error("发送未取得进展");
        }
    }
}

void ModbusCollector::read_until(int fd, Bytes& frame, size_t targetsize, Clock::time_point deadline) {
    while (frame.size() < targetsize) {
        wait_ready(fd, POLLIN, deadline);
        uint8_t buffer[256];
        const size_t wanted = targetsize - frame.size();
        const ssize_t count = read(fd, buffer, wanted);
        if (count > 0) {
            frame.insert(frame.end(), buffer, buffer + count);
        } else if (count < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            systemError("接收失败");
        } else if (count == 0) {
            throw runtime_error("串口已关闭");
        }
    }
}