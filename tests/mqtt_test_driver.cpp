#include "Mqtt_worker.hpp"
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

// 供自动化测试使用；只操作测试数据库，不访问串口或真实设备。
int main(int argc, char* argv[]) {
    try {
        if (argc < 4) return 2;
        const std::string mode = argv[1], path = argv[2];
        if (mode == "seed") {
            DatabaseManager db(path);
            if (!db.open()) return 3;
            std::vector<SensorData> samples;
            for (int i = 0; i < std::stoi(argv[3]); ++i) {
                SensorData s{1794500000000LL + i, 0,
                    static_cast<uint16_t>(1000 + i), (1000 + i) * 0.1, 1};
                samples.push_back(s);
            }
            return db.insert_batch(samples) ? 0 : 4;
        }
        if (mode != "run" || argc != 7) return 2;
        MqttRuntime runtime;
        MqttConfig config;
        config.host = argv[3];
        config.port = std::stoi(argv[4]);
        config.clientId = "test-gateway";
        config.keepAliveSeconds = 5;
        config.reconnectMaxSeconds = 2;
        config.ackTimeoutSeconds = static_cast<unsigned int>(std::stoi(argv[6]));
        MqttWorker worker(config, path, "test-gateway");
        if (!worker.start()) return 5;
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(std::stoi(argv[5]));
        while (std::chrono::steady_clock::now() < deadline) {
            const auto s = worker.stats();
            if (s.faulted || (s.pending == 0 && s.reported > 0)) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        worker.stop();
        const auto s = worker.stats();
        std::cout << "{\"submitted\":" << s.submitted << ",\"pubacks\":" << s.pubacks
                  << ",\"reported\":" << s.reported << ",\"pending\":" << s.pending
                  << ",\"failures\":" << s.failures << ",\"db_errors\":" << s.dbErrors
                  << ",\"faulted\":" << (s.faulted ? "true" : "false") << "}\n";
        return s.faulted ? 6 : 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 7;
    }
}
