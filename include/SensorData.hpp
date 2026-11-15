#pragma once 
#include <cstdint>

struct SensorData{
    int64_t timestamp_ms = 0;
    uint16_t reg_addr =0;
    uint16_t raw_value = 0;
    double scaled_value = 0.0;
    uint8_t slave_id = 0;
};

struct CollectorStats{
    uint64_t success_count =0;
    uint64_t fail_count = 0;
    uint64_t reconnect_count =0;
};