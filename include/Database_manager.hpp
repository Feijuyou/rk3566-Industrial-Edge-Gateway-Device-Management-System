#pragma once 

#include <cstdint>
#include <string>
#include <vector>
#include <sqlite3.h>

#include "SensorData.hpp"


struct PendingRecord{
    int64_t id;
    SensorData sample;
};

class DatabaseManager{
public:
    DatabaseManager(std::string db_path);
    ~DatabaseManager();

    DatabaseManager(const DatabaseManager&)=delete;
    DatabaseManager& operator=(const DatabaseManager&) =delete;

    bool open();
    void close();
    bool insert_batch(const std::vector<SensorData>& batch);
    int64_t get_total_records();
    bool get_pending_records(int limit,std::vector<PendingRecord>& records);
    bool mark_reported(int64_t id);
    int64_t get_pending_count();
private:

    std::string db_path_;
    sqlite3* db_=nullptr;
    sqlite3_stmt* insert_stmt_=nullptr;
    sqlite3_stmt* stmt = nullptr;
    bool init_schema();
    bool prepare_statements();
    bool execute_sql(const char* sql);

};









