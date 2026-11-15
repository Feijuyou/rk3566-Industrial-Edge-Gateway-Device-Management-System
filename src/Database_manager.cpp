#include "Database_manager.hpp"
#include <iostream>
#include <utility>

DatabaseManager::DatabaseManager(std::string db_path) : db_path_(std::move(db_path)) {}
DatabaseManager::~DatabaseManager() { close(); }

bool DatabaseManager::execute_sql(const char* sql) {
    char* error = nullptr;
    const int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &error);
    if (rc == SQLITE_OK) return true;
    std::cerr << "[Database] SQL 失败: "
              << (error ? error : sqlite3_errmsg(db_)) << '\n';
    sqlite3_free(error);
    return false;
}

bool DatabaseManager::open() {
    close();
    if (sqlite3_open(db_path_.c_str(), &db_) != SQLITE_OK) {
        std::cerr << "[Database] 打开失败: " << sqlite3_errmsg(db_) << '\n';
        close();
        return false;
    }
    // WAL 允许读写并行，但仍只有一个写入者；短暂锁冲突最多等待 1500ms。
    sqlite3_busy_timeout(db_, 1500);
    // FULL 更重视已提交事务的掉电持久性，仍依赖存储硬件正确实现同步。
    if (!execute_sql("PRAGMA journal_mode=WAL;") ||
        !execute_sql("PRAGMA synchronous=FULL;") ||
        !init_schema() || !prepare_statements()) {
        close();
        return false;
    }
    return true;
}

void DatabaseManager::close() {
    if (stmt) { sqlite3_finalize(stmt); stmt = nullptr; }
    if (insert_stmt_) { sqlite3_finalize(insert_stmt_); insert_stmt_ = nullptr; }
    if (db_) { sqlite3_close(db_); db_ = nullptr; }
}

bool DatabaseManager::init_schema() {
    if (!execute_sql(
        "CREATE TABLE IF NOT EXISTS sensor_history ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "timestamp_ms INTEGER NOT NULL, slave_id INTEGER NOT NULL,"
        "reg_addr INTEGER NOT NULL, raw_value INTEGER NOT NULL,"
        "scaled_value REAL NOT NULL, reported INTEGER DEFAULT 0);")) return false;

    // 兼容尚未包含 reported 的旧表；保留原始采样和主键。
    sqlite3_stmt* columns = nullptr;
    if (sqlite3_prepare_v2(db_, "PRAGMA table_info(sensor_history);", -1,
                           &columns, nullptr) != SQLITE_OK) return false;
    bool has_reported = false;
    int rc;
    while ((rc = sqlite3_step(columns)) == SQLITE_ROW) {
        const auto* name = sqlite3_column_text(columns, 1);
        if (name && std::string(reinterpret_cast<const char*>(name)) == "reported")
            has_reported = true;
    }
    sqlite3_finalize(columns);
    if (rc != SQLITE_DONE) return false;
    if (!has_reported && !execute_sql(
        "ALTER TABLE sensor_history ADD COLUMN reported INTEGER DEFAULT 0;")) return false;
    return execute_sql("UPDATE sensor_history SET reported=0 WHERE reported IS NULL;") &&
        execute_sql("CREATE INDEX IF NOT EXISTS idx_reported_id "
                    "ON sensor_history(reported,id);");
}

bool DatabaseManager::prepare_statements() {
    const char* sql =
        "INSERT INTO sensor_history "
        "(timestamp_ms,slave_id,reg_addr,raw_value,scaled_value,reported) "
        "VALUES(?,?,?,?,?,0);";
    return sqlite3_prepare_v2(db_, sql, -1, &insert_stmt_, nullptr) == SQLITE_OK;
}

bool DatabaseManager::insert_batch(const std::vector<SensorData>& batch) {
    if (batch.empty()) return true;
    if (!db_ || !insert_stmt_) return false;
    // BEGIN 失败时必须停止，不能继续执行而退化成逐条自动提交。
    if (!execute_sql("BEGIN IMMEDIATE;")) return false;
    for (const auto& data : batch) {
        sqlite3_reset(insert_stmt_);
        sqlite3_clear_bindings(insert_stmt_);
        const bool bound =
            sqlite3_bind_int64(insert_stmt_, 1, data.timestamp_ms) == SQLITE_OK &&
            sqlite3_bind_int(insert_stmt_, 2, data.slave_id) == SQLITE_OK &&
            sqlite3_bind_int(insert_stmt_, 3, data.reg_addr) == SQLITE_OK &&
            sqlite3_bind_int(insert_stmt_, 4, data.raw_value) == SQLITE_OK &&
            sqlite3_bind_double(insert_stmt_, 5, data.scaled_value) == SQLITE_OK;
        if (!bound || sqlite3_step(insert_stmt_) != SQLITE_DONE) {
            std::cerr << "[Database] 写入失败: " << sqlite3_errmsg(db_) << '\n';
            sqlite3_reset(insert_stmt_);
            execute_sql("ROLLBACK;");
            return false;
        }
    }
    sqlite3_reset(insert_stmt_);
    if (!execute_sql("COMMIT;")) {
        execute_sql("ROLLBACK;");
        return false;
    }
    return true;
}

int64_t DatabaseManager::get_total_records() {
    if (!db_) return -1;
    sqlite3_stmt* query = nullptr;
    if (sqlite3_prepare_v2(db_, "SELECT COUNT(*) FROM sensor_history;", -1,
                           &query, nullptr) != SQLITE_OK) return -1;
    const int64_t count = sqlite3_step(query) == SQLITE_ROW
        ? sqlite3_column_int64(query, 0) : -1;
    sqlite3_finalize(query);
    return count;
}

bool DatabaseManager::get_pending_records(int limit, std::vector<PendingRecord>& records) {
    records.clear();
    if (!db_ || limit <= 0) return false;
    const char* sql =
        "SELECT id,timestamp_ms,slave_id,reg_addr,raw_value,scaled_value "
        "FROM sensor_history WHERE reported=0 ORDER BY id ASC LIMIT ?;";
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return false;
    rc = sqlite3_bind_int(stmt, 1, limit);
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            PendingRecord record{};
            record.id = sqlite3_column_int64(stmt, 0);
            record.sample.timestamp_ms = sqlite3_column_int64(stmt, 1);
            record.sample.slave_id = static_cast<uint8_t>(sqlite3_column_int(stmt, 2));
            record.sample.reg_addr = static_cast<uint16_t>(sqlite3_column_int(stmt, 3));
            record.sample.raw_value = static_cast<uint16_t>(sqlite3_column_int(stmt, 4));
            record.sample.scaled_value = sqlite3_column_double(stmt, 5);
            records.push_back(record);
        }
    }
    sqlite3_finalize(stmt);
    stmt = nullptr;
    if (rc != SQLITE_DONE) { records.clear(); return false; }
    return true;
}

bool DatabaseManager::mark_reported(int64_t id) {
    if (!db_) return false;
    const char* sql = "UPDATE sensor_history SET reported=1 WHERE id=?;";
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return false;
    rc = sqlite3_bind_int64(stmt, 1, id);
    if (rc == SQLITE_OK) rc = sqlite3_step(stmt);
    // 不带 reported=0 条件，允许对同一个已存在的 ID 重复标记。
    const bool success = rc == SQLITE_DONE && sqlite3_changes(db_) == 1;
    sqlite3_finalize(stmt);
    stmt = nullptr;
    return success;
}

int64_t DatabaseManager::get_pending_count() {
    if (!db_) return -1;
    const char* sql = "SELECT COUNT(*) FROM sensor_history WHERE reported=0;";
    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return -1;
    rc = sqlite3_step(stmt);
    const int64_t count = rc == SQLITE_ROW ? sqlite3_column_int64(stmt, 0) : -1;
    sqlite3_finalize(stmt);
    stmt = nullptr;
    return count;
}
