#include "Database_manager.hpp"
#include "Storage_worker.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <thread>

static void check(bool ok, const char* text) {
    if (!ok) throw std::runtime_error(text);
}
static void exec(sqlite3* db, const char* sql) {
    check(sqlite3_exec(db, sql, nullptr, nullptr, nullptr) == SQLITE_OK, sql);
}
int main() {
    // 所有数据库位于本机临时目录，不在 Samba/NFS 上执行 WAL 测试。
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = (std::filesystem::temp_directory_path() /
                       ("rk3566-db-test-" + std::to_string(stamp) + ".db")).string();
    try {
        sqlite3* raw = nullptr;
        check(sqlite3_open(path.c_str(), &raw) == SQLITE_OK, "open old schema");
        exec(raw, "CREATE TABLE sensor_history(id INTEGER PRIMARY KEY AUTOINCREMENT,"
             "timestamp_ms INTEGER NOT NULL,slave_id INTEGER NOT NULL,"
             "reg_addr INTEGER NOT NULL,raw_value INTEGER NOT NULL,scaled_value REAL NOT NULL);");
        exec(raw, "INSERT INTO sensor_history VALUES(1,1794500000000,255,65535,65535,6553.5);");
        sqlite3_close(raw);
        DatabaseManager db(path);
        check(db.open(), "migrate old database");
        check(db.get_pending_count() == 1, "migration retains pending row");
        std::vector<PendingRecord> rows;
        check(db.get_pending_records(1, rows) && rows.size() == 1, "read pending");
        check(rows[0].sample.slave_id == 255 && rows[0].sample.raw_value == 65535 &&
              rows[0].sample.timestamp_ms == 1794500000000LL, "integer types preserved");
        check(!db.get_pending_records(0, rows), "reject zero limit");
        check(!db.mark_reported(99999), "reject missing row");
        check(db.mark_reported(1) && db.mark_reported(1), "mark is idempotent");
        check(db.get_pending_count() == 0, "pending zero");
        check(db.get_pending_records(1, rows) && rows.empty(), "empty query succeeds");

        SensorData sample{1794500000123LL, 0, 1234, 123.4, 1};
        check(db.insert_batch({sample, sample}), "insert transaction");
        check(db.get_total_records() == 3 && db.get_pending_count() == 2, "batch count");

        check(sqlite3_open(path.c_str(), &raw) == SQLITE_OK, "open lock holder");
        exec(raw, "BEGIN IMMEDIATE;");
        check(!db.mark_reported(2), "busy update must not report success");
        exec(raw, "ROLLBACK;");
        check(db.get_pending_count() == 2, "busy update retains pending");
        db.close();
        check(!db.insert_batch({sample}) && db.get_pending_count() == -1, "closed connection fails");

        // 验证存储线程退出时刷入不足一批的数据。
        SensorDataQueue queue(10);
        StorageWorker storage(queue, path, 10, std::chrono::seconds(20));
        check(storage.start(), "storage start");
        queue.push(sample);
        queue.push(sample);
        queue.stop();
        storage.stop();
        check(storage.total_saved_count() == 2 && storage.db_record_count() == 5,
              "stop flushes final batch and keeps cached count");

        // 用触发器注入写入错误，验证批次/队列都有上限且停止不会无限重试。
        exec(raw, "CREATE TRIGGER fail_insert BEFORE INSERT ON sensor_history "
                  "BEGIN SELECT RAISE(ABORT,'injected storage failure'); END;");
        SensorDataQueue bounded(3);
        StorageWorker failing(bounded, path, 2, std::chrono::milliseconds(10));
        check(failing.start(), "failing storage start");
        bounded.push(sample);
        bounded.push(sample);
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        for (int i = 0; i < 20; ++i) bounded.push(sample);
        bounded.stop();
        failing.stop();
        check(failing.failed_flushes() > 0 && failing.unpersisted_count() > 0 &&
              failing.unpersisted_count() <= 5 && bounded.dropped_count() > 0,
              "failed writes remain bounded and observable");
        sqlite3_close(raw);
        for (const auto* suffix : {"", "-wal", "-shm"}) std::filesystem::remove(path + suffix);
        std::cout << "database/storage tests passed\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAILED: " << e.what() << " db=" << path << '\n';
        return 1;
    }
}
