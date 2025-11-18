#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <mysql/mysql.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include "uthash.h"  // uthash library for hash table
#include "civetweb/include/civetweb.h"

#define PORT "8080"
#define MAX_CACHE_ENTRIES 20000  // Increased cache size
#define MAX_VALUE_SIZE 512
#define DB_POOL_SIZE 50          // Reduced pool size for better connection reuse
#define CACHE_SHARDS 256         // More shards for better concurrency

// Write batching configuration
#define BATCH_BUFFER_SIZE 1000   // Batch up to 1000 writes
#define BATCH_FLUSH_INTERVAL_MS 10  // Flush every 10ms or when buffer full

// Cache entry structure for uthash
typedef struct CacheEntry 
{
    int key;                    
    char value[MAX_VALUE_SIZE]; 
    int access_time;            
    UT_hash_handle hh;          
} CacheEntry;

// Cache sharding for reduced contention
typedef struct CacheShard 
{
    CacheEntry *cache;
    int cache_count;
    int access_counter;
    pthread_mutex_t lock;
} CacheShard;

// Write batch entry
typedef struct BatchEntry 
{
    int key;
    char value[MAX_VALUE_SIZE];
    int operation; // 0=INSERT/UPDATE, 1=DELETE
} BatchEntry;

// Write batch buffer
typedef struct WriteBatch 
{
    BatchEntry entries[BATCH_BUFFER_SIZE];
    int count;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int flush_requested;
} WriteBatch;

// Global variables
CacheShard cache_shards[CACHE_SHARDS];
int max_entries_per_shard = MAX_CACHE_ENTRIES / CACHE_SHARDS;

// Database connection pool
MYSQL *db_pool[DB_POOL_SIZE];
pthread_mutex_t db_pool_locks[DB_POOL_SIZE];
int db_available[DB_POOL_SIZE];
pthread_mutex_t db_pool_counter_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t db_pool_cond = PTHREAD_COND_INITIALIZER;

// Write batching
WriteBatch write_batch = {0};
pthread_t batch_thread;

volatile int server_running = 1;
struct mg_context *ctx = NULL;

// Forward declarations
void* batch_writer_thread(void* arg);
int add_to_batch(int key, const char* value, int operation);

// Initialize MySQL connection pool with optimizations
void init_mysql() 
{
    // MySQL client library initialization with optimizations
    mysql_library_init(0, NULL, NULL);
    
    for (int i = 0; i < DB_POOL_SIZE; i++) 
    {
        db_pool[i] = mysql_init(NULL);
        if (!db_pool[i]) 
        {
            fprintf(stderr, "mysql_init() failed for connection %d\n", i);
            exit(1);
        }

        // Connection optimizations
        unsigned int reconnect = 1;
        mysql_options(db_pool[i], MYSQL_OPT_RECONNECT, &reconnect);
        
        // Enable compression for better throughput
        mysql_options(db_pool[i], MYSQL_OPT_COMPRESS, NULL);
        
        // Set larger network buffer
        unsigned int net_buffer = 32 * 1024 * 1024; // 32MB
        mysql_options(db_pool[i], MYSQL_OPT_MAX_ALLOWED_PACKET, &net_buffer);

        if (!mysql_real_connect(db_pool[i], "localhost", "decsuser", "decs123", "decsdb", 0, NULL, CLIENT_COMPRESS)) 
        {
            fprintf(stderr, "mysql_real_connect() failed: %s\n", mysql_error(db_pool[i]));
            exit(1);
        }
        
        // Set per-connection optimizations
        mysql_query(db_pool[i], "SET SESSION innodb_flush_log_at_trx_commit = 0");  // Don't fsync on every commit
        mysql_query(db_pool[i], "SET SESSION sync_binlog = 0");                     // Don't sync binlog  
        mysql_query(db_pool[i], "SET SESSION autocommit = 1");                      // Enable autocommit for single queries
        mysql_query(db_pool[i], "SET SESSION sql_log_bin = 0");                     // Disable binary logging for this session
        mysql_query(db_pool[i], "SET SESSION foreign_key_checks = 0");              // Disable FK checks for speed
        mysql_query(db_pool[i], "SET SESSION unique_checks = 0");                   // Disable unique checks for bulk ops
        
        pthread_mutex_init(&db_pool_locks[i], NULL);
        db_available[i] = 1;
    }

    // Create table with optimizations using first connection
    const char *create_table = "CREATE TABLE IF NOT EXISTS kvstore ("
                               "key_id INT PRIMARY KEY, "
                               "value_data VARCHAR(512)"
                               ") ENGINE=InnoDB "
                               "ROW_FORMAT=COMPACT "           // More compact storage
                               "KEY_BLOCK_SIZE=8";             // 8KB pages for better compression

    if (mysql_query(db_pool[0], create_table)) 
    {
        fprintf(stderr, "Failed to create table: %s\n", mysql_error(db_pool[0]));
        exit(1);
    }

    // Set global MySQL optimizations (if we had privileges)
    mysql_query(db_pool[0], "SET SESSION innodb_buffer_pool_size = 1073741824");  // 1GB if possible

    printf("Connected to MySQL with %d optimized connections\n", DB_POOL_SIZE);
    
    // Initialize write batch
    pthread_mutex_init(&write_batch.lock, NULL);
    pthread_cond_init(&write_batch.cond, NULL);
    write_batch.count = 0;
    write_batch.flush_requested = 0;
    
    // Start batch writer thread
    if (pthread_create(&batch_thread, NULL, batch_writer_thread, NULL) != 0) {
        fprintf(stderr, "Failed to create batch writer thread\n");
        exit(1);
    }
    
    printf("Write batching initialized (buffer size: %d, flush interval: %dms)\n", 
           BATCH_BUFFER_SIZE, BATCH_FLUSH_INTERVAL_MS);
}

// Get shard index for a key
static inline int get_shard_index(int key) 
{
    return key % CACHE_SHARDS;
}

// Get database connection from pool
int get_db_connection() 
{
    pthread_mutex_lock(&db_pool_counter_lock);
    
    int conn_id = -1;
    while (conn_id == -1) 
    {
        for (int i = 0; i < DB_POOL_SIZE; i++) 
        {
            if (db_available[i]) 
            {
                db_available[i] = 0;
                conn_id = i;
                break;
            }
        }
        if (conn_id == -1) 
        {
            pthread_cond_wait(&db_pool_cond, &db_pool_counter_lock);
        }
    }
    
    pthread_mutex_unlock(&db_pool_counter_lock);
    pthread_mutex_lock(&db_pool_locks[conn_id]);
    return conn_id;
}

// Release database connection back to pool
void release_db_connection(int conn_id) 
{
    pthread_mutex_unlock(&db_pool_locks[conn_id]);
    pthread_mutex_lock(&db_pool_counter_lock);
    db_available[conn_id] = 1;
    pthread_cond_signal(&db_pool_cond);
    pthread_mutex_unlock(&db_pool_counter_lock);
}

// Find LRU entry for eviction in a shard
static CacheEntry* find_lru_entry(CacheShard *shard) 
{
    CacheEntry *entry, *lru_entry = NULL;
    int oldest_time = shard->access_counter + 1;
    
    for (entry = shard->cache; entry != NULL; entry = entry->hh.next) 
    {
        if (entry->access_time < oldest_time) 
        {
            oldest_time = entry->access_time;
            lru_entry = entry;
        }
    }
    return lru_entry;
}

// Cache operations using uthash
void cache_put(int key, const char* value) 
{
    int shard_idx = get_shard_index(key);
    CacheShard *shard = &cache_shards[shard_idx];
    
    pthread_mutex_lock(&shard->lock);
    
    CacheEntry *entry;
    
    // Check if key already exists
    HASH_FIND_INT(shard->cache, &key, entry);
    if (entry) 
    {
        // Update existing entry
        strncpy(entry->value, value, MAX_VALUE_SIZE - 1);
        entry->value[MAX_VALUE_SIZE - 1] = '\0';
        entry->access_time = ++shard->access_counter;
        pthread_mutex_unlock(&shard->lock);
        return;
    }
    
    // If shard is full, evict LRU entry
    if (shard->cache_count >= max_entries_per_shard) 
    {
        CacheEntry *lru = find_lru_entry(shard);
        if (lru) 
        {
            HASH_DEL(shard->cache, lru);
            free(lru);
            shard->cache_count--;
        }
    }
    
    // Create new entry
    entry = (CacheEntry*)malloc(sizeof(CacheEntry));
    if (!entry) 
    {
        pthread_mutex_unlock(&shard->lock);
        return;
    }
    
    entry->key = key;
    strncpy(entry->value, value, MAX_VALUE_SIZE - 1);
    entry->value[MAX_VALUE_SIZE - 1] = '\0';
    entry->access_time = ++shard->access_counter;
    
    HASH_ADD_INT(shard->cache, key, entry);
    shard->cache_count++;
    
    pthread_mutex_unlock(&shard->lock);
}

char* cache_get(int key) 
{
    int shard_idx = get_shard_index(key);
    CacheShard *shard = &cache_shards[shard_idx];
    
    pthread_mutex_lock(&shard->lock);
    
    CacheEntry *entry;
    HASH_FIND_INT(shard->cache, &key, entry);
    
    if (entry) 
    {
        static __thread char result[MAX_VALUE_SIZE];
        strcpy(result, entry->value);
        entry->access_time = ++shard->access_counter;
        pthread_mutex_unlock(&shard->lock);
        return result;
    }
    
    pthread_mutex_unlock(&shard->lock);
    return NULL;
}

void cache_delete(int key) 
{
    int shard_idx = get_shard_index(key);
    CacheShard *shard = &cache_shards[shard_idx];
    
    pthread_mutex_lock(&shard->lock);
    
    CacheEntry *entry;
    HASH_FIND_INT(shard->cache, &key, entry);
    
    if (entry) 
    {
        HASH_DEL(shard->cache, entry);
        free(entry);
        shard->cache_count--;
    }
    
    pthread_mutex_unlock(&shard->lock);
}

// Initialize cache
void cache_init() 
{
    for (int i = 0; i < CACHE_SHARDS; i++) 
    {
        cache_shards[i].cache = NULL;
        cache_shards[i].cache_count = 0;
        cache_shards[i].access_counter = 0;
        pthread_mutex_init(&cache_shards[i].lock, NULL);
    }
}

// Cleanup cache
void cache_cleanup() 
{
    for (int i = 0; i < CACHE_SHARDS; i++) 
    {
        pthread_mutex_lock(&cache_shards[i].lock);
        
        CacheEntry *entry, *tmp;
        HASH_ITER(hh, cache_shards[i].cache, entry, tmp) 
        {
            HASH_DEL(cache_shards[i].cache, entry);
            free(entry);
        }
        cache_shards[i].cache_count = 0;
        
        pthread_mutex_unlock(&cache_shards[i].lock);
        pthread_mutex_destroy(&cache_shards[i].lock);
    }
}

// Add operation to write batch
int add_to_batch(int key, const char* value, int operation) 
{
    pthread_mutex_lock(&write_batch.lock);
    
    if (write_batch.count >= BATCH_BUFFER_SIZE) {
        // Buffer is full, wait for flush
        write_batch.flush_requested = 1;
        pthread_cond_signal(&write_batch.cond);
        
        while (write_batch.count >= BATCH_BUFFER_SIZE) {
            pthread_cond_wait(&write_batch.cond, &write_batch.lock);
        }
    }
    
    // Add to batch
    BatchEntry *entry = &write_batch.entries[write_batch.count];
    entry->key = key;
    entry->operation = operation;
    
    if (value) {
        strncpy(entry->value, value, MAX_VALUE_SIZE - 1);
        entry->value[MAX_VALUE_SIZE - 1] = '\0';
    } else {
        entry->value[0] = '\0';
    }
    
    write_batch.count++;
    
    pthread_mutex_unlock(&write_batch.lock);
    return 1;
}

// Batch writer thread - processes batched writes
void* batch_writer_thread(void* arg) 
{
    (void)arg;
    
    while (server_running) 
    {
        pthread_mutex_lock(&write_batch.lock);
        
        // Wait for batch to fill up or timeout
        struct timespec ts;
        struct timeval tv;
        gettimeofday(&tv, NULL);
        ts.tv_sec = tv.tv_sec;
        ts.tv_nsec = tv.tv_usec * 1000;
        ts.tv_nsec += BATCH_FLUSH_INTERVAL_MS * 1000000; // Convert ms to ns
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }
        
        while (write_batch.count == 0 && !write_batch.flush_requested && server_running) {
            pthread_cond_timedwait(&write_batch.cond, &write_batch.lock, &ts);
            break; // Always break to check for timeout
        }
        
        if (write_batch.count == 0) {
            pthread_mutex_unlock(&write_batch.lock);
            continue;
        }
        
        // Copy batch for processing
        BatchEntry batch_copy[BATCH_BUFFER_SIZE];
        int batch_count = write_batch.count;
        memcpy(batch_copy, write_batch.entries, sizeof(BatchEntry) * batch_count);
        
        // Clear the batch
        write_batch.count = 0;
        write_batch.flush_requested = 0;
        pthread_cond_broadcast(&write_batch.cond);
        pthread_mutex_unlock(&write_batch.lock);
        
        // Process the batch
        if (batch_count > 0) {
            int conn_id = get_db_connection();
            MYSQL *conn = db_pool[conn_id];
            
            // Build bulk INSERT/UPDATE query
            char query[65536]; // Large buffer for bulk operations
            int query_len = 0;
            int insert_count = 0;
            int delete_count = 0;
            
            // First, handle all INSERTs/UPDATEs
            query_len = snprintf(query, sizeof(query), 
                "INSERT INTO kvstore (key_id, value_data) VALUES ");
            
            for (int i = 0; i < batch_count; i++) {
                if (batch_copy[i].operation == 0) { // INSERT/UPDATE
                    if (insert_count > 0) {
                        query_len += snprintf(query + query_len, sizeof(query) - query_len, ",");
                    }
                    
                    char escaped_value[MAX_VALUE_SIZE * 2 + 1];
                    mysql_real_escape_string(conn, escaped_value, batch_copy[i].value, strlen(batch_copy[i].value));
                    
                    query_len += snprintf(query + query_len, sizeof(query) - query_len,
                        "(%d,'%s')", batch_copy[i].key, escaped_value);
                    insert_count++;
                    
                    if (query_len > sizeof(query) - 1000) break; // Prevent buffer overflow
                }
            }
            
            if (insert_count > 0) {
                query_len += snprintf(query + query_len, sizeof(query) - query_len,
                    " ON DUPLICATE KEY UPDATE value_data = VALUES(value_data)");
                
                if (mysql_query(conn, query) != 0) {
                    fprintf(stderr, "Batch INSERT failed: %s\n", mysql_error(conn));
                }
            }
            
            // Handle DELETEs if any
            for (int i = 0; i < batch_count; i++) {
                if (batch_copy[i].operation == 1) { // DELETE
                    if (delete_count == 0) {
                        query_len = snprintf(query, sizeof(query), "DELETE FROM kvstore WHERE key_id IN (");
                    } else {
                        query_len += snprintf(query + query_len, sizeof(query) - query_len, ",");
                    }
                    
                    query_len += snprintf(query + query_len, sizeof(query) - query_len, "%d", batch_copy[i].key);
                    delete_count++;
                    
                    if (query_len > sizeof(query) - 100) break;
                }
            }
            
            if (delete_count > 0) {
                query_len += snprintf(query + query_len, sizeof(query) - query_len, ")");
                if (mysql_query(conn, query) != 0) {
                    fprintf(stderr, "Batch DELETE failed: %s\n", mysql_error(conn));
                }
            }
            
            release_db_connection(conn_id);
        }
    }
    
    return NULL;
}

// Optimized database operations using batching
int db_put(int key, const char* value) 
{
    return add_to_batch(key, value, 0); // 0 = INSERT/UPDATE
}

char* db_get(int key) 
{
    int conn_id = get_db_connection();
    MYSQL *conn = db_pool[conn_id];
    
    char query[256];
    snprintf(query, sizeof(query), "SELECT value_data FROM kvstore WHERE key_id = %d", key);
    
    static __thread char value[MAX_VALUE_SIZE];
    value[0] = '\0';
    
    if (mysql_query(conn, query) == 0) 
    {
        MYSQL_RES *result = mysql_store_result(conn);
        if (result) 
        {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (row && row[0]) 
            {
                strncpy(value, row[0], MAX_VALUE_SIZE - 1);
                value[MAX_VALUE_SIZE - 1] = '\0';
            }
            mysql_free_result(result);
        }
    }
    
    release_db_connection(conn_id);
    return (value[0] != '\0') ? value : NULL;
}

int db_delete(int key) 
{
    return add_to_batch(key, NULL, 1); // 1 = DELETE
}

// Extract key from URI path
int extract_key_from_uri(const char* uri, int* key) 
{
    if (uri[0] != '/') return 0;
    
    char *endptr;
    *key = strtol(uri + 1, &endptr, 10);
    
    return (endptr != uri + 1 && *key > 0);
}

// CivetWeb request handler (unchanged logic)
static int request_handler(struct mg_connection *conn) 
{
    const struct mg_request_info *request_info = mg_get_request_info(conn);
    const char* method = request_info->request_method;
    const char* uri = request_info->local_uri;
    
    int key;
    if (!extract_key_from_uri(uri, &key)) 
    {
        mg_printf(conn, "HTTP/1.1 400 Bad Request\r\n"
                       "Content-Type: text/plain\r\n"
                       "Content-Length: 20\r\n\r\n"
                       "Invalid key format\n");
        return 200;
    }
    
    if (strcmp(method, "GET") == 0) 
    {
        char *cached_value = cache_get(key);
        if (cached_value) 
        {
            mg_printf(conn, "HTTP/1.1 200 OK\r\n"
                           "Content-Type: text/plain\r\n"
                           "Content-Length: %d\r\n\r\n"
                           "%s\n", (int)(strlen(cached_value) + 1), cached_value);
        } 
        else 
        {
            char *db_value = db_get(key);
            if (db_value) 
            {
                cache_put(key, db_value);
                mg_printf(conn, "HTTP/1.1 200 OK\r\n"
                               "Content-Type: text/plain\r\n"
                               "Content-Length: %d\r\n\r\n"
                               "%s\n", (int)(strlen(db_value) + 1), db_value);
            } 
            else 
            {
                mg_printf(conn, "HTTP/1.1 404 Not Found\r\n"
                               "Content-Type: text/plain\r\n"
                               "Content-Length: 14\r\n\r\n"
                               "Key not found\n");
            }
        }
    }
    else if (strcmp(method, "POST") == 0 || strcmp(method, "PUT") == 0) 
    {
        char value[MAX_VALUE_SIZE];
        int data_len = mg_read(conn, value, sizeof(value) - 1);
        
        if (data_len <= 0) 
        {
            mg_printf(conn, "HTTP/1.1 400 Bad Request\r\n"
                           "Content-Type: text/plain\r\n"
                           "Content-Length: 23\r\n\r\n"
                           "Value required for POST\n");
        }
        else 
        {
            value[data_len] = '\0';
            
            char *newline = strchr(value, '\n');
            if (newline) *newline = '\0';
            newline = strchr(value, '\r');
            if (newline) *newline = '\0';
            
            if (db_put(key, value)) {
                cache_put(key, value);
                mg_printf(conn, "HTTP/1.1 200 OK\r\n"
                               "Content-Type: text/plain\r\n"
                               "Content-Length: 19\r\n\r\n"
                               "Stored successfully\n");
            } else {
                mg_printf(conn, "HTTP/1.1 500 Internal Server Error\r\n"
                               "Content-Type: text/plain\r\n"
                               "Content-Length: 15\r\n\r\n"
                               "Database error\n");
            }
        }
    }
    else if (strcmp(method, "DELETE") == 0) 
    {
        if (db_delete(key))
        {
            cache_delete(key);
            mg_printf(conn, "HTTP/1.1 200 OK\r\n"
                           "Content-Type: text/plain\r\n"
                           "Content-Length: 20\r\n\r\n"
                           "Deleted successfully\n");
        } 
        else 
        {
            mg_printf(conn, "HTTP/1.1 500 Internal Server Error\r\n"
                               "Content-Type: text/plain\r\n"
                               "Content-Length: 15\r\n\r\n"
                               "Database error\n");
        }
    }
    else 
    {
        mg_printf(conn, "HTTP/1.1 405 Method Not Allowed\r\n"
                       "Content-Type: text/plain\r\n"
                       "Content-Length: 21\r\n\r\n"
                       "Method not supported\n");
    }
    
    return 200;
}

// Signal handler for graceful shutdown
void signal_handler(int sig) 
{
    (void)sig;
    printf("\nShutting down server...\n");
    server_running = 0;
    
    if (ctx) 
    {
        mg_stop(ctx);
    }
    
    // Signal batch thread to exit
    pthread_mutex_lock(&write_batch.lock);
    pthread_cond_signal(&write_batch.cond);
    pthread_mutex_unlock(&write_batch.lock);
}

int main() 
{
    signal(SIGINT, signal_handler);
    
    cache_init();
    init_mysql();
    
    const char *options[] = 
    {
        "listening_ports", PORT,
        "num_threads", "300",           // Increased threads
        "request_timeout_ms", "10000",  
        "keep_alive_timeout_ms", "5000", 
        "max_request_size", "16384",    
        "enable_keep_alive", "yes",     
        "tcp_nodelay", "1",            
        NULL
    };
    
    struct mg_callbacks callbacks;
    memset(&callbacks, 0, sizeof(callbacks));
    callbacks.begin_request = request_handler;
    
    ctx = mg_start(&callbacks, NULL, options);
    if (ctx == NULL) 
    {
        fprintf(stderr, "Cannot start optimized CivetWeb server\n");
        return 1;
    }
    
    printf("=== Optimized CivetWeb Key-Value Server Started ===\n");
    printf("Port: %s\n", PORT);
    printf("Worker threads: 300\n");
    printf("Cache shards: %d (entries: %d)\n", CACHE_SHARDS, MAX_CACHE_ENTRIES);
    printf("Database pool: %d connections\n", DB_POOL_SIZE);
    printf("Write batching: %d entries, %dms flush interval\n", BATCH_BUFFER_SIZE, BATCH_FLUSH_INTERVAL_MS);
    printf("\nOptimizations enabled:\n");
    printf("  - Write batching with bulk INSERTs\n");
    printf("  - MySQL InnoDB optimizations\n");
    printf("  - Larger cache with more shards\n");
    printf("  - Connection compression\n");
    printf("  - Disabled durability for speed\n");
    printf("\nAPI Endpoints:\n");
    printf("  GET    /key       - Retrieve value\n");
    printf("  POST   /key       - Store value (batched)\n");
    printf("  PUT    /key       - Store value (batched)\n");
    printf("  DELETE /key       - Delete key (batched)\n");
    printf("\nPress Ctrl+C to stop...\n");
    
    while (server_running) 
    {
        sleep(1);
    }
    
    // Wait for batch thread to finish
    pthread_join(batch_thread, NULL);
    
    if (ctx) 
    {
        mg_stop(ctx);
    }
    
    // Cleanup
    for (int i = 0; i < DB_POOL_SIZE; i++) 
    {
        mysql_close(db_pool[i]);
        pthread_mutex_destroy(&db_pool_locks[i]);
    }
    
    pthread_mutex_destroy(&db_pool_counter_lock);
    pthread_cond_destroy(&db_pool_cond);
    pthread_mutex_destroy(&write_batch.lock);
    pthread_cond_destroy(&write_batch.cond);
    cache_cleanup();
    mysql_library_end();
    
    printf("Optimized server shutdown complete\n");
    return 0;
}
