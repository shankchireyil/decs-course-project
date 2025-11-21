#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <mysql/mysql.h>
#include <signal.h>
#include "uthash.h"  // uthash library for hash table
#include "civetweb/include/civetweb.h"

#define PORT "8080"
#define MAX_CACHE_ENTRIES 1000
#define MAX_VALUE_SIZE 512
#define DB_POOL_SIZE 100
#define CACHE_SHARDS 200

// Cache entry structure for uthash
typedef struct CacheEntry 
{
    int key;                    // key (must be int for uthash)
    char value[MAX_VALUE_SIZE]; // stored value
    int access_time;            // for LRU eviction
    UT_hash_handle hh;          // makes this structure hashable

} CacheEntry;

// Cache sharding for reduced contention
typedef struct CacheShard 
{
    CacheEntry *cache;
    int cache_count;
    int access_counter;
    pthread_mutex_t lock;

} CacheShard;

// Global variables
CacheShard cache_shards[CACHE_SHARDS];
int max_entries_per_shard = MAX_CACHE_ENTRIES / CACHE_SHARDS;

// Database connection pool
MYSQL *db_pool[DB_POOL_SIZE];
pthread_mutex_t db_pool_locks[DB_POOL_SIZE];
int db_available[DB_POOL_SIZE];
pthread_mutex_t db_pool_counter_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t db_pool_cond = PTHREAD_COND_INITIALIZER;

volatile int server_running = 1;
struct mg_context *ctx = NULL;

// Initialize MySQL connection pool
void init_mysql() 
{
    for (int i = 0; i < DB_POOL_SIZE; i++) 
    {
        db_pool[i] = mysql_init(NULL);
        if (!db_pool[i]) 
        {
            fprintf(stderr, "mysql_init() failed for connection %d\n", i);
            exit(1);
        }


        if (!mysql_real_connect(db_pool[i], "localhost", "decsuser", "decs123", "decsdb", 0, NULL, 0)) 
        {
            fprintf(stderr, "mysql_real_connect() failed: %s\n", mysql_error(db_pool[i]));
            exit(1);
        }
        
        pthread_mutex_init(&db_pool_locks[i], NULL);
        db_available[i] = 1;  // Mark as available
    }

    // Create table using first connection
    const char *create_table = "CREATE TABLE IF NOT EXISTS kvstore ("
                               "key_id INT PRIMARY KEY, "
                               "value_data VARCHAR(512))";
    
    if (mysql_query(db_pool[0], create_table)) 
    {
        fprintf(stderr, "Failed to create table: %s\n", mysql_error(db_pool[0]));
        exit(1);
    }

    printf("Connected to MySQL with %d connections\n", DB_POOL_SIZE);
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
        entry->access_time = ++shard->access_counter;  // Update LRU
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

// Initialize cache (called at startup)
void cache_init() 
{
    for (int i = 0; i < CACHE_SHARDS; i++) 
    {
        cache_shards[i].cache = NULL;  // uthash requires NULL initialization
        cache_shards[i].cache_count = 0;
        cache_shards[i].access_counter = 0;
        pthread_mutex_init(&cache_shards[i].lock, NULL);
    }
}

// Cleanup cache (called at shutdown)
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

// Database operations
int db_put(int key, const char* value) 
{
    int conn_id = get_db_connection();
    MYSQL *conn = db_pool[conn_id];
    
    char query[4096];
    char escaped_value[MAX_VALUE_SIZE * 2 + 1];
    
    mysql_real_escape_string(conn, escaped_value, value, strlen(value));
    
    int query_len = snprintf(query, sizeof(query), 
                            "INSERT INTO kvstore (key_id, value_data) VALUES (%d, '%s') "
                            "ON DUPLICATE KEY UPDATE value_data = '%s'", 
                            key, escaped_value, escaped_value);
    
    int result = 0;
    if (query_len < (int)sizeof(query)) 
    {
        result = (mysql_query(conn, query) == 0);
    }
    
    release_db_connection(conn_id);
    return result;
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
    int conn_id = get_db_connection();
    MYSQL *conn = db_pool[conn_id];
    
    char query[256];
    snprintf(query, sizeof(query), "DELETE FROM kvstore WHERE key_id = %d", key);
    
    int result = (mysql_query(conn, query) == 0);
    
    release_db_connection(conn_id);
    return result;
}

// Extract key from URI path
int extract_key_from_uri(const char* uri, int* key) 
{
    // Expected format: /key or /key/value
    if (uri[0] != '/') return 0;
    
    char *endptr;
    *key = strtol(uri + 1, &endptr, 10);
    
    // Check if conversion was successful and we have a valid key
    return (endptr != uri + 1 && *key > 0);
}

// CivetWeb request handler
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
        return 200; // Handler processed the request
    }
    
    if (strcmp(method, "GET") == 0) 
    {
        // Try cache first
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
            // Try database
            char *db_value = db_get(key);
            if (db_value) 
            {
                cache_put(key, db_value);  // Cache the result
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
        // Read request body for value
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
            
            // Remove newlines if any
            char *newline = strchr(value, '\n');
            if (newline) *newline = '\0';
            newline = strchr(value, '\r');
            if (newline) *newline = '\0';
            
            if (db_put(key, value)) {
                // cache_put(key, value);  // Update cache
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
            cache_delete(key);  // Remove from cache
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
    
    return 200; // Handler processed the request
}

// Signal handler for graceful shutdown
void signal_handler(int sig) 
{
    (void)sig; // Suppress unused parameter warning
    printf("\nShutting down server...\n");
    server_running = 0;
    
    if (ctx) 
    {
        mg_stop(ctx);
    }
}

int main() 
{
    // Set up signal handler
    signal(SIGINT, signal_handler);
    
    // Initialize cache
    cache_init();
    
    // Initialize MySQL
    init_mysql();
    
    // CivetWeb options
    const char *options[] = 
    {
        "listening_ports", PORT,
        "num_threads", "200",           // Number of worker threads
        "request_timeout_ms", "10000",  // 10 second timeout
        "keep_alive_timeout_ms", "5000", // 5 second keep-alive
        "max_request_size", "16384",    // 16KB max request size
        "enable_keep_alive", "yes",     // Enable HTTP keep-alive
        "tcp_nodelay", "1",            // Enable TCP_NODELAY
        NULL
    };
    
    struct mg_callbacks callbacks;
    memset(&callbacks, 0, sizeof(callbacks));
    callbacks.begin_request = request_handler;
    
    // Start CivetWeb server
    ctx = mg_start(&callbacks, NULL, options);
    if (ctx == NULL) 
    {
        fprintf(stderr, "Cannot start CivetWeb server\n");
        return 1;
    }
    
    printf("=== CivetWeb Key-Value Server Started ===\n");
    printf("Port: %s\n", PORT);
    printf("Worker threads: 200\n");
    printf("Cache shards: %d\n", CACHE_SHARDS);
    printf("Cache entries: %d\n", MAX_CACHE_ENTRIES);
    printf("Database pool: %d connections\n", DB_POOL_SIZE);
    printf("\nAPI Endpoints:\n");
    printf("  GET    /key       - Retrieve value\n");
    printf("  POST   /key       - Store value (body contains value)\n");
    printf("  PUT    /key       - Store value (body contains value)\n");
    printf("  DELETE /key       - Delete key\n");
    printf("\nPress Ctrl+C to stop...\n");
    
    // Keep server running
    while (server_running) 
    {
        sleep(1);
    }
    
    // Cleanup
    if (ctx) 
    {
        mg_stop(ctx);
    }
    
    // Clean up database pool
    for (int i = 0; i < DB_POOL_SIZE; i++) 
    {
        mysql_close(db_pool[i]);
        pthread_mutex_destroy(&db_pool_locks[i]);
    }
    
    pthread_mutex_destroy(&db_pool_counter_lock);
    pthread_cond_destroy(&db_pool_cond);
    cache_cleanup();
    
    printf("Server shutdown complete\n");
    return 0;
}
