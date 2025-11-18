#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <errno.h>
#include <netinet/tcp.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 8080
#define MAX_THREADS 10000
#define DEFAULT_TEST_DURATION 30
#define KEY_RANGE 1000000
#define POPULAR_KEYS 1000
#define BUFFER_SIZE 4096
#define LATENCY_BUFFER_SIZE 100

typedef enum { GET, POST, DELETE, MIXED, PUT_ALL, GET_ALL, GET_POPULAR, GET_PUT } ReqType;

// Thread-local latency buffer
typedef struct {
    long latencies[LATENCY_BUFFER_SIZE];
    int count;
} LatencyBuffer;

typedef struct 
{
    int thread_id;
    ReqType type;
    int requests_sent;
    int successful_requests;
    int failed_requests;
    long total_response_time_us;
    double read_percentage;
    double write_percentage;
    double delete_percentage;
    int sockfd;  // Raw socket connection
    char send_buffer[BUFFER_SIZE];
    char recv_buffer[BUFFER_SIZE];
    LatencyBuffer latency_buffer;
} ThreadStats;

volatile int stop_test = 0;
pthread_mutex_t print_lock = PTHREAD_MUTEX_INITIALIZER;
long *all_latencies = NULL;
int latency_count = 0;
pthread_mutex_t latency_lock = PTHREAD_MUTEX_INITIALIZER;

// Get current time in microseconds
long now_us() 
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000000L + tv.tv_usec);
}

// Add latency measurement to thread-local buffer
void add_latency_local(LatencyBuffer *buffer, long latency) 
{
    if (buffer->count < LATENCY_BUFFER_SIZE) {
        buffer->latencies[buffer->count++] = latency;
    }
}

// Flush thread-local latency buffer to global array
void flush_latency_buffer(LatencyBuffer *buffer) 
{
    if (buffer->count == 0) return;
    
    pthread_mutex_lock(&latency_lock);
    for (int i = 0; i < buffer->count && latency_count < MAX_THREADS * 1000; i++) {
        all_latencies[latency_count++] = buffer->latencies[i];
    }
    pthread_mutex_unlock(&latency_lock);
    
    buffer->count = 0;
}

// Compare function for qsort
int compare_longs(const void *a, const void *b) 
{
    long la = *(const long*)a;
    long lb = *(const long*)b;
    return (la > lb) - (la < lb);
}

// Calculate latency percentiles
typedef struct 
{
    long min_latency;
    long max_latency;
    long p50_latency;
    long p95_latency;
    long p99_latency;
} LatencyStats;

LatencyStats calculate_latencies() 
{
    LatencyStats stats = {0};
    
    if (latency_count == 0) return stats;
    
    qsort(all_latencies, latency_count, sizeof(long), compare_longs);
    
    stats.min_latency = all_latencies[0];
    stats.max_latency = all_latencies[latency_count - 1];
    stats.p50_latency = all_latencies[latency_count / 2];
    stats.p95_latency = all_latencies[(int)(latency_count * 0.95)];
    stats.p99_latency = all_latencies[(int)(latency_count * 0.99)];
    
    return stats;
}

// Create and configure socket connection
int create_connection() 
{
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        return -1;
    }
    
    // Enable TCP_NODELAY for low latency
    int flag = 1;
    setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    
    // Set socket to reuse address
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
    
    // Set buffer sizes
    int bufsize = 32768;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    server_addr.sin_addr.s_addr = inet_addr(SERVER_IP);
    
    if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(sockfd);
        return -1;
    }
    
    return sockfd;
}

// Send HTTP request over raw socket
int send_raw_http_request(ThreadStats *stats, const char* method, int key, const char* value, long* latency) 
{
    if (stats->sockfd < 0) return 0;
    
    int request_len;
    
    // Build HTTP request
    if (strcmp(method, "GET") == 0) {
        request_len = snprintf(stats->send_buffer, BUFFER_SIZE,
            "GET /%d HTTP/1.1\r\n"
            "Host: %s:%d\r\n"
            "Connection: keep-alive\r\n"
            "\r\n",
            key, SERVER_IP, SERVER_PORT);
    }
    else if (strcmp(method, "POST") == 0) {
        int content_len = strlen(value);
        request_len = snprintf(stats->send_buffer, BUFFER_SIZE,
            "POST /%d HTTP/1.1\r\n"
            "Host: %s:%d\r\n"
            "Connection: keep-alive\r\n"
            "Content-Length: %d\r\n"
            "\r\n"
            "%s",
            key, SERVER_IP, SERVER_PORT, content_len, value);
    }
    else if (strcmp(method, "DELETE") == 0) {
        request_len = snprintf(stats->send_buffer, BUFFER_SIZE,
            "DELETE /%d HTTP/1.1\r\n"
            "Host: %s:%d\r\n"
            "Connection: keep-alive\r\n"
            "\r\n",
            key, SERVER_IP, SERVER_PORT);
    }
    else {
        return 0;
    }
    
    long start = now_us();
    
    // Send request
    ssize_t sent = send(stats->sockfd, stats->send_buffer, request_len, MSG_NOSIGNAL);
    if (sent <= 0) {
        // Connection broken, try to reconnect
        close(stats->sockfd);
        stats->sockfd = create_connection();
        if (stats->sockfd < 0) return 0;
        
        sent = send(stats->sockfd, stats->send_buffer, request_len, MSG_NOSIGNAL);
        if (sent <= 0) return 0;
    }
    
    // Read response
    ssize_t received = recv(stats->sockfd, stats->recv_buffer, BUFFER_SIZE - 1, 0);
    long end = now_us();
    
    *latency = end - start;
    add_latency_local(&stats->latency_buffer, *latency);
    
    if (received <= 0) {
        // Connection broken
        close(stats->sockfd);
        stats->sockfd = create_connection();
        return 0;
    }
    
    stats->recv_buffer[received] = '\0';
    
    // Quick response validation - check for HTTP 200 or 404
    int success = (strstr(stats->recv_buffer, "HTTP/1.1 200") != NULL || 
                   strstr(stats->recv_buffer, "HTTP/1.1 404") != NULL);
    
    return success;
}

// Client thread function
void* client_thread(void* arg) 
{
    ThreadStats* stats = (ThreadStats*)arg;
    srand(time(NULL) + stats->thread_id);
    
    // Initialize latency buffer
    stats->latency_buffer.count = 0;
    
    // Stagger connection attempts
    usleep((stats->thread_id % 1000) * 1000);
    
    // Create socket connection
    stats->sockfd = create_connection();
    if (stats->sockfd < 0) 
    {
        printf("Thread %d: Failed to connect to server\n", stats->thread_id);
        return NULL;
    }
    
    // Pre-allocate value buffer to avoid repeated snprintf overhead
    char value[256];
    int request_count = 0;
    
    while (!stop_test) 
    {
        int key;
        long latency = 0;
        int success = 0;
        
        // Generate key based on workload type
        switch (stats->type) 
        {
            case PUT_ALL:
                key = rand() % KEY_RANGE + 1;
                snprintf(value, sizeof(value), "value_%d_%ld", key, now_us());
                if (rand() % 2 == 0) {
                    success = send_raw_http_request(stats, "POST", key, value, &latency);
                } else {
                    success = send_raw_http_request(stats, "DELETE", key, NULL, &latency);
                }
                break;
                
            case GET_ALL:
                key = rand() % KEY_RANGE + 1;
                success = send_raw_http_request(stats, "GET", key, NULL, &latency);
                break;
                
            case GET_POPULAR:
                key = rand() % POPULAR_KEYS + 1;
                success = send_raw_http_request(stats, "GET", key, NULL, &latency);
                break;
                
            case GET_PUT:
                key = rand() % KEY_RANGE + 1;
                snprintf(value, sizeof(value), "value_%d_%ld", key, now_us());
                int operation = rand() % 100;
                if (operation < 60) {  // 60% GET
                    success = send_raw_http_request(stats, "GET", key, NULL, &latency);
                } else if (operation < 85) {  // 25% POST
                    success = send_raw_http_request(stats, "POST", key, value, &latency);
                } else {  // 15% DELETE
                    success = send_raw_http_request(stats, "DELETE", key, NULL, &latency);
                }
                break;
                
            case MIXED:
                key = rand() % KEY_RANGE + 1;
                snprintf(value, sizeof(value), "value_%d_%ld", key, now_us());
                int rand_num = rand() % 100;
                if (rand_num < (int)(stats->read_percentage)) {
                    success = send_raw_http_request(stats, "GET", key, NULL, &latency);
                } else if (rand_num < (int)(stats->read_percentage + stats->write_percentage)) {
                    success = send_raw_http_request(stats, "POST", key, value, &latency);
                } else {
                    success = send_raw_http_request(stats, "DELETE", key, NULL, &latency);
                }
                break;
                
            default:
                key = rand() % KEY_RANGE + 1;
                snprintf(value, sizeof(value), "value_%d_%ld", key, now_us());
                if (stats->type == GET) {
                    success = send_raw_http_request(stats, "GET", key, NULL, &latency);
                } else if (stats->type == POST) {
                    success = send_raw_http_request(stats, "POST", key, value, &latency);
                } else if (stats->type == DELETE) {
                    success = send_raw_http_request(stats, "DELETE", key, NULL, &latency);
                }
                break;
        }

        stats->requests_sent++;
        stats->total_response_time_us += latency;
        
        if (success) 
        {
            stats->successful_requests++;
        } else 
        {
            stats->failed_requests++;
        }
        
        // Batch operations and reduce overhead
        request_count++;
        
        // Flush latency buffer periodically to reduce contention
        if (stats->latency_buffer.count >= LATENCY_BUFFER_SIZE - 10) {
            flush_latency_buffer(&stats->latency_buffer);
        }
        
        // Optional: Reduce CPU usage by yielding occasionally (remove for max performance)
        // if (request_count % 1000 == 0) {
        //     usleep(1);
        // }
    }
    
    // Final flush of latency buffer
    flush_latency_buffer(&stats->latency_buffer);
    
    // Cleanup socket
    if (stats->sockfd >= 0) {
        close(stats->sockfd);
    }

    return NULL;
}

// Signal handler for graceful shutdown
void signal_handler(int sig) 
{
    (void)sig;
    printf("\nStopping load test...\n");
    stop_test = 1;
}

// Print usage information
void print_usage(const char* program_name) {
    printf("Usage: %s [OPTIONS]\n", program_name);
    printf("Options:\n");
    printf("  -t <threads>     Number of client threads (default: 10)\n");
    printf("  -d <duration>    Test duration in seconds (default: 30)\n");
    printf("  -o <operation>   Operation type (default: mixed):\n");
    printf("                   get, post, delete, mixed, put-all, get-all, get-popular, get-put\n");
    printf("  -r <percentage>  Read percentage for mixed workload (default: 70)\n");
    printf("  -w <percentage>  Write percentage for mixed workload (default: 25)\n");
    printf("  -h               Show this help message\n");
    printf("\nWorkload Types:\n");
    printf("  get          - Only GET requests\n");
    printf("  post         - Only POST requests\n"); 
    printf("  delete       - Only DELETE requests\n");
    printf("  mixed        - Custom mix of operations (use -r and -w to specify)\n");
    printf("  put-all      - Only POST/DELETE (disk-bound, cache misses)\n");
    printf("  get-all      - Only GET with unique keys (disk-bound, cache misses)\n");
    printf("  get-popular  - Only GET with popular keys (CPU/memory-bound, cache hits)\n");
    printf("  get-put      - Mixed GET/POST/DELETE (60%%/25%%/15%%)\n");
    printf("\nExamples:\n");
    printf("  %s -t 50 -d 60 -o get-popular      # Cache hit workload\n", program_name);
    printf("  %s -t 1000 -o put-all              # High-throughput disk workload\n", program_name);
    printf("  %s -t 500 -o mixed -r 80 -w 15     # Custom mixed workload\n", program_name);
}

int main(int argc, char* argv[]) 
{
    int num_threads = 10;
    int test_duration = DEFAULT_TEST_DURATION;
    ReqType req_type = MIXED;
    double read_percentage = 70.0;
    double write_percentage = 25.0;
    double delete_percentage = 5.0;

    // Parse command line arguments
    int opt;
    while ((opt = getopt(argc, argv, "t:d:o:r:w:h")) != -1) 
    {
        switch (opt) 
        {
            case 't':
                num_threads = atoi(optarg);
                if (num_threads > MAX_THREADS) {
                    printf("Warning: Requested %d threads, limiting to %d\n", num_threads, MAX_THREADS);
                    num_threads = MAX_THREADS;
                }
                break;
            case 'd':
                test_duration = atoi(optarg);
                break;
            case 'o':
                if (strcmp(optarg, "get") == 0) req_type = GET;
                else if (strcmp(optarg, "post") == 0) req_type = POST;
                else if (strcmp(optarg, "delete") == 0) req_type = DELETE;
                else if (strcmp(optarg, "mixed") == 0) req_type = MIXED;
                else if (strcmp(optarg, "put-all") == 0) req_type = PUT_ALL;
                else if (strcmp(optarg, "get-all") == 0) req_type = GET_ALL;
                else if (strcmp(optarg, "get-popular") == 0) req_type = GET_POPULAR;
                else if (strcmp(optarg, "get-put") == 0) req_type = GET_PUT;
                else {
                    printf("Invalid operation type: %s\n", optarg);
                    return 1;
                }
                break;
            case 'r':
                read_percentage = atof(optarg);
                break;
            case 'w':
                write_percentage = atof(optarg);
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    // Validate percentages
    if (req_type == MIXED) {
        delete_percentage = 100.0 - read_percentage - write_percentage;
        if (delete_percentage < 0) {
            printf("Error: Read and write percentages sum to more than 100%%\n");
            return 1;
        }
    }

    // Set up signal handler
    signal(SIGINT, signal_handler);

    // Allocate memory for latency tracking
    all_latencies = malloc(sizeof(long) * MAX_THREADS * 1000);
    if (!all_latencies) {
        printf("Failed to allocate memory for latency tracking\n");
        return 1;
    }

    // Allocate thread arrays on heap
    pthread_t *threads = malloc(sizeof(pthread_t) * num_threads);
    ThreadStats *stats = calloc(num_threads, sizeof(ThreadStats));
    
    if (!threads || !stats) {
        printf("Failed to allocate memory for threads\n");
        return 1;
    }

    printf("=== Raw Socket Key-Value Server Load Test ===\n");
    printf("Threads: %d\n", num_threads);
    printf("Duration: %d seconds\n", test_duration);
    printf("Server: http://%s:%d\n", SERVER_IP, SERVER_PORT);
    printf("Connection: Raw TCP sockets with HTTP/1.1 keep-alive\n");
    
    // Display workload type
    printf("Workload: ");
    switch (req_type) {
        case GET: printf("GET only (read-only)\n"); break;
        case POST: printf("POST only (write-only)\n"); break;
        case DELETE: printf("DELETE only (delete-only)\n"); break;
        case MIXED: printf("Mixed (%.1f%% read, %.1f%% write, %.1f%% delete)\n", 
                           read_percentage, write_percentage, delete_percentage); break;
        case PUT_ALL: printf("PUT_ALL (50%% POST, 50%% DELETE - disk-bound)\n"); break;
        case GET_ALL: printf("GET_ALL (unique keys - disk-bound)\n"); break;
        case GET_POPULAR: printf("GET_POPULAR (popular keys 1-%d - CPU/memory-bound)\n", POPULAR_KEYS); break;
        case GET_PUT: printf("GET_PUT (60%% GET, 25%% POST, 15%% DELETE)\n"); break;
    }
    
    printf("Key range: 1 to %d\n", KEY_RANGE);
    printf("\nStarting load test...\n");

    // Start all client threads
    for (int i = 0; i < num_threads; i++) 
    {
        stats[i].thread_id = i;
        stats[i].type = req_type;
        stats[i].read_percentage = read_percentage;
        stats[i].write_percentage = write_percentage;
        stats[i].delete_percentage = delete_percentage;
        stats[i].sockfd = -1;
        
        if (pthread_create(&threads[i], NULL, client_thread, &stats[i]) != 0) {
            printf("Failed to create thread %d\n", i);
            return 1;
        }
    }

    // Run test for specified duration
    sleep(test_duration);
    stop_test = 1;

    // Wait for all threads to complete
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Aggregate results
    long total_requests = 0;
    long total_successful = 0;
    long total_failed = 0;
    long total_time_us = 0;
    
    for (int i = 0; i < num_threads; i++) {
        total_requests += stats[i].requests_sent;
        total_successful += stats[i].successful_requests;
        total_failed += stats[i].failed_requests;
        total_time_us += stats[i].total_response_time_us;
    }

    // Calculate performance metrics
    double throughput = total_requests / (double)test_duration;
    double success_rate = (total_requests > 0) ? (total_successful * 100.0 / total_requests) : 0;
    double avg_latency_ms = (total_requests > 0) ? (total_time_us / 1000.0) / total_requests : 0;

    // Calculate latency percentiles
    LatencyStats latency_stats = calculate_latencies();

    // Print results
    printf("\n=== Raw Socket Load Test Results ===\n");
    printf("Total Requests:        %ld\n", total_requests);
    printf("Successful Requests:   %ld (%.2f%%)\n", total_successful, success_rate);
    printf("Failed Requests:       %ld\n", total_failed);
    printf("Throughput:            %.2f requests/second\n", throughput);
    printf("Average Latency:       %.2f ms\n", avg_latency_ms);
    
    if (latency_count > 0) {
        printf("Latency Percentiles:\n");
        printf("  Min:                 %.2f ms\n", latency_stats.min_latency / 1000.0);
        printf("  50th percentile:     %.2f ms\n", latency_stats.p50_latency / 1000.0);
        printf("  95th percentile:     %.2f ms\n", latency_stats.p95_latency / 1000.0);
        printf("  99th percentile:     %.2f ms\n", latency_stats.p99_latency / 1000.0);
        printf("  Max:                 %.2f ms\n", latency_stats.max_latency / 1000.0);
    }

    // Cleanup
    free(all_latencies);
    free(threads);
    free(stats);
    pthread_mutex_destroy(&print_lock);
    pthread_mutex_destroy(&latency_lock);

    return 0;
}
