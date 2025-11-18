#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>

int main(int argc, char* argv[]) 
{
    printf("Test client starting...\n");
    
    // Initialize cURL globally
    CURLcode res = curl_global_init(CURL_GLOBAL_DEFAULT);
    if (res != CURLE_OK) {
        printf("Failed to initialize cURL: %s\n", curl_easy_strerror(res));
        return 1;
    }
    
    printf("cURL initialized successfully\n");
    
    // Test basic cURL handle creation
    CURL *curl = curl_easy_init();
    if (!curl) {
        printf("Failed to create cURL handle\n");
        curl_global_cleanup();
        return 1;
    }
    
    printf("cURL handle created successfully\n");
    
    curl_easy_cleanup(curl);
    curl_global_cleanup();
    
    printf("Test completed successfully\n");
    return 0;
}
