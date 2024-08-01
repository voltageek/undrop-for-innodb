#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#define _XOPEN_SOURCE 600
#include <fcntl.h>
// #ifndef _GNU_SOURCE
// #define _GNU_SOURCE // for O_NOATIME
// #define O_NOATIME 01000000
// #endif
#include <time.h>
#include <libgen.h>
#include <limits.h>
#include <stdarg.h>

#ifdef __APPLE__
#include <dispatch/dispatch.h>
#else
#include <semaphore.h>
#endif

#include "mysql_def.h"
#include "zlib/zlib.h"

#define _GNU_SOURCE
#include <pthread.h>
#include <unistd.h>
#include <execinfo.h>
#include <signal.h>

#define BUF_NO_CHECKSUM_MAGIC 0xDEADBEEFU
#define FIL_PAGE_COMP_METADATA_LEN 2
#define FIL_PAGE_COMP_SIZE 0

#define UNIV_PAGE_SIZE_SHIFT_MAX 16U
#define UNIV_PAGE_SIZE_MAX (1U << UNIV_PAGE_SIZE_SHIFT_MAX)
#define PAGE_UNCOMPRESSED 0
#define PAGE_ZLIB_ALGORITHM 1
#define PAGE_LZ4_ALGORITHM 2
#define PAGE_LZO_ALGORITHM 3
#define PAGE_LZMA_ALGORITHM 4
#define PAGE_BZIP2_ALGORITHM 5
#define PAGE_SNAPPY_ALGORITHM 6
// #define STREAM_PARSER_DEBUG

#define THREAD_POOL_SIZE 4
#define CHUNK_SIZE (1024 * 1024 * 10) // 10MB chunks
#define PAGE_DATA 38
#define PAGE_LEVEL 26
#define PAGE_N_DIRECTION 54
#define PAGE_DIRECTION 56
#define PAGE_N_DIRECTION_BYTES 2


pthread_mutex_t global_mutex;
off_t next_chunk_start = 0;
int fn = 0;
off_t initial_offset = 0;
uint32_t page_size = UNIV_PAGE_SIZE;

void segfault_handler(int signal) {
    fprintf(stderr, "Caught segmentation fault!\n");

    // Print stack trace
    void *array[10];
    size_t size;
    char **strings;
    size = backtrace(array, 10);
    strings = backtrace_symbols(array, size);

    fprintf(stderr, "Obtained %zd stack frames.\n", size);

    for (size_t i = 0; i < size; i++) {
        fprintf(stderr, "%s\n", strings[i]);
    }

    free(strings);

    exit(EXIT_FAILURE);
}

// Read integers of various sizes
uint64_t mach_read_from_n(const unsigned char *buf, int n) {
    uint64_t value = 0;
    for (int i = 0; i < n; i++) {
        value = (value << 8) | buf[i];
    }
    return value;
}


uint64_t mach_read_from_6(page_t *b)
{
    uint32_t high;
    uint32_t low;

    high = mach_read_from_2(b);
    low = mach_read_from_4(b + 2);
    return ((uint64_t)high << 32) + (uint64_t)low;
}

uint32_t safe_read_uint32(const page_t *page, size_t offset, size_t max_size) {
    if (offset + 4 > max_size) {
        printf("Debug: Attempted to read beyond buffer in safe_read_uint32\n");
        return 0;
    }
    return mach_read_from_4(page + offset);
}

// Global flags from getopt
#ifdef STREAM_PARSER_DEBUG
int debug = 1;
#else
int debug = 0;
#endif
int info_log = 0;

uint64_t filter_id;
int use_filter_id = 0;

ssize_t cache_size = 8 * 1024 * 1024; // 8M
off_t ib_size = 0;
uint64_t max_page_id = 0;
char dst_dir[1024] = "";
int worker = 0;
#define mutext_pool_size (8)
#ifdef __APPLE__
dispatch_semaphore_t index_mutex[mutext_pool_size];
dispatch_semaphore_t blob_mutex[mutext_pool_size];
#else
sem_t index_mutex[mutext_pool_size];
sem_t blob_mutex[mutext_pool_size];
#endif

void usage(char *);
inline int valid_innodb_page(page_t* page, uint64_t block_size, off_t* step);

void log_error(const char *format, ...) {
    va_list args;
    va_start(args, format);
    fprintf(stderr, "ERROR: ");
    vfprintf(stderr, format, args);
    fprintf(stderr, "\n");
    va_end(args);
}

void show_progress(off_t offset, off_t length) {
    static time_t last_update = 0;
    time_t now = time(NULL);
    if (now - last_update < 1) return; // Update at most once per second

    double progress = (double)offset / length;
    int bar_width = 50;
    int pos = bar_width * progress;

    printf("[");
    for (int i = 0; i < bar_width; ++i) {
        if (i < pos) printf("=");
        else if (i == pos) printf(">");
        else printf(" ");
    }
    printf("] %3d%%\r", (int)(progress * 100));
    fflush(stdout);

    last_update = now;
}

int DEBUG_LOG(char *format, ...)
{
    if (debug)
    {
        char msg[1024] = "";
        // sprintf(format, "Worker(%d): %s\n", worker, fmt);
        va_list args;
        va_start(args, format);
        vsprintf(msg, format, args);
        va_end(args);
        return fprintf(stderr, "Worker(%d): %s\n", worker, msg);
    }
    return 0;
}

int INFO_LOG(char *format, ...)
{
    if (info_log)
    {
        char msg[1024] = "";
        // sprintf(format, "Worker(%d): %s\n", worker, fmt);
        va_list args;
        va_start(args, format);
        vsprintf(msg, format, args);
        va_end(args);
        return fprintf(stderr, "Worker(%d): %s\n", worker, msg);
    }
    return 0;
}

void error(char *msg)
{
    fprintf(stderr, "Error: %s\n", msg);
    exit(EXIT_FAILURE);
}
// Prints size in human readable form
char *h_size(unsigned long long int size, char *buf)
{
    unsigned int power = 0;
    double d_size = size;
    while (d_size >= 1024)
    {
        d_size /= 1024;
        power += 3;
    }
    sprintf(buf, "%3.3f", d_size);
    switch (power)
    {
    case 3:
        sprintf(buf, "%s %s", buf, "kiB");
        break;
    case 6:
        sprintf(buf, "%s %s", buf, "MiB");
        break;
    case 9:
        sprintf(buf, "%s %s", buf, "GiB");
        break;
    case 12:
        sprintf(buf, "%s %s", buf, "TiB");
        break;
    default:
        sprintf(buf, "%s exp(+%u)", buf, power);
        break;
    }
    return buf;
}

void create_invalid_pages_dir(const char *base_dir) {
    char invalid_dir[1024];
    snprintf(invalid_dir, sizeof(invalid_dir), "%s-invalid", base_dir);
    if (mkdir(invalid_dir, 0755) == -1 && errno != EEXIST) {
        fprintf(stderr, "Could not create directory %s\n", invalid_dir);
        perror("mkdir()");
        exit(EXIT_FAILURE);
    }
}

uint32_t get_page_id(page_t *page)
{
    return mach_read_from_4(page + FIL_PAGE_OFFSET);
}

uint64_t fil_page_decompress(
    page_t *tmp_buf,
    page_t *buf)
{
    uint header_len;
    uint comp_algo;

    header_len = FIL_PAGE_DATA + FIL_PAGE_COMP_METADATA_LEN;
    if (mach_read_from_6(FIL_PAGE_FILE_FLUSH_LSN + buf))
    {
        return 0;
    }
    comp_algo = mach_read_from_2(FIL_PAGE_FILE_FLUSH_LSN + 6 + buf);

    if (mach_read_from_4(buf + FIL_PAGE_SPACE_OR_CHKSUM) != BUF_NO_CHECKSUM_MAGIC)
    {
        return 0;
    }

    uint64_t actual_size = mach_read_from_2(buf + FIL_PAGE_DATA + FIL_PAGE_COMP_SIZE);

    if (actual_size == 0 || actual_size > cache_size - header_len)
    {
        return 0;
    }

    switch (comp_algo)
    {
    case PAGE_ZLIB_ALGORITHM:
    {
        ssize_t len = UNIV_PAGE_SIZE;
        int result = R_OK == uncompress(tmp_buf, &len, buf + header_len, actual_size);
        if (!(result && len == UNIV_PAGE_SIZE))
        {
            return 0;
        }

        memcpy(buf, tmp_buf, UNIV_PAGE_SIZE);
        return actual_size;
    }
    default:
    {
        fprintf(stderr, "Unsupported page compression algorithm %i\n", comp_algo);
        return 0;
    }
    }
}

inline int valid_innodb_checksum(page_t *p)
{
    uint32_t oldcsum, oldcsumfield, csum, csumfield;
    int result = 0;

    // Checking old style checksums
    oldcsum = buf_calc_page_old_checksum(p);
    oldcsumfield = mach_read_from_4(p + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM);
#ifdef STREAM_PARSER_DEBUG
    DEBUG_LOG("Old checksum position %08X", UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM);
#endif
    // stream_parser.c:104:5: warning: implicit declaration of function ‘buf_calc_page_crc32’ [-Wimplicit-function-declaration]
    // #ifdef STREAM_PARSER_DEBUG
    //    DEBUG_LOG("Old checksum: calculated=%lu, crc32=%lu, stored=%lu", oldcsum, buf_calc_page_crc32(p), oldcsumfield);
    // #endif
    if (oldcsumfield != oldcsum)
    {
#ifdef STREAM_PARSER_DEBUG
        DEBUG_LOG("Old checksum: calculated=%08X, stored=%08X", oldcsum, oldcsumfield);
#endif
        result = 0;
        goto valid_innodb_checksum_exit;
    }
    // Checking new style checksums
    csum = buf_calc_page_new_checksum(p);
    csumfield = mach_read_from_4(p + FIL_PAGE_SPACE_OR_CHKSUM);
    if (csumfield != 0 && csum != csumfield)
    {
#ifdef STREAM_PARSER_DEBUG
        DEBUG_LOG("New checksum: calculated=%u, stored=%u", csum, csumfield);
#endif
        result = 0;
        goto valid_innodb_checksum_exit;
    }
    // return success
    result = 1;
valid_innodb_checksum_exit:
#ifdef STREAM_PARSER_DEBUG
    DEBUG_LOG("Checksum is good = %u", result);
#endif

    return result;
}

inline int valid_blob_page(page_t *page)
{
    uint16_t page_type = mach_read_from_2(page + FIL_PAGE_TYPE);
    uint32_t page_id = mach_read_from_4(page + FIL_PAGE_OFFSET);
#ifdef STREAM_PARSER_DEBUG
    DEBUG_LOG("Checking page id = %u", page_id);
#endif

    if (page_type != FIL_PAGE_TYPE_BLOB)
    {
#ifdef STREAM_PARSER_DEBUG
        DEBUG_LOG("Wrong page type (blob) = %u", page_type);
#endif
        return 0;
    }
    page_t *blob_header = page + FIL_PAGE_DATA;
    uint32_t part_len = mach_read_from_4(blob_header + 0 /*BTR_BLOB_HDR_PART_LEN*/);
    if (part_len > UNIV_PAGE_SIZE)
    {
#ifdef STREAM_PARSER_DEBUG
        DEBUG_LOG("Wrong part len = %u", part_len);
#endif
        return 0;
    }
    if (page_id > max_page_id || page_id == 0)
    {
#ifdef STREAM_PARSER_DEBUG
        DEBUG_LOG("Wrong page id %u. Maximum page_ud %u", page_id, max_page_id);
#endif
        return 0;
    }
    uint32_t next_page = mach_read_from_4(blob_header + 4 /*BTR_BLOB_HDR_NEXT_PAGE_NO*/);
    if (next_page != 0xFFFFFFFF && (next_page > max_page_id || next_page == 0))
    {
#ifdef STREAM_PARSER_DEBUG
        DEBUG_LOG("Wrong next page_id = %u", next_page);
#endif
        return 0;
    }
#ifdef STREAM_PARSER_DEBUG
    DEBUG_LOG("All criterias are good. Checking checksum");
#endif
    return valid_innodb_checksum(page);
}

inline int valid_mysql_compressed_page(page_t *page)
{
    uint16_t page_type = mach_read_from_2(page + FIL_PAGE_TYPE);
    uint32_t page_id = mach_read_from_4(page + FIL_PAGE_OFFSET);
#ifdef STREAM_PARSER_DEBUG
    DEBUG_LOG("Checking page id = %u", page_id);
#endif

    if (page_type != 14)
    {
#ifdef STREAM_PARSER_DEBUG
        DEBUG_LOG("Wrong page type (mysql compressed) = %u", page_type);
#endif
        return 0;
    }
    // TODO: incomplete support for mysql compressed page, but just assume its correct to count it
    return 1;
}

inline int valid_mariadb_compressed_page(page_t *page, uint64_t block_size, page_t *tmp_page)
{
    uint16_t page_type = mach_read_from_2(page + FIL_PAGE_TYPE);
    uint32_t page_id = mach_read_from_4(page + FIL_PAGE_OFFSET);

#ifdef STREAM_PARSER_DEBUG
    DEBUG_LOG("Checking page id (mariadb compressed) = %u", page_id);
#endif

    if (page_type != 34354)
    {
#ifdef STREAM_PARSER_DEBUG
        DEBUG_LOG("Wrong page type (mariadb compressed) = %u", page_type);
#endif
        return 0;
    }

    uint16_t compression_format = mach_read_from_8(page + FIL_PAGE_FILE_FLUSH_LSN);

    page_t *tmp_frame = NULL;
    tmp_frame = malloc(UNIV_PAGE_SIZE_MAX);

    memcpy(tmp_page, page, UNIV_PAGE_SIZE_MAX);

    uint64_t decomp = fil_page_decompress(tmp_frame, tmp_page);
    if (!decomp)
    {
#ifdef STREAM_PARSER_DEBUG
        DEBUG_LOG("Page decompression failed");
#endif
        return 0;
    }
    off_t cache_step = 1;

    int is_valid_innodb_page = valid_innodb_page(tmp_page, block_size, &cache_step);
#ifdef STREAM_PARSER_DEBUG
    DEBUG_LOG("Page decompression %i result valid, %i checksum", is_valid_innodb_page, valid_innodb_checksum(tmp_page));
#endif
    return is_valid_innodb_page;
}

inline int valid_innodb_page(page_t *page, uint64_t block_size, off_t *step)
{
    printf("Debug: Entering valid_innodb_page function\n");
    
    if (page == NULL || step == NULL || block_size < page_size) {
        printf("Debug: Invalid parameters passed to valid_innodb_page\n");
        return 0;
    }

    *step = 1;

    // Print the first 16 bytes of the page for debugging
    printf("Debug: Page contents (first 16 bytes): ");
    for (int i = 0; i < 16 && i < block_size; i++) {
        printf("%02x ", (unsigned char)(page[i]));
    }
    printf("\n");

    if (block_size < FIL_PAGE_OFFSET + 4) {
        printf("Debug: Block size too small to contain necessary fields\n");
        return 0;
    }

    uint32_t page_id = mach_read_from_4(page + FIL_PAGE_OFFSET);
    printf("Debug: Page ID: %u\n", page_id);

    if (page_id > max_page_id) {
        printf("Debug: Invalid page ID\n");
        return 0;
    }

    // Check for 'infimum' and 'supremum' records, but don't fail if not found
    const char *infimum = "infimum";
    const char *supremum = "supremum";
    int inf_offset = PAGE_NEW_INFIMUM;
    int sup_offset = PAGE_NEW_SUPREMUM;

    if (inf_offset + 7 <= block_size && memcmp(page + inf_offset, infimum, 7) == 0) {
        printf("Debug: 'infimum' record found\n");
    } else {
        printf("Debug: 'infimum' record not found, but continuing\n");
    }

    if (sup_offset + 8 <= block_size && memcmp(page + sup_offset, supremum, 8) == 0) {
        printf("Debug: 'supremum' record found\n");
    } else {
        printf("Debug: 'supremum' record not found, but continuing\n");
    }

    printf("Debug: Considering page as valid InnoDB page\n");
    return 1;
}

inline void process_ibpage(page_t *page)
{
    printf("Debug: Entering process_ibpage\n");

    if (page == NULL) {
        fprintf(stderr, "Error: Null page passed to process_ibpage\n");
        return;
    }

    uint32_t page_id = mach_read_from_4(page + FIL_PAGE_OFFSET);
    uint64_t index_id = mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID);
    uint16_t page_type = mach_read_from_2(page + FIL_PAGE_TYPE);
    uint64_t lsn = mach_read_from_8(page + FIL_PAGE_LSN);
    uint32_t space_id = mach_read_from_4(page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
    uint16_t page_level = mach_read_from_2(page + PAGE_HEADER + PAGE_LEVEL);
    uint16_t n_recs = mach_read_from_2(page + PAGE_HEADER + PAGE_N_RECS);
    uint16_t n_heap = mach_read_from_2(page + PAGE_HEADER + PAGE_N_HEAP);
    uint16_t n_direction = mach_read_from_2(page + PAGE_HEADER + PAGE_N_DIRECTION);
    uint16_t direction = mach_read_from_2(page + PAGE_HEADER + PAGE_DIRECTION);

    printf("Page Analysis:\n");
    printf("  Page ID: %u\n", page_id);
    printf("  Index ID: %llu\n", (unsigned long long)index_id);
    printf("  Page Type: %u\n", page_type);
    printf("  LSN: %llu\n", (unsigned long long)lsn);
    printf("  Space ID: %u\n", space_id);
    printf("  Page Level: %u\n", page_level);
    printf("  Number of Records: %u\n", n_recs);
    printf("  Number of Heap: %u\n", n_heap);
    printf("  Number of Direction: %u\n", n_direction);
    printf("  Direction: %u\n", direction);

    // Basic check for page data
    printf("Data Recovery:\n");
    page_t *data_start = page + PAGE_DATA;
    
    printf("  First 100 bytes of data (hex):\n  ");
    for (int i = 0; i < 100 && i < (UNIV_PAGE_SIZE - PAGE_DATA); i++) {
        printf("%02x ", data_start[i]);
        if ((i + 1) % 16 == 0) printf("\n  ");
    }
    printf("\n");

    printf("  First 100 bytes of data (ASCII):\n  ");
    for (int i = 0; i < 100 && i < (UNIV_PAGE_SIZE - PAGE_DATA); i++) {
        printf("%c", (data_start[i] >= 32 && data_start[i] <= 126) ? data_start[i] : '.');
        if ((i + 1) % 50 == 0) printf("\n  ");
    }
    printf("\n");
    
    
    char file_name[1024] = "";
    int flags;
    if (page_type == FIL_PAGE_INDEX)
    {

#ifdef __APPLE__
        sprintf(file_name, "%s/FIL_PAGE_INDEX/%016llu.page", dst_dir, (unsigned long long)index_id);
#else
        sprintf(file_name, "%s/FIL_PAGE_INDEX/%016lu.page", dst_dir, index_id);
#endif
        flags = O_WRONLY | O_CREAT | O_APPEND;
    }
    else
    {
        sprintf(file_name, "%s/FIL_PAGE_TYPE_BLOB/%016u.page", dst_dir, page_id);
        flags = O_WRONLY | O_CREAT | O_APPEND;
    }

    printf("Debug: Attempting to open file: %s\n", file_name);
    int fn = open(file_name, flags, 0644);
    if (fn == -1)
    {
        fprintf(stderr, "Error: Can't open file to save page: %s\n", strerror(errno));
        return;
    }

    printf("Debug: File opened successfully. Writing page...\n");

    ssize_t written = write(fn, page, UNIV_PAGE_SIZE);
    if (written == -1)
    {
        fprintf(stderr, "Error: Can't write page to disk: %s\n", strerror(errno));
        close(fn);
        return;
    }
    else if (written != UNIV_PAGE_SIZE)
    {
        fprintf(stderr, "Warning: Incomplete page write. Expected %d bytes, wrote %zd bytes\n", UNIV_PAGE_SIZE, written);
    }

    printf("Debug: Page written successfully\n");

    close(fn);
    printf("Debug: Exiting process_ibpage\n");
}

void process_ibpage_alt(page_t* page, int is_valid) {
    uint32_t page_id = mach_read_from_4(page + FIL_PAGE_OFFSET);
    uint64_t index_id = mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID);
    uint16_t page_type = mach_read_from_2(page + FIL_PAGE_TYPE);
    
    char file_name[1024] = "";
    int flags = O_WRONLY | O_CREAT | O_APPEND;

    if (!is_valid) {
        snprintf(file_name, sizeof(file_name), "%s-invalid/%016u.page", dst_dir, page_id);
    } else if (page_type == FIL_PAGE_INDEX) {
        snprintf(file_name, sizeof(file_name), "%s/FIL_PAGE_INDEX/%016lu.page", dst_dir, index_id);
    } else {
        snprintf(file_name, sizeof(file_name), "%s/FIL_PAGE_TYPE_BLOB/%016u.page", dst_dir, page_id);
    }

    int fn = open(file_name, flags, 0644);
    if (fn == -1) {
        fprintf(stderr, "Can't open file to save page: %s\n", strerror(errno));
        return;
    }
    
    if (write(fn, page, UNIV_PAGE_SIZE) == -1) {
        fprintf(stderr, "Can't write a page on disk: %s\n", strerror(errno));
        close(fn);
        return;
    }
    
    close(fn);
}

void process_ibfile(int fn, off_t start_offset, ssize_t length)
{
    printf("Debug: Entering process_ibfile. start_offset: %ld, length: %ld\n", start_offset, length);
    
    page_t *cache = NULL;
    page_t *decompressed_page = NULL;
    int result = 0;

    cache = malloc(cache_size);
    if (!cache) {
        fprintf(stderr, "Failed to allocate memory for cache in process_ibfile\n");
        return;
    }
    printf("Debug: Cache allocated in process_ibfile\n");

    decompressed_page = malloc(UNIV_PAGE_SIZE_MAX);
    if (!decompressed_page) {
        fprintf(stderr, "Failed to allocate memory for decompressed_page in process_ibfile\n");
        free(cache);
        return;
    }
    printf("Debug: Decompressed page allocated in process_ibfile\n");

    ssize_t disk_read;
    off_t curr_disk_offset = 0;
    off_t global_offset = 0;

    int valid_blob_pages = 0;
    int valid_innodb_pages = 0;
    int mysql_compressed_pages = 0;
    int mariadb_compressed_pages = 0;
    int invalid_pages = 0;

    ssize_t curr_cache_offset = 0;
    curr_disk_offset = lseek(fn, start_offset + initial_offset, SEEK_SET);
    // curr_disk_offset = lseek(fn, start_offset, SEEK_SET);
    printf("Debug: Seeked to offset %ld\n", curr_disk_offset);

    #define MAX_ITERATIONS 1000000
    int iteration_count = 0;
    int consecutive_invalid_pages = 0;
    const int MAX_CONSECUTIVE_INVALID_PAGES = 1000;

    while ((curr_disk_offset - start_offset) < length && iteration_count < MAX_ITERATIONS)
    {
        printf("Debug: Reading from offset %ld\n", curr_disk_offset);
        disk_read = read(fn, cache + curr_cache_offset, cache_size - curr_cache_offset);
        if (disk_read == -1)
        {
            fprintf(stderr, "Worker(%d): Failed to read from disk: %s\n", worker, strerror(errno));
            result = -1;
            goto cleanup;
        }
        if (disk_read == 0) break;

        printf("Debug: Read %zd bytes from disk\n", disk_read);

        ssize_t bytes_in_cache = curr_cache_offset + disk_read;
        curr_cache_offset = 0;
        
        printf("Debug: Processing %zd bytes in cache\n", bytes_in_cache);
        
        while (bytes_in_cache - curr_cache_offset >= UNIV_PAGE_SIZE && iteration_count < MAX_ITERATIONS)
        {
            if (curr_cache_offset + UNIV_PAGE_SIZE > bytes_in_cache) {
                printf("Debug: Reached end of cache buffer\n");
                break;
            }

            printf("Debug: Processing page at cache offset %zd\n", curr_cache_offset);

            off_t cache_step = 1;
            uint32_t page_id = get_page_id(cache + curr_cache_offset);
            
            printf("Debug: Page ID: %u\n", page_id);

            int is_valid_blob_page = valid_blob_page(cache + curr_cache_offset);
            int is_valid_innodb_page = !is_valid_blob_page && valid_innodb_page(cache + curr_cache_offset, bytes_in_cache - curr_cache_offset, &cache_step);
            int is_valid_mysql_compressed_page = !is_valid_blob_page && !is_valid_innodb_page && valid_mysql_compressed_page(cache + curr_cache_offset);
            int is_valid_mariadb_compressed_page = !is_valid_blob_page && !is_valid_innodb_page && !is_valid_mysql_compressed_page && valid_mariadb_compressed_page(cache + curr_cache_offset, bytes_in_cache - curr_cache_offset, decompressed_page);

            if (is_valid_blob_page || is_valid_innodb_page)
            {
                printf("Debug: Processing valid Blob/InnoDB page\n");
                process_ibpage(cache + curr_cache_offset);
                cache_step = UNIV_PAGE_SIZE;
                is_valid_blob_page ? valid_blob_pages++ : valid_innodb_pages++;
                consecutive_invalid_pages = 0;
            }
            else if (is_valid_mariadb_compressed_page)
            {
                printf("Debug: Processing valid Compressed MariaDB page\n");
                process_ibpage(decompressed_page);
                cache_step = UNIV_PAGE_SIZE;
                mariadb_compressed_pages++;
                consecutive_invalid_pages = 0;
            }
            else if (is_valid_mysql_compressed_page)
            {
                printf("Debug: Found valid MySQL compressed page\n");
                cache_step = UNIV_PAGE_SIZE;
                mysql_compressed_pages++;
                consecutive_invalid_pages = 0;
            }
            else
            {
                printf("Debug: Invalid page found\n");
                invalid_pages++;
                cache_step = 1;  // Move only one byte for invalid pages
                consecutive_invalid_pages++;

                if (consecutive_invalid_pages >= MAX_CONSECUTIVE_INVALID_PAGES) {
                    printf("Debug: Too many consecutive invalid pages. Stopping processing.\n");
                    result = -2;
                    goto cleanup;
                }
            }

            printf("Debug: Moving cache pointer %ld bytes\n", cache_step);
            curr_cache_offset += cache_step;
            global_offset += cache_step;
            iteration_count++;

            if (iteration_count % 1000 == 0) {
                printf("Debug: Processed %d pages\n", iteration_count);
            }
        }

        if (curr_cache_offset < bytes_in_cache)
        {
            memmove(cache, cache + curr_cache_offset, bytes_in_cache - curr_cache_offset);
            curr_cache_offset = bytes_in_cache - curr_cache_offset;
        }
        else
        {
            curr_cache_offset = 0;
        }

        show_progress(curr_disk_offset - start_offset, length);
        curr_disk_offset = lseek(fn, 0, SEEK_CUR);
    }

    if (iteration_count >= MAX_ITERATIONS) {
        printf("Debug: Reached maximum iteration limit\n");
        result = -3;
    }

cleanup:
    fprintf(stderr, "Stream contained %i blob, %i innodb, %i mysql compressed, %i mariadb compressed and %i ignored page read attempts\n", 
            valid_blob_pages, valid_innodb_pages, mysql_compressed_pages, mariadb_compressed_pages, invalid_pages);

    free(cache);
    free(decompressed_page);
    printf("Debug: Exiting process_ibfile with result %d\n", result);
}

int open_ibfile(char *fname)
{
    struct stat st;
    int fn;
    char buf[255];

    fprintf(stderr, "Opening file: %s\n", fname);
    fprintf(stderr, "File information:\n\n");

    if (stat(fname, &st) != 0)
    {
        printf("Errno = %d, Error = %s\n", errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
#ifdef __APPLE__
    fprintf(stderr, "ID of device containing file: %12d\n", st.st_dev);
    fprintf(stderr, "inode number:                 %12llu\n", st.st_ino);
#else
    fprintf(stderr, "ID of device containing file: %12ju\n", st.st_dev);
    fprintf(stderr, "inode number:                 %12ju\n", st.st_ino);
#endif
    fprintf(stderr, "protection:                   %12o ", st.st_mode);
    switch (st.st_mode & S_IFMT)
    {
    case S_IFBLK:
        fprintf(stderr, "(block device)\n");
        break;
    case S_IFCHR:
        fprintf(stderr, "(character device)\n");
        break;
    case S_IFDIR:
        fprintf(stderr, "(directory)\n");
        break;
    case S_IFIFO:
        fprintf(stderr, "(FIFO/pipe)\n");
        break;
    case S_IFLNK:
        fprintf(stderr, "(symlink)\n");
        break;
    case S_IFREG:
        fprintf(stderr, "(regular file)\n");
        break;
    case S_IFSOCK:
        fprintf(stderr, "(socket)\n");
        break;
    default:
        fprintf(stderr, "(unknown file type?)\n");
        break;
    }
#ifdef __APPLE__
    fprintf(stderr, "number of hard links:         %12u\n", st.st_nlink);
#else
    fprintf(stderr, "number of hard links:         %12zu\n", st.st_nlink);
#endif
    fprintf(stderr, "user ID of owner:             %12u\n", st.st_uid);
    fprintf(stderr, "group ID of owner:            %12u\n", st.st_gid);
#ifdef __APPLE__
    fprintf(stderr, "device ID (if special file):  %12d\n", st.st_rdev);
    fprintf(stderr, "blocksize for filesystem I/O: %12d\n", st.st_blksize);
    fprintf(stderr, "number of blocks allocated:   %12lld\n", st.st_blocks);
#else
    fprintf(stderr, "device ID (if special file):  %12ju\n", st.st_rdev);
    fprintf(stderr, "blocksize for filesystem I/O: %12lu\n", st.st_blksize);
    fprintf(stderr, "number of blocks allocated:   %12ju\n", st.st_blocks);
#endif
    fprintf(stderr, "time of last access:          %12lu %s", st.st_atime, ctime(&(st.st_atime)));
    fprintf(stderr, "time of last modification:    %12lu %s", st.st_mtime, ctime(&(st.st_mtime)));
    fprintf(stderr, "time of last status change:   %12lu %s", st.st_ctime, ctime(&(st.st_ctime)));
    h_size(st.st_size, buf);
    fprintf(stderr, "total size, in bytes:         %12jd (%s)\n\n", (intmax_t)st.st_size, buf);

    fn = open(fname, O_RDONLY);
#ifdef posix_fadvise
    posix_fadvise(fn, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
    if (fn == -1)
    {
        perror("Can't open file");
        exit(EXIT_FAILURE);
    }
    if (ib_size == 0)
    { // determine tablespace size if not given
        if (st.st_size != 0)
        {
            ib_size = st.st_size;
        }
    }
    if (ib_size == 0)
    {
        fprintf(stderr, "Can't determine size of %s. Specify it manually with -t option\n", fname);
        exit(EXIT_FAILURE);
    }
#ifdef __APPLE__
    fprintf(stderr, "Size to process:              %12lld (%s)\n", ib_size, h_size(ib_size, buf));
#else
    fprintf(stderr, "Size to process:              %12lu (%s)\n", ib_size, h_size(ib_size, buf));
#endif
    // max_page_id = ib_size/UNIV_PAGE_SIZE;
    max_page_id = 9000000000;
    printf("Debug: File size: %lu bytes\n", (unsigned long)st.st_size);

    return fn;
}

void usage(char *cmd)
{
    fprintf(stderr,
            "Usage: %s -f <innodb_datafile> [-T N:M] [-s size] [-t size] [-V|-g] [-i]\n"
            "  Where:\n"
            "    -h         - Print this help\n"
            "    -V or -g   - Print debug information\n"
            "    -i         - Print info logs\n"
            "    -s size    - Amount of memory used for disk cache (allowed examples 1G 10M). Default 100M\n"
            "    -T         - retrieves only pages with index id = NM (N - high word, M - low word of id)\n"
            "    -t size    - Size of InnoDB tablespace to scan. Use it only if the parser can't determine it by himself.\n",
            cmd);
}



uint64_t get_factor(char suffix)
{
    uint64_t factor = 1;
    switch (suffix)
    {
    case 'k':
    case 'K':
        factor = 1024;
        break;
    case 'm':
    case 'M':
        factor = 1024 * 1024;
        break;
    case 'g':
    case 'G':
        factor = 1024 * 1024 * 1024;
        break;
    default:
        fprintf(stderr, "Unrecognized size suffix %c\n", suffix);
        factor = 1;
    }
    return factor;
}
/*******************************************************************/
int main(int argc, char **argv)
{
    signal(SIGSEGV, segfault_handler);
    signal(SIGABRT, segfault_handler);
    signal(SIGILL, segfault_handler);
    signal(SIGFPE, segfault_handler);

    int ch;
    float m;
    char suffix;
    char buf[255];
    char ibfile[1024] = "";

    while ((ch = getopt(argc, argv, "igVhf:T:s:t:d:o:p:")) != -1)
    {
        switch (ch)
        {
        case 'p':
            page_size = atoi(optarg);
            break;
        case 'o':
            initial_offset = atoll(optarg);
            break;
        case 'f':
            strncpy(ibfile, optarg, sizeof(ibfile));
            break;
        case 'd':
            strncpy(dst_dir, optarg, sizeof(dst_dir));
            break;
        case 'V':
        case 'g':
            debug = 1;
            break;
        case 'i':
            info_log = 1;
            break;
        case 's':
            sscanf(optarg, "%f%c", &m, &suffix);
            cache_size = m * get_factor(suffix);
            if (cache_size < UNIV_PAGE_SIZE)
            {
                fprintf(stderr, "Disk cache size %lu can't be less than %u\n", cache_size, UNIV_PAGE_SIZE);
                usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            fprintf(stderr, "Disk cache:                   %12lu (%s)\n\n", cache_size, h_size(cache_size, buf));
            break;
        case 't':
            sscanf(optarg, "%f%c", &m, &suffix);
            ib_size = m * get_factor(suffix);
            break;
        case 'T':
            filter_id = strtoull(optarg, NULL, 10);
            break;
        default:
        case '?':
        case 'h':
            usage(argv[0]);
            exit(EXIT_SUCCESS);
        }
    }
    if (strlen(ibfile) == 0)
    {
        fprintf(stderr, "You must specify file with -f option\n");
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    if (strlen(dst_dir) == 0)
    {
        snprintf(dst_dir, sizeof(dst_dir), "pages-%s", basename(ibfile));
    }
    // Create pages directory
    if (mkdir(dst_dir, 0755) == -1 && errno != EEXIST) 
    {
        fprintf(stderr, "Could not create directory %s\n", dst_dir);
        perror("mkdir()");
        exit(EXIT_FAILURE);
    }

    // Create directory for invalid pages
    create_invalid_pages_dir(dst_dir);

    char d[1024];
    // Create directory for index pages
    sprintf(d, "%s/FIL_PAGE_INDEX", dst_dir);
    if (mkdir(d, 0755) == -1 && errno != EEXIST)
    {
        fprintf(stderr, "Could not create directory %s\n", d);
        perror("mkdir()");
        exit(EXIT_FAILURE);
    }
    sprintf(d, "%s/FIL_PAGE_TYPE_BLOB", dst_dir);
    if (mkdir(d, 0755) == -1 && errno != EEXIST)
    {
        fprintf(stderr, "Could not create directory %s\n", d);
        perror("mkdir()");
        exit(EXIT_FAILURE);
    }
    // Initialize the mutex
    if (pthread_mutex_init(&global_mutex, NULL) != 0) {
        fprintf(stderr, "Mutex initialization failed\n");
        return 1;
    }

    printf("Debug: Before opening file\n");
    fn = open_ibfile(ibfile);
    if (fn == 0) {
        fprintf(stderr, "Cannot open file %s\n", ibfile);
        usage(argv[0]);
        pthread_mutex_destroy(&global_mutex);
        exit(EXIT_FAILURE);
    }
    printf("Debug: File opened successfully\n");

    pthread_t threads[THREAD_POOL_SIZE];
    int thread_ids[THREAD_POOL_SIZE];

    time_t start_time, end_time;
    time(&start_time);

    process_ibfile(fn, 0, ib_size);

    time(&end_time);
    printf("\nAll workers finished in %ld seconds\n", end_time - start_time);

    close(fn);
    pthread_mutex_destroy(&global_mutex);
    exit(EXIT_SUCCESS);

}
