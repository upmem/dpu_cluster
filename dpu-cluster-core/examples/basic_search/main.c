#include <stdint.h>
#include <alloc.h>
#include <mram.h>
#include <defs.h>
#include <string.h>
#include <mutex.h>

#define QUERY_OFFSET  (0) 
#define QUERY_SIZE    16
#define INPUT_OFFSET  (QUERY_OFFSET + QUERY_SIZE)
#define INPUT_SIZE    (1 << 24)
#define OUTPUT_OFFSET (INPUT_OFFSET + INPUT_SIZE)
#define OUTPUT_SIZE   (8 + (1 << 24))

#define INPUT_ENTRY_SIZE 32 
#define BLOCK_SIZE       (INPUT_SIZE / NR_THREADS)

#define NR_THREADS 16

#define CONCAT(x, y) _CONCAT(x, y)
#define _CONCAT(x, y) x ## y

#define MRAM_READ(wram, mram, size) CONCAT(mram_read, size) (mram, wram)

#define READ_QUERY(wram)  MRAM_READ(wram, QUERY_OFFSET, QUERY_SIZE)
#define READ_INPUT(input, offset) MRAM_READ(input, offset, INPUT_ENTRY_SIZE)
#define WRITE_OUTPUT(output) \
    do { \
        uint32_t _index = current_output_index; \
        CONCAT(mram_write, INPUT_ENTRY_SIZE) (output, OUTPUT_OFFSET + 8 + (_index * INPUT_ENTRY_SIZE)); \
        current_output_index = _index + 1; \
    } while(0)

uint32_t __attribute__ ((aligned (8))) current_output_index = 0;
uint32_t count = 0;

int main() {
    uint8_t* query = mem_alloc_dma(QUERY_SIZE); 
    uint8_t* input = mem_alloc_dma(INPUT_ENTRY_SIZE);
    mutex_t mutex = mutex_get(0);

    READ_QUERY(query);
    uint32_t query_length = strlen((const char*) query);
    mram_addr_t starting_input_offset = INPUT_OFFSET + BLOCK_SIZE * me();
    
    for (uint32_t offset = 0; offset < BLOCK_SIZE; offset += INPUT_ENTRY_SIZE) {
        READ_INPUT(input, starting_input_offset + offset);
        
        if (strncmp((const char*) query, (const char*) input, query_length) == 0) {
            mutex_lock(mutex);

            WRITE_OUTPUT(input);

            mutex_unlock(mutex);
        }
    }
    
    mutex_lock(mutex);
    count++;
    if (count == NR_THREADS) {
        mram_write8(&current_output_index, OUTPUT_OFFSET);
        current_output_index = 0;
        count = 0;
    }
    mutex_unlock(mutex);

    return 0;
}
