#ifdef __cplusplus
extern "C" {
#endif

#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

typedef struct logdevice_client_t logdevice_client_t;
typedef void logdevice_read_callback_t(char **data, ssize_t size);

typedef uint64_t lsn_t;

void set_dbg_level_error(void);
void init_logdevice(void);

logdevice_client_t *new_logdevice_client(char *config_path);
void free_logdevice_client(logdevice_client_t *client);

lsn_t append_sync(logdevice_client_t *client, uint64_t logid,
                  const char *payload, int64_t *ms);

#ifdef __cplusplus
} /* end extern "C" */
#endif
