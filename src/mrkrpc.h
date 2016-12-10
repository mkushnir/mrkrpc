#ifndef MRKRPC_H_DEFINED
#define MRKRPC_H_DEFINED

#include <stdint.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <mrkcommon/array.h>
#include <mrkcommon/btrie.h>
#include <mrkcommon/dtqueue.h>
#include <mrkthr.h>
#include <mrkdata.h>

#ifdef __cplusplus
extern "C" {
#endif

const char *mrkrpc_diag_str(int);

typedef uint64_t mrkrpc_nid_t;
typedef uint64_t mrkrpc_sid_t;

typedef struct _mrkrpc_node {
    mrkrpc_nid_t nid;
    char *hostname;
    int port;
    struct sockaddr *addr;
    socklen_t addrlen;
} mrkrpc_node_t;

typedef struct _mrkrpc_queue_entry {
    /* weak ref */
    mrkrpc_node_t *peer;

    mrkrpc_nid_t nid;
    mrkrpc_sid_t sid;

    /* weak ref */
    uint8_t sendop;
    mrkdata_datum_t *senddat;

    unsigned char *buf;
    size_t sz;

    /* weak ref */
    uint8_t recvop;
    mrkdata_datum_t *recvdat;

    mrkthr_signal_t signal;
    mrkthr_rwlock_t rwlock;
    /* mrkrpc_call() return value */
    int res;

    DTQUEUE_ENTRY(_mrkrpc_queue_entry, link);
} mrkrpc_queue_entry_t;

struct _mrkrpc_ctx;

typedef int (*mrkrpc_recv_handler_t)(struct _mrkrpc_ctx *,
                                     mrkrpc_queue_entry_t *);

typedef struct _mrkrpc_op_entry {
    mrkdata_spec_t *spec;
    mrkrpc_recv_handler_t reqhandler;
} mrkrpc_op_entry_t;

typedef DTQUEUE(_mrkrpc_queue_entry, mrkrpc_queue_t);

typedef struct _mrkrpc_ctx {
    int family;
    int socktype;
    int protocol;

    mrkrpc_node_t me;
    int fd;
    mnarray_t ops;

    mrkthr_ctx_t *sendthr;
    mrkrpc_queue_t sendq;
    mrkthr_signal_t sendq_signal;

    mrkthr_ctx_t *recvthr;

    mnbtrie_t pending;
    size_t nsent;
    size_t nrecvd;

    uint64_t call_timeout;

} mrkrpc_ctx_t;

/* module */
void mrkrpc_init(void);
void mrkrpc_shutdown(void);
void mrkrpc_fini(void);

/* ctx */
int mrkrpc_ctx_init(mrkrpc_ctx_t *);
void mrkrpc_ctx_close(mrkrpc_ctx_t *);
int mrkrpc_ctx_fini(mrkrpc_ctx_t *);
int mrkrpc_ctx_set_local_node(mrkrpc_ctx_t *,
                              mrkrpc_nid_t,
                              const char *,
                              int);
void mrkrpc_ctx_set_call_timeout(mrkrpc_ctx_t *, uint64_t);
int mrkrpc_ctx_register_msg(mrkrpc_ctx_t *,
                            uint8_t,
                            mrkdata_spec_t *,
                            mrkrpc_recv_handler_t);
size_t mrkrpc_ctx_get_pending_volume(mrkrpc_ctx_t *);
size_t mrkrpc_ctx_get_pending_length(mrkrpc_ctx_t *);
size_t mrkrpc_ctx_get_sendq_length(mrkrpc_ctx_t *);
size_t mrkrpc_ctx_compact_pending(mrkrpc_ctx_t *, size_t);

/* node */
int mrkrpc_node_init(mrkrpc_node_t *);
int mrkrpc_node_init_from_params(mrkrpc_ctx_t *,
                                 mrkrpc_node_t *,
                                 mrkrpc_nid_t,
                                 const char *,
                                 int);
int mrkrpc_node_init_from_addr(mrkrpc_node_t *,
                               mrkrpc_nid_t,
                               const struct sockaddr *,
                               socklen_t);
int mrkrpc_node_fini(mrkrpc_node_t *);
int mrkrpc_node_dump(mrkrpc_node_t *);
int mrkrpc_node_str(mrkrpc_node_t *, char *, size_t);
mrkrpc_node_t *mrkrpc_make_node_from_params(mrkrpc_ctx_t *,
                                            mrkrpc_nid_t,
                                            const char *,
                                            int);
mrkrpc_node_t *mrkrpc_make_node_from_addr(mrkrpc_nid_t,
                                          const struct sockaddr *,
                                          socklen_t);
void mrkrpc_node_destroy(mrkrpc_node_t **);
void mrkrpc_node_copy(mrkrpc_node_t *, mrkrpc_node_t *);
int mrkrpc_nodes_equal(mrkrpc_node_t *, mrkrpc_node_t *);


/* operations */
int mrkrpc_run(mrkrpc_ctx_t *);

#define MRKRPC_CALL_TIMEOUT (-1)
int mrkrpc_call(mrkrpc_ctx_t *,
                mrkrpc_node_t *,
                uint8_t,
                mrkdata_datum_t *,
                mrkdata_datum_t **);
int mrkrpc_serve(mrkrpc_ctx_t *);
const char *mrkrpc_diag_str(int);

#ifdef __cplusplus
}
#endif

#endif /* MRKRPC_H_DEFINED */
