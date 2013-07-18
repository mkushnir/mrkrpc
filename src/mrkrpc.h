#ifndef MRKRPC_H_DEFINED
#define MRKRPC_H_DEFINED

#include <stdint.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <mrkcommon/array.h>
#include <mrkcommon/trie.h>
#include <mrkdata.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _mrkrpc_node {
    uint64_t nid;
    char *hostname;
    int port;
    struct sockaddr *addr;
    socklen_t addrlen;
} mrkrpc_node_t;

typedef struct _mrkrpc_queue_entry {
    /* weak ref */
    mrkrpc_node_t *peer;

    uint8_t op;
    uint64_t nid;
    uint64_t sid;

    /* weak ref */
    mrkdata_datum_t *senddat;

    unsigned char *buf;
    size_t sz;

    /* weak ref */
    mrkdata_datum_t *recvdat;

    mrkthr_signal_t signal;

    struct _mrkrpc_queue_entry *next;
} mrkrpc_queue_entry_t;

typedef struct _mrkrpc_queue {
    mrkrpc_queue_entry_t *head;
    mrkrpc_queue_entry_t *tail;
    mrkthr_signal_t signal;
} mrkrpc_queue_t;

struct _mrkrpc_ctx;

typedef void (*mrkrpc_recv_handler_t)(struct _mrkrpc_ctx *,
                                      mrkrpc_queue_entry_t *);

typedef struct _mrkrpc_op_entry {
    mrkdata_spec_t *reqspec;
    mrkrpc_recv_handler_t reqhandler;
    mrkdata_spec_t *respspec;
    mrkrpc_recv_handler_t resphandler;
} mrkrpc_op_entry_t;

typedef struct _mrkrpc_ctx {
    mrkrpc_node_t me;
    int fd;
    array_t ops;

    mrkthr_ctx_t *sendthr;
    mrkrpc_queue_t sendq;

    mrkthr_ctx_t *recvthr;
    mrkrpc_queue_t recvq;

    trie_t pending;

} mrkrpc_ctx_t;

/* module */
void mrkrpc_init(void);
void mrkrpc_fini(void);

/* ctx */
int mrkrpc_ctx_init(mrkrpc_ctx_t *);
int mrkrpc_ctx_fini(mrkrpc_ctx_t *);
int mrkrpc_ctx_set_me(mrkrpc_ctx_t *, uint64_t, const char *, int);
int mrkrpc_ctx_register_op(mrkrpc_ctx_t *, uint8_t, mrkrpc_op_entry_t *);
mrkrpc_op_entry_t *mrkrpc_ctx_get_op(mrkrpc_ctx_t *, uint8_t);

/* node */
int mrkrpc_node_init(mrkrpc_node_t *);
int mrkrpc_node_fini(mrkrpc_node_t *);
int mrkrpc_node_dump(mrkrpc_node_t *);
mrkrpc_node_t *mrkrpc_make_node(uint64_t, const char *, int);
mrkrpc_node_t *mrkrpc_make_node_from_addr(uint64_t, const struct sockaddr *, socklen_t);
void mrkrpc_node_destroy(mrkrpc_node_t **);


/* operations */
int mrkrpc_run(mrkrpc_ctx_t *);
int mrkrpc_call(mrkrpc_ctx_t *,
                mrkrpc_node_t *,
                uint8_t,
                mrkdata_datum_t *,
                mrkdata_datum_t **);
int mrkrpc_serve(mrkrpc_ctx_t *);

/* queue */
void mrkrpc_queue_entry_enqueue(mrkrpc_queue_t *, mrkrpc_queue_entry_t *);
#ifdef __cplusplus
}
#endif

#endif /* MRKRPC_H_DEFINED */
