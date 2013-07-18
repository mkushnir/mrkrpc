#include <assert.h>
#include <stdlib.h>
#include <stdarg.h>

#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <mrkcommon/array.h>
//#define TRRET_DEBUG_VERBOSE
#include <mrkcommon/dumpm.h>
#include <mrkcommon/trie.h>
#include <mrkcommon/util.h>
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkrpc);
#include <mrkdata.h>
#include <mrkthr.h>

#include "mrkrpc.h"
#include "diag.h"

#define MRKRPC_MFLAG_INITIALIZED (0x01)
static unsigned mflags = 0;

#define MRKRPC_VERSION 0x81
#define MRKRPC_MAX_OPS 256 /* uint8_t */
#define QUEUE_ENTRY_DEFAULT_BUFSZ 1024
#define NODE_DEFAULT_ADDRLEN 256

static mrkdata_spec_t *header_spec;
#define MRKRPC_HEADER_VER 0
#define MRKRPC_HEADER_OP 1
#define MRKRPC_HEADER_NID 2
#define MRKRPC_HEADER_SID 3
static size_t calculated_default_message_sz;

static uint64_t sidbase;
static uint64_t sid = 0;

static uint64_t
new_sid(void)
{
    return ++sid ^ sidbase;
}

static int
null_init(void **p)
{
    *p = NULL;
    return 0;
}

static int
null_fini(void **p)
{
    *p = NULL;
    return 0;
}

/* node */

int
mrkrpc_node_init(mrkrpc_node_t *node)
{
    node->nid = 0;
    node->hostname = NULL;
    node->port = -1;
    node->addr = NULL;
    return 0;
}

int
mrkrpc_node_fini(mrkrpc_node_t *node)
{
    node->nid = 0;
    if (node->hostname != NULL) {
        free(node->hostname);
        node->hostname = NULL;
    }
    node->port = -1;
    if (node->addr != NULL) {
        free(node->addr);
        node->addr = NULL;
    }
    return 0;
}

int
mrkrpc_node_dump(mrkrpc_node_t *node)
{
    CTRACE("<node nid=%016lx @ %s:%d>", node->nid, node->hostname, node->port);
    return 0;
}

mrkrpc_node_t *
mrkrpc_make_node_from_addr(uint64_t nid,
                           const struct sockaddr *addr,
                           socklen_t addrlen)
{
    mrkrpc_node_t *res;

    if ((res = malloc(sizeof(mrkrpc_node_t))) == NULL) {
        FAIL("malloc");
    }
    res->nid = nid;
    res->hostname = NULL;
    res->port = 0;
    res->addrlen = addrlen;
    if ((res->addr = malloc(res->addrlen)) == NULL) {
        FAIL("malloc");
    }
    memcpy(res->addr, addr, res->addrlen);
    return res;
}

mrkrpc_node_t *
mrkrpc_make_node(uint64_t nid, const char *hostname, int port)
{
    mrkrpc_node_t *res;
    struct addrinfo hints, *ai = NULL, *ain;
    char portstr[32];

    if ((res = malloc(sizeof(mrkrpc_node_t))) == NULL) {
        FAIL("malloc");
    }
    res->nid = nid;
    if (hostname != NULL) {
        if ((res->hostname = strdup(hostname)) == NULL) {
            FAIL("strdup");
        }
        res->port = port;

        snprintf(portstr, sizeof(portstr), "%d", port);

        memset(&hints, '\0', sizeof(struct addrinfo));
        hints.ai_family = PF_INET;
        hints.ai_socktype = SOCK_DGRAM;
        hints.ai_protocol = IPPROTO_UDP;


        if (getaddrinfo(hostname, portstr, &hints, &ai) != 0) {
            TRRETNULL(MRKRPC_MAKE_NODE + 1);
        }

        for (ain = ai; ain != NULL; ain = ain->ai_next) {
            /* stop at the fisrt getaddrinfo() entry */
            break;
        }

        if (ain == NULL) {
            TRRETNULL(MRKRPC_MAKE_NODE + 2);
        }

        if ((res->addr = malloc(ain->ai_addrlen)) == NULL) {
            FAIL("malloc");
        }
        memcpy(res->addr, ain->ai_addr, ain->ai_addrlen);
        res->addrlen = ain->ai_addrlen;
        freeaddrinfo(ai);
    } else {
        res->hostname = NULL;
        res->port = port;
        res->addrlen = NODE_DEFAULT_ADDRLEN;
        if ((res->addr = malloc(res->addrlen)) == NULL) {
            FAIL("malloc");
        }
        memset(res->addr, '\0', res->addrlen);
    }

    return res;
}

void
mrkrpc_node_destroy(mrkrpc_node_t **node)
{
    if (*node != NULL) {
        mrkrpc_node_fini(*node);
        free(*node);
        *node = NULL;
    }
}

/* queue */

static void
queue_entry_init(mrkrpc_queue_entry_t *e,
                 mrkrpc_node_t *peer,
                 uint8_t op,
                 uint64_t nid,
                 uint64_t sid)
{
    e->peer = peer;
    e->op = op;
    e->nid = nid;
    e->sid = sid;
    e->senddat = NULL;
    e->buf = NULL;
    e->sz = 0;
    e->recvdat = NULL;
}

static void
queue_entry_fini(mrkrpc_queue_entry_t *e)
{
    e->peer = NULL;
    e->op = 0;
    e->nid = 0;
    e->sid = 0;
    e->senddat = NULL;
    if (e->buf != NULL) {
        free(e->buf);
        e->buf = NULL;
    }
    e->sz = 0;
    e->recvdat = NULL;
    e->next = NULL;
}

static void
queue_entry_destroy(mrkrpc_queue_entry_t **e)
{
    if (*e != NULL) {
        queue_entry_fini(*e);
        free(*e);
        *e = NULL;
    }
}

static mrkrpc_queue_entry_t *
make_queue_entry(mrkrpc_node_t *me,
                 mrkrpc_node_t *peer,
                 uint8_t op,
                 uint64_t sid)
{
    mrkrpc_queue_entry_t *qe;

    if ((qe = malloc(sizeof(mrkrpc_queue_entry_t))) == NULL) {
        FAIL("malloc");
    }

    queue_entry_init(qe, peer, op, me->nid, sid);

    return qe;
}

static mrkdata_datum_t *
make_header(uint8_t op, uint64_t nid, uint64_t sid)
{
    mrkdata_datum_t *header;

    if ((header = mrkdata_datum_from_spec(header_spec, NULL, 0)) == NULL) {
        return NULL;
    }
    mrkdata_datum_add_field(header, mrkdata_datum_make_u8(MRKRPC_VERSION));
    mrkdata_datum_add_field(header, mrkdata_datum_make_u8(op));
    mrkdata_datum_add_field(header, mrkdata_datum_make_u64(nid));
    mrkdata_datum_add_field(header, mrkdata_datum_make_u64(sid));
    return header;
}


static int
pack_queue_entry(mrkrpc_queue_entry_t *qe)
{
    unsigned char *pbuf;
    size_t nwritten;
    mrkdata_datum_t *header;

    if ((header = make_header(qe->op, qe->nid, qe->sid)) == NULL) {
        TRRET(PACK_QUEUE_ENTRY + 1);
    }

    if (qe->senddat != NULL) {
        if ((qe->buf = malloc(header->packsz + qe->senddat->packsz)) == NULL) {
            FAIL("malloc");
        }

        pbuf = qe->buf;

        if ((nwritten = mrkdata_pack_datum(header,
                                           pbuf,
                                           header->packsz)) <= 0) {
            TRRET(PACK_QUEUE_ENTRY + 2);
        }

        qe->sz += nwritten;
        pbuf += nwritten;

        if ((nwritten = mrkdata_pack_datum(qe->senddat,
                                           pbuf,
                                           qe->senddat->packsz)) <= 0) {
            TRRET(PACK_QUEUE_ENTRY + 3);
        }
        qe->sz += nwritten;
        pbuf += nwritten;

    } else {
        if ((qe->buf = malloc(header->packsz)) == NULL) {
            FAIL("malloc");
        }

        pbuf = qe->buf;

        if ((nwritten = mrkdata_pack_datum(header,
                                           pbuf,
                                           header->packsz)) <= 0) {
            TRRET(PACK_QUEUE_ENTRY + 2);
        }

        qe->sz += nwritten;
        pbuf += nwritten;
    }
    //CTRACE("packed sz=%ld", qe->sz);

    mrkdata_datum_destroy(&header);

    TRRET(0);
}

static void
queue_entry_enqueue(mrkrpc_queue_t *queue, mrkrpc_queue_entry_t *qe)
{
    /* tail push */
    qe->next = NULL;
    if (queue->head == NULL) {
        queue->head = qe;
        queue->tail = qe;
    } else {
        queue->tail->next = qe;
        queue->tail = qe;
    }
    mrkthr_signal_send(&queue->signal);
}

static mrkrpc_queue_entry_t *
queue_entry_dequeue(mrkrpc_queue_t *queue)
{
    mrkrpc_queue_entry_t *qe;

    while (1) {
        if (queue->head == NULL) {
            if (mrkthr_signal_subscribe(&queue->signal) != 0) {
                break;
            }
            /* loop again, there must be something in the queue */
        } else {
            qe = queue->head;
            queue->head = qe->next;
            if (queue->head == NULL) {
                queue->tail = NULL;
            }
            return qe;
        }
    }
    TRRETNULL(QUEUE_ENTRY_DEQUEUE + 1);
}

static int
sendthr_loop(int argc, void *argv[])
{
    mrkrpc_ctx_t *ctx;

    assert(argc == 1);
    ctx = argv[0];


    while (1) {
        mrkrpc_queue_entry_t *qe;

        if ((qe = queue_entry_dequeue(&ctx->sendq)) == NULL) {
            break;
        }

        if (pack_queue_entry(qe) != 0) {
            CTRACE("pack_queue_entry failure");
            qe->peer = NULL;
            mrkthr_signal_send(&qe->signal);
            continue;
        }

        if (mrkthr_sendto_all(ctx->fd,
                              qe->buf,
                              qe->sz,
                              0,
                              qe->peer->addr,
                              qe->peer->addrlen) != 0) {
            perror("sendto");
        }

        if (!mrkthr_signal_has_owner(&qe->signal)) {
            /*
             * take care of this qe
             */
            /* this was the remote peer */
            mrkrpc_node_destroy(&qe->peer);
            /* this was the request received */
            mrkdata_datum_destroy(&qe->recvdat);
            /* this was the response sent */
            mrkdata_datum_destroy(&qe->senddat);
            queue_entry_destroy(&qe);
        }
    }

    TRRET(0);
}

static int
recvthr_loop(int argc, void *argv[])
{
    mrkrpc_ctx_t *ctx;
    /* buf is a reusable memory to store the raw datagram */
    char *buf;

    assert(argc == 1);
    ctx = argv[0];

    if ((buf = malloc(calculated_default_message_sz)) == NULL) {
        FAIL("malloc");
    }

    while (1) {
        unsigned char      *pbuf;
        ssize_t             nread;
        ssize_t             ndecoded;
        char                addr[NODE_DEFAULT_ADDRLEN];
        socklen_t           addrlen = NODE_DEFAULT_ADDRLEN;
        mrkdata_datum_t    *header = NULL;
        mrkdata_datum_t    *ver;
        mrkdata_datum_t    *op;
        mrkdata_datum_t    *nid;
        mrkdata_datum_t    *sid;
        trie_node_t        *trn;

        if ((nread = mrkthr_recvfrom_allb(ctx->fd,
                                          buf,
                                          calculated_default_message_sz,
                                          0,
                                          (struct sockaddr *)addr,
                                          &addrlen)) < 0) {
            CTRACE("EOF?");
            break;
        }

        //CTRACE("calculated_default_message_sz=%ld", calculated_default_message_sz);
        //CTRACE("nread=%ld recvfrom", nread);
        //D8(buf, nread);

        pbuf = (unsigned char *)buf;

        /* unpack header */
        if ((ndecoded = mrkdata_unpack_buf(header_spec,
                                           pbuf,
                                           nread,
                                           &header)) == 0) {

            CTRACE("mrkdata_unpack_buf failure, header:");
            D16(pbuf, nread);

            goto CONTINUE;
        }

        pbuf += ndecoded;
        nread -= ndecoded;

        //CTRACE("nread=%ld header", nread);

        /* check version */
        if ((ver = mrkdata_datum_get_field(header,
                                           MRKRPC_HEADER_VER)) == NULL) {
            CTRACE("corrupt header or spec");
            mrkdata_datum_dump(header);
            goto CONTINUE;

        } else {
            //CTRACE("received ver %x", ver->value.u8);
        }

        /* check op */
        if ((op = mrkdata_datum_get_field(header,
                                          MRKRPC_HEADER_OP)) == NULL) {
            CTRACE("corrupt header or spec");
            mrkdata_datum_dump(header);
            goto CONTINUE;

        } else {
            //CTRACE("received op %x", op->value.u8);
        }

        /* check nid */
        if ((nid = mrkdata_datum_get_field(header,
                                           MRKRPC_HEADER_NID)) == NULL) {
            CTRACE("corrupt header or spec");
            mrkdata_datum_dump(header);
            goto CONTINUE;

        }
        //CTRACE("received nid %016lx", nid->value.u64);

        /* check sid */
        if ((sid = mrkdata_datum_get_field(header,
                                           MRKRPC_HEADER_SID)) == NULL) {
            CTRACE("corrupt header or spec");
            mrkdata_datum_dump(header);
            goto CONTINUE;

        }
        //CTRACE("received sid %016lx", sid->value.u64);

        /* find the saved queue entry under header:sid */

        if ((trn = trie_find_exact(&ctx->pending, sid->value.u64)) != NULL) {
            mrkrpc_queue_entry_t *qe;
            mrkrpc_op_entry_t *ope;

            assert(trn->value != NULL);

            /*
             * This is a request queue entry that is waiting for response.
             * Update it with response datum and wake the requesting
             * thread.
             */

            qe = trn->value;

            /*
             * Check nid
             */
            if (nid->value.u64 != qe->peer->nid) {
                CTRACE("incoming nid %016lx  doesn't match the waiting "
                       "one %016lx while sids are matching: %016lx. "
                       "Possible sid collision or a sid forgery attempt?",
                       nid->value.u64, qe->peer->nid, sid->value.u64);
            }

            /*
             * XXX Check socket addr. It's probably OK that addr and
             * XXX qe->peer->addr don't match?
             */

            /* complete request operation */
            if ((ope = mrkrpc_ctx_get_op(ctx, op->value.u8)) == NULL) {
                /* XXX any indication to the sender? */
                CTRACE("op is not supported: %02x, original op was: %02x",
                       op->value.u8,
                       qe->op);

            } else {
                if ((ndecoded = mrkdata_unpack_buf(ope->respspec,
                                                   pbuf,
                                                   nread,
                                                   &qe->recvdat)) == 0) {
                    CTRACE("mrkdata_unpack_buf failure, resp:");
                    mrkdata_spec_dump(ope->respspec);
                    D16(pbuf, nread);
                    if (qe->recvdat != NULL) {
                        free(qe->recvdat);
                        qe->recvdat = NULL;
                        goto CONTINUE;
                    }
                }
                pbuf += ndecoded;
                nread -= ndecoded;

                //CTRACE("nread=%ld recvdat", nread);

                /* optional post-procesing */
                if (ope->resphandler != NULL) {
                    ope->resphandler(ctx, qe);
                }
            }

            /*
             * for this assertion, see mrkrpc_call()
             */
            assert(mrkthr_signal_has_owner(&qe->signal));
            mrkthr_dump(qe->signal.owner);
            mrkthr_signal_send(&qe->signal);

        } else {
            mrkdata_datum_t *req = NULL;
            mrkrpc_op_entry_t *ope;
            mrkrpc_queue_entry_t *qe;
            mrkrpc_node_t *from;

            /*
             * Here is an incoming request.
             */

            if ((ope = mrkrpc_ctx_get_op(ctx, op->value.u8)) == NULL) {
                CTRACE("op is not supported: %02x", op->value.u8);

            } else {
                if ((from = mrkrpc_make_node_from_addr(nid->value.u64,
                        (const struct sockaddr *)addr, addrlen)) == NULL) {

                    /* XXX handle it better */
                    CTRACE("mrkrpc_make_node failure");
                    mrkdata_datum_destroy(&req);
                }

                qe = make_queue_entry(&ctx->me,
                                      from,
                                      op->value.u8,
                                      sid->value.u64);
                /*
                 * Here we clear the signal owner to tell sendq_thr() to
                 * take care of this qe disposal.
                 */
                mrkthr_signal_fini(&qe->signal);

                if ((ndecoded = mrkdata_unpack_buf(ope->reqspec,
                                                   pbuf,
                                                   nread,
                                                   &qe->recvdat)) == 0) {
                    CTRACE("mrkdata_unpack_buf failure, req:");
                    D16(pbuf, nread);
                    if (req != NULL) {
                        free(req);
                        req = NULL;
                        goto CONTINUE;
                    }
                }
                pbuf += ndecoded;
                nread -= ndecoded;

                //CTRACE("nread=%ld recvdat", nread);

                /*
                 * In most cases reqhandler won't be NULL, since this is
                 * the place where qe->recvdat has to be analyzed and the
                 * response has to be constructed and placed in qe->senddat.
                 */
                if (ope->reqhandler != NULL) {
                    ope->reqhandler(ctx, qe);
                }
                queue_entry_enqueue(&ctx->sendq, qe);
            }
        }
CONTINUE:
        if (header != NULL) {
            mrkdata_datum_destroy(&header);
        }
    }

    free(buf);
    buf = NULL;

    TRRET(0);
}

int
mrkrpc_serve(mrkrpc_ctx_t *ctx)
{
    return mrkthr_join(ctx->recvthr);
}

int
mrkrpc_call(mrkrpc_ctx_t *ctx,
            mrkrpc_node_t *to,
            UNUSED uint8_t op,
            mrkdata_datum_t *arg,
            mrkdata_datum_t **rv)
{
    int res = 0;
    mrkrpc_queue_entry_t *qe = NULL;
    trie_node_t *trn;

    qe = make_queue_entry(&ctx->me, to, op, new_sid());
    qe->senddat = arg;
    /*
     * Set owner to tell sendthr_loop() that we are going to ake care this
     * qe.
     */
    mrkthr_signal_init(&qe->signal, mrkthr_me());

    /*
     * Remember this qe, recvthr_loop() will retrieve it at response time.
     */
    if ((trn = trie_add_node(&ctx->pending, qe->sid)) == NULL) {
        FAIL("trie_add_node");
    }
    trn->value = qe;

    queue_entry_enqueue(&ctx->sendq, qe);

    /*
     * fall asleep until we receive response
     */
    mrkthr_signal_subscribe(&qe->signal);
    mrkthr_signal_fini(&qe->signal);

    /* response */

    if (qe->peer == NULL) {
        res = MRKRPC_CALL + 1;
    }

    /*
     * qe->recvdat is a return value. We should return it back to the caller.
     */
    *rv = qe->recvdat;
    queue_entry_destroy(&qe);

    TRRET(res);
}

int
mrkrpc_run(mrkrpc_ctx_t *ctx)
{
    assert(ctx->sendthr != NULL);
    mrkthr_run(ctx->sendthr);
    assert(ctx->recvthr != NULL);
    mrkthr_run(ctx->recvthr);
    mrkthr_sleep(0);
    return 0;
}

/* ctx */

int
mrkrpc_ctx_init(mrkrpc_ctx_t *ctx)
{
    /* node */
    mrkrpc_node_init(&ctx->me);

    /* fd */
    ctx->fd = -1;

    /* ops */
    if (array_init(&ctx->ops, sizeof(mrkrpc_op_entry_t *), MRKRPC_MAX_OPS,
                   (array_initializer_t)null_init,
                   (array_finalizer_t)null_fini) != 0) {
        FAIL("array_init");
    }

    /* sendq */
    ctx->sendthr = mrkthr_new("sendthr", sendthr_loop, 1, ctx);
    mrkthr_signal_init(&ctx->sendq.signal, ctx->sendthr);
    ctx->sendq.head = NULL;
    ctx->sendq.tail = NULL;

    /* recvq */
    ctx->recvthr = mrkthr_new("recvthr", recvthr_loop, 1, ctx);
    mrkthr_signal_init(&ctx->recvq.signal, ctx->recvthr);
    ctx->recvq.head = NULL;
    ctx->recvq.tail = NULL;

    /* pending */
    trie_init(&ctx->pending);

    return 0;
}

int
mrkrpc_ctx_fini(mrkrpc_ctx_t *ctx)
{
    mrkrpc_queue_entry_t *e, *next;

    /* XXX clean up queue entries */
    trie_fini(&ctx->pending);

    /* sendq */
    mrkthr_set_interrupt(ctx->sendthr);
    mrkthr_join(ctx->sendthr);
    mrkthr_signal_fini(&ctx->sendq.signal);
    for (e = ctx->sendq.head; e != NULL;) {
        next = e->next;
        queue_entry_destroy(&e);
        e = next;
    }
    ctx->sendq.head = NULL;
    ctx->sendq.tail = NULL;

    /* recvq */
    mrkthr_set_interrupt(ctx->recvthr);
    //mrkthr_join(ctx->recvthr);
    mrkthr_signal_fini(&ctx->recvq.signal);
    for (e = ctx->recvq.head; e != NULL;) {
        next = e->next;
        mrkrpc_node_destroy(&e->peer);
        queue_entry_destroy(&e);
        e = next;
    }
    ctx->recvq.head = NULL;
    ctx->recvq.tail = NULL;

    /* ops */
    if (array_fini(&ctx->ops) != 0) {
        FAIL("array_fini");
    }

    /* fd */
    if (ctx->fd != -1) {
        close(ctx->fd);
        ctx->fd = -1;
    }

    /* node */
    mrkrpc_node_fini(&ctx->me);
    return 0;
}

int
mrkrpc_ctx_set_me(mrkrpc_ctx_t *ctx,
                    uint64_t nid,
                    const char *hostname,
                    int port)
{
    struct addrinfo hints, *ai = NULL, *ain;
    char portstr[32];

    ctx->me.nid = nid;

    if ((ctx->me.hostname = strdup(hostname)) == NULL) {
        FAIL("strdup");
    }
    ctx->me.port = port;
    snprintf(portstr, sizeof(portstr), "%d", port);

    memset(&hints, '\0', sizeof(struct addrinfo));
    hints.ai_family = PF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;

    if (ctx->fd != -1) {
        close(ctx->fd);
    }

    if (getaddrinfo(hostname, portstr, &hints, &ai) != 0) {
        TRRET(MRKRPC_CTX_SET_ME + 1);
    }

    for (ain = ai; ain != NULL; ain = ain->ai_next) {
        if ((ctx->fd = socket(ain->ai_family,
                              ain->ai_socktype,
                              ain->ai_protocol)) < 0) {
            continue;
        }

        if (bind(ctx->fd, ain->ai_addr, ain->ai_addrlen) != 0) {
            TRRET(MRKRPC_CTX_SET_ME + 2);
        }

        break;
    }

    freeaddrinfo(ai);

    if (ctx->fd < 0) {
        TRRET(MRKRPC_CTX_SET_ME + 3);
    }

    return 0;
}


int
mrkrpc_ctx_register_op(mrkrpc_ctx_t *ctx, uint8_t op, mrkrpc_op_entry_t *ope)
{
    mrkrpc_op_entry_t **p;

    assert(op < MRKRPC_MAX_OPS);

    if ((p = array_get(&ctx->ops, op)) == NULL) {
        FAIL("array_get");
    }
    *p = ope;
    return 0;
}

mrkrpc_op_entry_t *
mrkrpc_ctx_get_op(mrkrpc_ctx_t *ctx, uint8_t op)
{
    mrkrpc_op_entry_t **p;

    assert(op < MRKRPC_MAX_OPS);

    if ((p = array_get(&ctx->ops, op)) == NULL) {
        return NULL;
    }
    return *p;
}


/* module */

void
mrkrpc_init(void)
{
    mrkrpc_queue_entry_t *tmp;
    mrkrpc_node_t *n;

    if (mflags & MRKRPC_MFLAG_INITIALIZED) {
        return;
    }

    sidbase = (((uint64_t)random()) << 32) | ((uint64_t)random());

    MEMDEBUG_REGISTER(mrkrpc);

    mrkdata_init();

    if ((header_spec = mrkdata_make_spec(MRKDATA_STRUCT)) == NULL) {
        FAIL("mrkdata_make_spec");
    }
    /* version */
    mrkdata_spec_add_field(header_spec, mrkdata_make_spec(MRKDATA_UINT8));
    /* op */
    mrkdata_spec_add_field(header_spec, mrkdata_make_spec(MRKDATA_UINT8));
    /* nid */
    mrkdata_spec_add_field(header_spec, mrkdata_make_spec(MRKDATA_UINT64));
    /* sid */
    mrkdata_spec_add_field(header_spec, mrkdata_make_spec(MRKDATA_UINT64));

    /* discover default message size */
    n = mrkrpc_make_node(0, NULL, 0);
    tmp = make_queue_entry(n, n, 0, 0);
    pack_queue_entry(tmp);
    calculated_default_message_sz = tmp->sz + QUEUE_ENTRY_DEFAULT_BUFSZ;
    queue_entry_destroy(&tmp);
    mrkrpc_node_destroy(&n);

    mflags |= MRKRPC_MFLAG_INITIALIZED;
}

void
mrkrpc_fini(void)
{
    if (! (mflags & MRKRPC_MFLAG_INITIALIZED)) {
        return;
    }

    mrkdata_fini();

    mflags &= ~MRKRPC_MFLAG_INITIALIZED;
}

