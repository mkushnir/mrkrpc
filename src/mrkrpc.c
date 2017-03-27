#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
/* IPTOS_ */
#include <netinet/ip.h>
#include <netdb.h>
/* inet_ntop(3) */
#include <arpa/inet.h>

#include <mrkcommon/array.h>
//#define TRRET_DEBUG_VERBOSE
#include <mrkcommon/dumpm.h>
#include <mrkcommon/btrie.h>
#include <mrkcommon/util.h>
#include <mrkdata.h>
#include <mrkthr.h>

#include "mrkrpc_private.h"

#include "diag.h"

#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkrpc);

#define MRKRPC_MFLAG_INITIALIZED (0x01)
#define MRKRPC_MFLAG_SHUTDOWN (0x02)
static unsigned mflags = 0;

#define MRKRPC_VERSION 0x81
#define MRKRPC_MAX_OPS 256 /* uint8_t */
#define QUEUE_ENTRY_DEFAULT_BUFSZ 8192
#define NODE_DEFAULT_ADDRLEN 256

static mrkdata_spec_t *header_spec;
#define MRKRPC_HEADER_VER 0
#define MRKRPC_HEADER_OP 1
#define MRKRPC_HEADER_NID 2
#define MRKRPC_HEADER_SID 3
static size_t calculated_default_message_sz;

static mrkrpc_sid_t sidbase;
UNUSED static mrkrpc_sid_t sid = 0;

static mrkrpc_sid_t
new_sid(void)
{
    //return (((uint64_t)random()) << 32) | random();
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
    char buf[256];
    struct sockaddr_in *a;

    a = ((struct sockaddr_in *)(node->addr));

#ifndef __GNUC__
    assert(sizeof(buf) >= a->sin_len);
#endif

    CTRACE("<node nid=%016lx @ %s:%d>",
           node->nid,
           inet_ntop(a->sin_family, &a->sin_addr, buf, a->sin_len),
           ntohs(a->sin_port));

    return 0;
}

int
mrkrpc_node_str(mrkrpc_node_t *node, char *out, size_t sz)
{
    char buf[256];
    struct sockaddr_in *a;

    a = ((struct sockaddr_in *)(node->addr));

#ifndef __GNUC__
    assert(sizeof(buf) >= a->sin_len);
#endif

    return snprintf(out,
                    sz,
                    "<node nid=%016lx @ %s:%d>",
                    node->nid,
                    inet_ntop(a->sin_family, &a->sin_addr, buf, a->sin_len),
                    ntohs(a->sin_port));
}

mrkrpc_node_t *
mrkrpc_make_node_from_addr(mrkrpc_nid_t nid,
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
    if ((res->addr = malloc(addrlen)) == NULL) {
        FAIL("malloc");
    }
    memcpy(res->addr, addr, addrlen);
    return res;
}

int
mrkrpc_node_init_from_addr(mrkrpc_node_t *node,
                           mrkrpc_nid_t nid,
                           const struct sockaddr *addr,
                           socklen_t addrlen)
{
    node->nid = nid;
    node->hostname = NULL;
    node->port = 0;
    node->addrlen = addrlen;
    if ((node->addr = malloc(addrlen)) == NULL) {
        FAIL("malloc");
    }
    memcpy(node->addr, addr, addrlen);
    return 0;
}

int
mrkrpc_node_init_from_params(mrkrpc_ctx_t *ctx,
                             mrkrpc_node_t *node,
                             mrkrpc_nid_t nid,
                             const char *hostname,
                             int port)
{
    struct addrinfo hints, *ai = NULL, *ain;
    char portstr[32];

    node->nid = nid;
    if (hostname != NULL) {
        if ((node->hostname = strdup(hostname)) == NULL) {
            FAIL("strdup");
        }
        node->port = port;

        snprintf(portstr, sizeof(portstr), "%d", port);

        memset(&hints, '\0', sizeof(struct addrinfo));
        hints.ai_family = ctx->family;
        hints.ai_socktype = ctx->socktype;
        hints.ai_protocol = ctx->protocol;


        if (getaddrinfo(hostname, portstr, &hints, &ai) != 0) {
            TRRET(MRKRPC_MAKE_NODE_FROM_PARAMS + 1);
        }

        for (ain = ai; ain != NULL; ain = ain->ai_next) {
            /* stop at the fisrt getaddrinfo() entry */
            break;
        }

        if (ain == NULL) {
            TRRET(MRKRPC_MAKE_NODE_FROM_PARAMS + 2);
        }

        if ((node->addr = malloc(ain->ai_addrlen)) == NULL) {
            FAIL("malloc");
        }
        memcpy(node->addr, ain->ai_addr, ain->ai_addrlen);
        node->addrlen = ain->ai_addrlen;
        freeaddrinfo(ai);
    } else {
        node->hostname = NULL;
        node->port = port;
        node->addrlen = NODE_DEFAULT_ADDRLEN;
        if ((node->addr = malloc(node->addrlen)) == NULL) {
            FAIL("malloc");
        }
        memset(node->addr, '\0', node->addrlen);
    }

    return 0;
}

mrkrpc_node_t *
mrkrpc_make_node_from_params(mrkrpc_ctx_t *ctx,
                             mrkrpc_nid_t nid,
                             const char *hostname,
                             int port)
{
    mrkrpc_node_t *res;

    if ((res = malloc(sizeof(mrkrpc_node_t))) == NULL) {
        FAIL("malloc");
    }

    if (mrkrpc_node_init_from_params(ctx, res, nid, hostname, port) != 0) {
        free(res);
        res = NULL;
    }
    return res;
}

void
mrkrpc_node_copy(mrkrpc_node_t *dst, mrkrpc_node_t *src)
{
    dst->nid = src->nid;
    if (src->hostname != NULL) {
        if ((dst->hostname = strdup(src->hostname)) == NULL) {
            FAIL("strdup");
        }
    }
    dst->port = src->port;
    if (src->addrlen > 0) {
        if ((dst->addr = malloc(src->addrlen)) == NULL) {
            FAIL("nallos");
        }
        memcpy(dst->addr, src->addr, src->addrlen);
        dst->addrlen = src->addrlen;
    }
}


int
mrkrpc_nodes_equal(mrkrpc_node_t *a, mrkrpc_node_t *b)
{
    /* XXX uint64_t implied */
    return (a->nid == b->nid) &&
           (a->addrlen == b->addrlen) &&
           (memcmp(a->addr, b->addr, a->addrlen) == 0);
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
                 uint8_t sendop,
                 uint8_t recvop,
                 mrkrpc_nid_t nid,
                 mrkrpc_sid_t sid)
{
    e->peer = peer;
    e->sendop = sendop;
    e->recvop = recvop;
    e->nid = nid;
    e->sid = sid;
    e->senddat = NULL;
    e->buf = NULL;
    e->sz = 0;
    e->recvdat = NULL;
    e->res = 0;
    DTQUEUE_ENTRY_INIT(link, e);
    mrkthr_signal_fini(&e->signal);
    mrkthr_rwlock_init(&e->rwlock);
}

static void
queue_entry_fini(mrkrpc_queue_entry_t *e)
{
    e->peer = NULL;
    e->sendop = 0;
    e->recvop = 0;
    e->nid = 0;
    e->sid = 0;
    e->senddat = NULL;
    if (e->buf != NULL) {
        free(e->buf);
        e->buf = NULL;
    }
    e->sz = 0;
    e->recvdat = NULL;
    e->res = 0;
    DTQUEUE_ENTRY_FINI(link, e);
    mrkthr_rwlock_fini(&e->rwlock);
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
                 uint8_t sendop,
                 uint8_t recvop,
                 mrkrpc_sid_t sid)
{
    mrkrpc_queue_entry_t *qe;

    if ((qe = malloc(sizeof(mrkrpc_queue_entry_t))) == NULL) {
        FAIL("malloc");
    }

    queue_entry_init(qe, peer, sendop, recvop, me->nid, sid);

    return qe;
}

static mrkdata_datum_t *
make_header(uint8_t op, mrkrpc_nid_t nid, mrkrpc_sid_t sid)
{
    mrkdata_datum_t *header;

    if ((header = mrkdata_datum_from_spec(header_spec, NULL, 0)) == NULL) {
        return NULL;
    }
    mrkdata_datum_add_field(header, mrkdata_datum_make_u8(MRKRPC_VERSION));
    mrkdata_datum_add_field(header, mrkdata_datum_make_u8(op));
    mrkdata_datum_add_field(header, mrkdata_datum_make_u64(nid));
    mrkdata_datum_add_field(header, mrkdata_datum_make_u64(sid));
    mrkdata_datum_add_field(header, mrkdata_datum_make_u64(0));
    return header;
}


static int
pack_queue_entry(mrkrpc_queue_entry_t *qe)
{
    unsigned char *pbuf;
    size_t nwritten;
    mrkdata_datum_t *header;

    if ((header = make_header(qe->sendop, qe->nid, qe->sid)) == NULL) {
        TRRET(PACK_QUEUE_ENTRY + 1);
    }

    if (qe->senddat != NULL) {
        if ((qe->buf = malloc(header->packsz +
                              qe->senddat->packsz)) == NULL) {
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
queue_entry_enqueue(mrkrpc_queue_t *queue,
                    mrkthr_signal_t *signal,
                    mrkrpc_queue_entry_t *qe)
{
    DTQUEUE_ENQUEUE(queue, link, qe);
    mrkthr_signal_send(signal);
}

static mrkrpc_queue_entry_t *
queue_entry_dequeue(mrkrpc_queue_t *queue,
                    mrkthr_signal_t *signal)
{
    mrkrpc_queue_entry_t *qe;

    while (!(mflags & MRKRPC_MFLAG_SHUTDOWN)) {
        if (DTQUEUE_HEAD(queue) == NULL) {
            if (mrkthr_signal_subscribe(signal) != 0) {
                break;
            }
        } else {
            qe = DTQUEUE_HEAD(queue);
            DTQUEUE_DEQUEUE(queue, link);
            DTQUEUE_ENTRY_FINI(link, qe);
            return qe;
        }
    }
    CTRACE("exiting ...");
    TRRETNULL(QUEUE_ENTRY_DEQUEUE + 1);
}

static void
queue_entry_dump(mrkrpc_queue_entry_t *qe)
{
    CTRACE("<qe sendop=%hhu recvop=%hhu nid=%016lx peernid=%016lx sid=%016lx res=%08x>",
           qe->sendop,
           qe->recvop,
           qe->nid,
           qe->peer->nid,
           qe->sid,
           qe->res);
}

UNUSED static void
queue_dump(mrkrpc_queue_t *queue)
{
    mrkrpc_queue_entry_t *qe;

    CTRACE("queue:");
    for (qe = DTQUEUE_HEAD(queue);
         qe != NULL;
         qe = DTQUEUE_NEXT(link, qe)) {
        queue_entry_dump(qe);
    }
    CTRACE("end of queue");
}

static int
sendthr_loop(UNUSED int argc, void *argv[])
{
    mrkrpc_ctx_t *ctx;

    assert(argc == 1);
    ctx = argv[0];


    while (!(mflags & MRKRPC_MFLAG_SHUTDOWN)) {
        mrkrpc_queue_entry_t *qe;

        if ((qe = queue_entry_dequeue(&ctx->sendq,
                                      &ctx->sendq_signal)) == NULL) {
            break;
        }

        //CTRACE("dequeued entry for sending:");
        //queue_entry_dump(qe);

        /*
         * Remember this qe if it has the signal owner (!), recvthr_loop()
         * or mrkrpc_call() will retrieve it at response time.
         */
        if (mrkthr_signal_has_owner(&qe->signal)) {
            mnbtrie_node_t *trn;
            if ((trn = btrie_add_node(&ctx->pending, qe->sid)) == NULL) {
                FAIL("btrie_add_node");
            }
            trn->value = qe;
        }

        if (qe->res != 0) {
            CTRACE("qe->res=%d, ignoring this qe:", qe->res);
            queue_entry_dump(qe);

        } else {
            if (pack_queue_entry(qe) != 0) {
                CTRACE("pack_queue_entry failure on this qe:");
                queue_entry_dump(qe);
                qe->res = SENDTHR_LOOP + 1;
                mrkthr_signal_send(&qe->signal);
                continue;
            }

            if (mrkthr_rwlock_try_acquire_write(&qe->rwlock) == 0) {
                if (mrkthr_sendto_all(ctx->fd,
                                      qe->buf,
                                      qe->sz,
                                      0,
                                      qe->peer->addr,
                                      qe->peer->addrlen) != 0) {
                    perror("sendto");
                } else {
                    ++(ctx->nsent);
                }
                mrkthr_rwlock_release_write(&qe->rwlock);
            }
        }
        //CTRACE("sent %hhd %016lx", qe->sendop, qe->sid);
        if (!mrkthr_signal_has_owner(&qe->signal)) {
            mrkrpc_node_destroy(&qe->peer);
            mrkdata_datum_destroy(&qe->recvdat);
            mrkdata_datum_destroy(&qe->senddat);
            queue_entry_destroy(&qe);
        }
    }

    CTRACE("exiting ...");
    TRRET(0);
}

static mrkrpc_op_entry_t *
get_op_entry(mrkrpc_ctx_t *ctx, uint8_t op)
{
#ifndef __GNUC__
    assert(op < MRKRPC_MAX_OPS);
#endif
    return array_get(&ctx->ops, op);
}

static int
_reqhandler(UNUSED int argc, void **argv)
{
    int res = 0;
    mrkrpc_recv_handler_t h;
    mrkrpc_ctx_t *ctx;
    mrkrpc_queue_entry_t *qe;

    assert(argc == 3);
    h = argv[0];
    ctx = argv[1];
    qe = argv[2];

    qe->res = h(ctx, qe);
    if (qe->res == 0) {
        queue_entry_enqueue(&ctx->sendq,
                            &ctx->sendq_signal,
                            qe);
        //CTRACE("enqueued %hhx %016lx", qe->sendop, qe->sid);
    } else {
        res = qe->res;
        CTRACE("reqhandler returned %s", diag_str(qe->res));
        CTRACE("discarding this qe:");
        queue_entry_dump(qe);
        //D32(qe, sizeof(*qe));

        mrkrpc_node_destroy(&qe->peer);
        mrkdata_datum_destroy(&qe->recvdat);
        mrkdata_datum_destroy(&qe->senddat);
        queue_entry_destroy(&qe);
    }
    return res;
}


static ssize_t
process_one_msg(mrkrpc_ctx_t *ctx,
                unsigned char *buf,
                ssize_t bufsz,
                char *addr,
                socklen_t addrlen)
{
    ssize_t             ndecoded;
    mrkdata_datum_t    *header = NULL;
    mrkdata_datum_t    *ver = NULL;
    mrkdata_datum_t    *op = NULL;
    mrkdata_datum_t    *nid = NULL;
    mrkdata_datum_t    *sid = NULL;
    mnbtrie_node_t     *trn = NULL;
    unsigned char      *pbuf = buf;

    /* unpack header */
    if ((ndecoded = mrkdata_unpack_buf(header_spec,
                                       pbuf,
                                       bufsz,
                                       &header)) == 0) {

        CTRACE("mrkdata_unpack_buf failure, header:");
        D16(pbuf, bufsz);

        goto bad;
    }

    pbuf += ndecoded;
    bufsz -= ndecoded;
    ++(ctx->nrecvd);

    //CTRACE("bufsz=%ld ndecoded=%ld", bufsz, ndecoded);

    /* check version */
    if ((ver = mrkdata_datum_get_field(header,
                                       MRKRPC_HEADER_VER)) == NULL) {
        CTRACE("corrupt header or spec");
        mrkdata_datum_dump(header);
        goto end;
    }
    //CTRACE("received ver %x", ver->value.u8);

    /* check op */
    if ((op = mrkdata_datum_get_field(header,
                                      MRKRPC_HEADER_OP)) == NULL) {
        CTRACE("corrupt header or spec");
        mrkdata_datum_dump(header);
        goto end;
    }
    //CTRACE("received op %x", op->value.u8);

    /* check nid */
    if ((nid = mrkdata_datum_get_field(header,
                                       MRKRPC_HEADER_NID)) == NULL) {
        CTRACE("corrupt header or spec");
        mrkdata_datum_dump(header);
        goto end;
    }
    //CTRACE("received nid %016lx", nid->value.u64);

    /* check sid */
    if ((sid = mrkdata_datum_get_field(header,
                                       MRKRPC_HEADER_SID)) == NULL) {
        CTRACE("corrupt header or spec");
        mrkdata_datum_dump(header);
        goto end;
    }
    //CTRACE("received sid %016lx", sid->value.u64);
    //CTRACE("received %hhd %016lx", op->value.u8, sid->value.u64);

    /* find the saved queue entry under header:sid */

    if ((trn = btrie_find_exact(&ctx->pending, sid->value.u64)) != NULL) {
        mrkrpc_queue_entry_t *qe;
        mrkrpc_op_entry_t *ope;

        //CTRACE("seeing response ...");
        assert(trn->value != NULL);

        /*
         * This is a request queue entry that is waiting for response.
         * Update it with response datum and wake the requesting
         * thread.
         */

        qe = trn->value;
        btrie_remove_node(&ctx->pending, trn);

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
        if ((ope = get_op_entry(ctx, op->value.u8)) == NULL) {
            /* XXX any indication to the caller? */
            CTRACE("op is not supported: %02x, original op was: %02x",
                   op->value.u8,
                   qe->sendop);

        } else {
            qe->recvop = op->value.u8;
            if (ope->spec != NULL) {
                if ((ndecoded = mrkdata_unpack_buf(ope->spec,
                                                   pbuf,
                                                   bufsz,
                                                   &qe->recvdat)) == 0) {
                    CTRACE("mrkdata_unpack_buf failure, resp 1:");
                    //mrkdata_spec_dump(ope->spec);
                    D16(pbuf, bufsz);
                    if (qe->recvdat != NULL) {
                        free(qe->recvdat);
                        qe->recvdat = NULL;
                        goto bad;
                    }
                }
                pbuf += ndecoded;
                bufsz -= ndecoded;
                //CTRACE("bufsz=%ld recvdat", bufsz);
            } else {
                //CTRACE("spec is NULL");
            }
        }

        /*
         * for this assertion, see mrkrpc_call()
         */
        //CTRACE("qe->signal.owner=%p", qe->signal.owner);
        //assert(mrkthr_signal_has_owner(&qe->signal));
        if (!mrkthr_signal_has_owner(&qe->signal)) {
            CTRACE("timed out qe:");
            queue_entry_dump(qe);
        }
        mrkthr_signal_send(&qe->signal);
        //CTRACE("signal sent");

    } else {
        mrkrpc_op_entry_t *ope;

        //CTRACE("seeing request or a timed out response ...");
        /*
         * No qe found:
         *  - an incoming request, or
         *  - a timed out response.
         */

        if ((ope = get_op_entry(ctx, op->value.u8)) == NULL) {
            CTRACE("op is not supported: %02x (discarding)",
                   op->value.u8);

        } else {
            mrkrpc_node_t *from;
            mrkrpc_queue_entry_t *qe;

            if ((from = mrkrpc_make_node_from_addr(nid->value.u64,
                    (const struct sockaddr *)addr, addrlen)) == NULL) {

                /* XXX handle it better */
                CTRACE("mrkrpc_make_node failure");
            }

            qe = make_queue_entry(&ctx->me,
                                  from,
                                  0,
                                  op->value.u8,
                                  sid->value.u64);

            if (ope->spec != NULL) {
                /* looks like a request */
                if ((ndecoded = mrkdata_unpack_buf(ope->spec,
                                                   pbuf,
                                                   bufsz,
                                                   &qe->recvdat)) == 0) {
                    CTRACE("mrkdata_unpack_buf failure, req:");
                    D16(pbuf, bufsz);

                    /*
                     * take care of this qe
                     */
                    mrkrpc_node_destroy(&qe->peer);
                    mrkdata_datum_destroy(&qe->recvdat);
                    mrkdata_datum_destroy(&qe->senddat);
                    queue_entry_destroy(&qe);

                    goto bad;
                }
                pbuf += ndecoded;
                bufsz -= ndecoded;
                //CTRACE("bufsz=%ld recvdat", bufsz);
            }


            /*
             * In most cases reqhandler won't be NULL, since this is
             * the place where qe->recvdat has to be analyzed and the
             * response has to be constructed and placed in qe->senddat.
             */
            if (ope->reqhandler != NULL) {
                mrkthr_spawn("_reqhandler",
                             _reqhandler,
                             3,
                             ope->reqhandler,
                             ctx,
                             qe);
            } else {
                CTRACE("discarding this qe without reqhandler:");
                queue_entry_dump(qe);

                mrkrpc_node_destroy(&qe->peer);
                mrkdata_datum_destroy(&qe->recvdat);
                mrkdata_datum_destroy(&qe->senddat);
                queue_entry_destroy(&qe);
            }
        }
    }

end:
    //CTRACE("destroying header: %p", header);
    mrkdata_datum_destroy(&header);

    return (ssize_t)((intptr_t)pbuf - (intptr_t)buf);

bad:
    pbuf = buf;
    goto end;
}


static int
recvthr_loop(UNUSED int argc, void *argv[])
{
    mrkrpc_ctx_t *ctx;
    char addr[NODE_DEFAULT_ADDRLEN];
    socklen_t addrlen = NODE_DEFAULT_ADDRLEN;
    /* buf is a reusable memory to store the raw datagram */
    unsigned char *buf;

    assert(argc == 1);
    ctx = argv[0];

    //CTRACE("started recvthr_loop");

    if ((buf = malloc(calculated_default_message_sz)) == NULL) {
        FAIL("malloc");
    }

    while (!(mflags & MRKRPC_MFLAG_SHUTDOWN)) {
        ssize_t nread;
        unsigned char *pbuf;

        if ((nread = mrkthr_recvfrom_allb(ctx->fd,
                                          buf,
                                          calculated_default_message_sz,
                                          0,
                                          (struct sockaddr *)addr,
                                          &addrlen)) < 0) {
            /* mrkthr_recvfrom_allb() never returns 0 */
            CTRACE("EOF?");
            break;
        }

        pbuf = buf;

        //D16(buf, nread);
        //CTRACE("nread=%ld", nread);

        while (nread > 0) {
            ssize_t nprocessed;
            nprocessed = process_one_msg(ctx, pbuf, nread, addr, addrlen);
            pbuf += nprocessed;
            nread -= nprocessed;

            //CTRACE(" nprocessed=%ld nread=%ld", nprocessed, nread);

            if (nprocessed <= 0) {
                break;
            }
        }
    }

    free(buf);
    buf = NULL;

    CTRACE("exiting ...");
    TRRET(0);
}

int
mrkrpc_serve(mrkrpc_ctx_t *ctx)
{
    return mrkthr_join(ctx->recvthr);
}


static int
enroll_queue_entry(int argc, void **argv)
{
    int res = 0;
    mrkrpc_ctx_t *ctx;
    mrkrpc_queue_entry_t *qe;

    assert(argc == 2);
    ctx = argv[0];
    qe = argv[1];

    queue_entry_enqueue(&ctx->sendq, &ctx->sendq_signal, qe);
    //TRACE(">>> C: op=%hhu nid=%016lx->%016lx sid=%016lx", qe->sendop, qe->nid, qe->peer->nid, qe->sid);
    //queue_dump(&ctx->sendq);

    /*
     * Set signal ownerhipto tell sendthr_loop() that we are going to take
     * care of this qe.
     */
    mrkthr_signal_init(&qe->signal, mrkthr_me());
    if ((res = mrkthr_signal_subscribe(&qe->signal)) != 0) {
        //CTRACE("mrkthr_signal_subscribe: %s", mrkthr_diag_str(res));
        res = ENROLL_QUEUE_ENTRY + 1;
    } else {
    }

    /*
     * Provide for the retval both for the "wait for" call and a regular
     * one.
     */
    MRKTHRET(res);
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
    mnbtrie_node_t *trn;

    if (arg != NULL && arg->packsz > QUEUE_ENTRY_DEFAULT_BUFSZ) {
        TRACE("arg->packsz: %ld > %d", arg->packsz, QUEUE_ENTRY_DEFAULT_BUFSZ);
        mrkdata_datum_dump(arg);
    }

    qe = make_queue_entry(&ctx->me, to, op, 0, new_sid());
    qe->senddat = arg;

    /*
     * fall asleep until we receive response
     */
    if (ctx->call_timeout > 0) {
        if ((res = mrkthr_wait_for(ctx->call_timeout,
                                   "[mrkthr_wait_for enroll_queue_entry]",
                                   enroll_queue_entry,
                                   2,
                                   ctx, qe)) == MRKTHR_WAIT_TIMEOUT) {
            res = MRKRPC_CALL_TIMEOUT;

        } else if (res != 0) {
            TRACE("enroll_queue_entry: %s", diag_str(res));
        }

    } else {
        void *argv[2] = {ctx, qe};
        res = enroll_queue_entry(2, argv);
    }

    if (mflags & MRKRPC_MFLAG_SHUTDOWN) {
        /* just give away */
        TRRET(MRKRPC_CALL + 1);
    }

    //TRACE("<<< C: op=%hhu nid=%016lx<-%016lx sid=%016lx", qe->recvop, qe->nid, qe->peer->nid, qe->sid);

    /* response */

    if (mrkthr_rwlock_acquire_write(&qe->rwlock) != 0) {
        TRRET(MRKRPC_CALL + 2);
    }
    /*
     * qe->recvdat is a return value. We should return it back to the
     * caller. The caller is responsible to dispose it.
     */
    *rv = qe->recvdat;
    if (res == 0) {
        /*
         * There was not a timeout (if configured), otherwise we return
         * simething like MRKTHR_WAIT_TIMEOUT.
         */
        res = qe->res;
    }
    if ((trn = btrie_find_exact(&ctx->pending, qe->sid)) != NULL) {
        assert(trn->value == qe);
        trn->value = NULL;
        btrie_remove_node(&ctx->pending, trn);
    }

    if (!DTQUEUE_ORPHAN(&ctx->sendq, link, qe)) {
        DTQUEUE_REMOVE(&ctx->sendq, link, qe);
    }

    mrkthr_rwlock_release_write(&qe->rwlock);

    /* Clear signal wnership, we are done. */
    mrkthr_signal_fini(&qe->signal);

    queue_entry_destroy(&qe);

    TRRET(res);
}

int
mrkrpc_run(mrkrpc_ctx_t *ctx)
{
    /* sendq */
    ctx->sendthr = mrkthr_spawn("sendthr", sendthr_loop, 1, ctx);
    mrkthr_signal_init(&ctx->sendq_signal, ctx->sendthr);

    /* recvq */
    ctx->recvthr = mrkthr_spawn("recvthr", recvthr_loop, 1, ctx);

    return 0;
}

/* ctx */

int
mrkrpc_ctx_init(mrkrpc_ctx_t *ctx)
{
    ctx->family = PF_INET;
    ctx->socktype = SOCK_DGRAM;
    ctx->protocol = IPPROTO_UDP;

    /* node */
    mrkrpc_node_init(&ctx->me);

    /* fd */
    ctx->fd = -1;

    /* ops */
    if (array_init(&ctx->ops, sizeof(mrkrpc_op_entry_t), MRKRPC_MAX_OPS,
                   (array_initializer_t)null_init,
                   (array_finalizer_t)null_fini) != 0) {
        FAIL("array_init");
    }

    /* sendq */
    ctx->sendthr = NULL;
    mrkthr_signal_init(&ctx->sendq_signal, NULL);
    DTQUEUE_INIT(&ctx->sendq);

    /* recvq */
    ctx->recvthr = NULL;
    ctx->call_timeout = 0;

    /* pending */
    btrie_init(&ctx->pending);
    ctx->nsent = 0;
    ctx->nrecvd = 0;

    return 0;
}

void
mrkrpc_ctx_set_call_timeout(mrkrpc_ctx_t *ctx, uint64_t timeout)
{
    ctx->call_timeout = timeout;
}


void
mrkrpc_ctx_close(mrkrpc_ctx_t *ctx)
{
    /* fd */
    if (ctx->fd != -1) {
        close(ctx->fd);
        ctx->fd = -1;
    }

    mrkthr_signal_send(&ctx->sendq_signal);
}


int
mrkrpc_ctx_fini(mrkrpc_ctx_t *ctx)
{
    mrkrpc_queue_entry_t *e;

    /* fd */
    mrkrpc_ctx_close(ctx);

    /* XXX clean up queue entries */
    btrie_fini(&ctx->pending);
    ctx->nsent = 0;
    ctx->nrecvd = 0;

    /* sendq */
    if (ctx->sendthr != NULL) {
        mrkthr_set_interrupt_and_join(ctx->sendthr);
        mrkthr_signal_fini(&ctx->sendq_signal);
        ctx->sendthr = NULL;
    }

    while ((e = DTQUEUE_HEAD(&ctx->sendq)) != NULL) {
        DTQUEUE_DEQUEUE(&ctx->sendq, link);
        queue_entry_destroy(&e);
    }
    DTQUEUE_FINI(&ctx->sendq);

    /* recvq */
    if (ctx->recvthr != NULL) {
        mrkthr_set_interrupt_and_join(ctx->recvthr);
        ctx->recvthr = NULL;
    }

    /* ops */
    if (array_fini(&ctx->ops) != 0) {
        FAIL("array_fini");
    }

    /* node */
    mrkrpc_node_fini(&ctx->me);

    return 0;
}

int
mrkrpc_ctx_set_local_node(mrkrpc_ctx_t *ctx,
                          mrkrpc_nid_t nid,
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
    hints.ai_family = ctx->family;
    hints.ai_socktype = ctx->socktype;
    hints.ai_protocol = ctx->protocol;

    if (ctx->fd != -1) {
        close(ctx->fd);
    }

    if (getaddrinfo(hostname, portstr, &hints, &ai) != 0) {
        TRRET(MRKRPC_CTX_SET_LOCAL_NODE + 1);
    }

    for (ain = ai; ain != NULL; ain = ain->ai_next) {
        int optval;
        ctx->family = ain->ai_family;
        ctx->socktype = ain->ai_socktype;
        ctx->protocol = ain->ai_protocol;

        if ((ctx->fd = socket(ain->ai_family,
                              ain->ai_socktype,
                              ain->ai_protocol)) < 0) {
            continue;
        }

        optval = IPTOS_LOWDELAY;
        if (setsockopt(ctx->fd, IPPROTO_IP, IP_TOS, &optval, sizeof(optval)) != 0) {
            perror("setsockopt tos");
        }

        optval = 1;
        if (setsockopt(ctx->fd, SOL_SOCKET, SO_SNDLOWAT, &optval, sizeof(optval)) != 0) {
            perror("setsockopt");
            FAIL("...");
        }

        if ((ctx->me.addr = malloc(ain->ai_addrlen)) == NULL) {
            FAIL("malloc");
        }
        memcpy(ctx->me.addr, ain->ai_addr, ain->ai_addrlen);
        ctx->me.addrlen = ain->ai_addrlen;

        if (bind(ctx->fd, ain->ai_addr, ain->ai_addrlen) != 0) {
            TRRET(MRKRPC_CTX_SET_LOCAL_NODE + 2);
        }

        break;
    }

    freeaddrinfo(ai);

    if (ctx->fd < 0) {
        TRRET(MRKRPC_CTX_SET_LOCAL_NODE + 3);
    }

    return 0;
}


int
mrkrpc_ctx_register_msg(mrkrpc_ctx_t *ctx,
                       uint8_t op,
                       mrkdata_spec_t *spec,
                       mrkrpc_recv_handler_t reqhandler)
{
    mrkrpc_op_entry_t *ope;

#ifndef __GNUC__
    assert(op < MRKRPC_MAX_MSGS);
#endif

    if ((ope = array_get(&ctx->ops, op)) == NULL) {
        FAIL("array_get");
    }
    ope->spec = spec;
    ope->reqhandler = reqhandler;
    return 0;
}

size_t
mrkrpc_ctx_get_pending_volume(mrkrpc_ctx_t *ctx)
{
    return btrie_get_volume(&ctx->pending);
}

size_t
mrkrpc_ctx_get_pending_length(mrkrpc_ctx_t *ctx)
{
    return btrie_get_nvals(&ctx->pending);
}

size_t
mrkrpc_ctx_get_sendq_length(mrkrpc_ctx_t *ctx)
{
    return DTQUEUE_LENGTH(&ctx->sendq);
}


size_t
mrkrpc_ctx_compact_pending(mrkrpc_ctx_t *ctx, size_t threshold)
{
    size_t length;

    length = btrie_get_volume(&ctx->pending);
    if (length > threshold) {
        btrie_cleanup(&ctx->pending);
    }
    return length;
}


/* module */

void
mrkrpc_shutdown(void)
{
    mflags |= MRKRPC_MFLAG_SHUTDOWN;
}


void
mrkrpc_init(void)
{
    mrkrpc_queue_entry_t *tmp;
    mrkrpc_node_t *n;
    mrkrpc_ctx_t ctx;

    if (mflags & MRKRPC_MFLAG_INITIALIZED) {
        return;
    }

    MEMDEBUG_REGISTER(mrkrpc);

    mrkdata_init();
    /*
     * High 32 bits of sid will be random. Low 32 bits will be
     * incrementing.
     */
    srandomdev();

    sidbase = (((uint64_t)random()) << 32);
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
    /* frame number */
    mrkdata_spec_add_field(header_spec, mrkdata_make_spec(MRKDATA_UINT64));

    /* discover default message size */
    mrkrpc_ctx_init(&ctx);
    n = mrkrpc_make_node_from_params(&ctx, 0, NULL, 0);
    tmp = make_queue_entry(n, n, 0, 0, 0);
    pack_queue_entry(tmp);
    calculated_default_message_sz = tmp->sz + QUEUE_ENTRY_DEFAULT_BUFSZ;
    queue_entry_destroy(&tmp);
    mrkrpc_node_destroy(&n);
    mrkrpc_ctx_fini(&ctx);

    mflags |= MRKRPC_MFLAG_INITIALIZED;
}

void
mrkrpc_fini(void)
{
    if (! (mflags & MRKRPC_MFLAG_INITIALIZED)) {
        return;
    }

    mrkdata_spec_destroy(&header_spec);

    mrkdata_fini();

    mflags &= ~MRKRPC_MFLAG_INITIALIZED;
}

