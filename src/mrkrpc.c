#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
/* inet_ntop(3) */
#include <arpa/inet.h>

#include <mrkcommon/array.h>
#define TRRET_DEBUG_VERBOSE
#include <mrkcommon/dumpm.h>
#include <mrkcommon/trie.h>
#include <mrkcommon/util.h>
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkrpc);
#include <mrkdata.h>
#include <mrkthr.h>

#include "mrkrpc_private.h"

#include "diag.h"

#define MRKRPC_MFLAG_INITIALIZED (0x01)
#define MRKRPC_MFLAG_SHUTDOWN (0x02)
static unsigned mflags = 0;

#define MRKRPC_PENDING_TRIE_VOLUME_THRESHOLD 50000

#define MRKRPC_VERSION 0x81
#define MRKRPC_MAX_MSGS 256 /* uint8_t */
#define QUEUE_ENTRY_DEFAULT_BUFSZ 1024
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
    return (((uint64_t)random()) << 32) | random();
    //return ++sid ^ sidbase;
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
                 uint8_t op,
                 mrkrpc_nid_t nid,
                 mrkrpc_sid_t sid)
{
    e->peer = peer;
    e->op = op;
    e->nid = nid;
    e->sid = sid;
    e->senddat = NULL;
    e->buf = NULL;
    e->sz = 0;
    e->recvdat = NULL;
    e->res = 0;
    e->next = NULL;
    mrkthr_signal_fini(&e->signal);
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
    e->res = 0;
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
                 mrkrpc_sid_t sid)
{
    mrkrpc_queue_entry_t *qe;

    if ((qe = malloc(sizeof(mrkrpc_queue_entry_t))) == NULL) {
        FAIL("malloc");
    }

    queue_entry_init(qe, peer, op, me->nid, sid);

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

    while (!(mflags & MRKRPC_MFLAG_SHUTDOWN)) {
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
    TRACE("exiting ...");
    TRRETNULL(QUEUE_ENTRY_DEQUEUE + 1);
}

static void
queue_entry_dump(mrkrpc_queue_entry_t *qe)
{
    CTRACE("<qe op=%02hhx nid=%016lx sid=%016lx res=%d>", qe->op, qe->nid, qe->sid, qe->res);
}


static int
sendthr_loop(UNUSED int argc, void *argv[])
{
    mrkrpc_ctx_t *ctx;

    assert(argc == 1);
    ctx = argv[0];


    while (!(mflags & MRKRPC_MFLAG_SHUTDOWN)) {
        mrkrpc_queue_entry_t *qe;

        if ((qe = queue_entry_dequeue(&ctx->sendq)) == NULL) {
            break;
        }

        if (qe->res != 0) {
            CTRACE("discarding this qe:");
            queue_entry_dump(qe);

        } else {
            if (pack_queue_entry(qe) != 0) {
                CTRACE("pack_queue_entry failure");
                qe->res = SENDTHR_LOOP + 1;
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
            ++(ctx->nsent);
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

    TRACE("exiting ...");
    TRRET(0);
}

static int
recvthr_loop(UNUSED int argc, void *argv[])
{
    mrkrpc_ctx_t *ctx;
    /* buf is a reusable memory to store the raw datagram */
    char *buf;

    assert(argc == 1);
    ctx = argv[0];

    if ((buf = malloc(calculated_default_message_sz)) == NULL) {
        FAIL("malloc");
    }

    while (!(mflags & MRKRPC_MFLAG_SHUTDOWN)) {
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
        ++(ctx->nrecvd);

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
            mrkrpc_msg_entry_t *msge;

            assert(trn->value != NULL);

            /*
             * This is a request queue entry that is waiting for response.
             * Update it with response datum and wake the requesting
             * thread.
             */

            qe = trn->value;
            trie_remove_node(&ctx->pending, trn);
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
            if ((msge = mrkrpc_ctx_get_msg(ctx, op->value.u8)) == NULL) {
                /* XXX any indication to the sender? */
                CTRACE("op is not supported: %02x, original op was: %02x",
                       op->value.u8,
                       qe->op);

            } else {
                if (msge->respspec != NULL) {
                    if ((ndecoded = mrkdata_unpack_buf(msge->respspec,
                                                       pbuf,
                                                       nread,
                                                       &qe->recvdat)) == 0) {
                        CTRACE("mrkdata_unpack_buf failure, resp:");
                        mrkdata_spec_dump(msge->respspec);
                        D16(pbuf, nread);
                        if (qe->recvdat != NULL) {
                            free(qe->recvdat);
                            qe->recvdat = NULL;
                            goto CONTINUE;
                        }
                    }
                    pbuf += ndecoded;
                    nread -= ndecoded;
                }

                //CTRACE("nread=%ld recvdat", nread);

                /* optional post-procesing */
                if (msge->resphandler != NULL) {
                    qe->res = msge->resphandler(ctx, qe);
                }
            }

            /*
             * for this assertion, see mrkrpc_call()
             */
            assert(mrkthr_signal_has_owner(&qe->signal));
            //CTRACE("qe->signal.owner=%p", qe->signal.owner);
            //mrkthr_dump(qe->signal.owner);
            mrkthr_signal_send(&qe->signal);

        } else {
            mrkdata_datum_t *req = NULL;
            mrkrpc_msg_entry_t *msge;
            mrkrpc_queue_entry_t *qe;
            mrkrpc_node_t *from;

            /*
             * Here is an incoming request.
             */

            if ((msge = mrkrpc_ctx_get_msg(ctx, op->value.u8)) == NULL) {
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

                if (msge->reqspec != NULL) {
                    if ((ndecoded = mrkdata_unpack_buf(msge->reqspec,
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
                }

                //CTRACE("nread=%ld recvdat", nread);

                /*
                 * In most cases reqhandler won't be NULL, since this is
                 * the place where qe->recvdat has to be analyzed and the
                 * response has to be constructed and placed in qe->senddat.
                 */
                if (msge->reqhandler != NULL) {
                    qe->res = msge->reqhandler(ctx, qe);
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

    TRACE("exiting ...");
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

    /*
     * qe->recvdat is a return value. We should return it back to the
     * caller. The caller is responsible to dispose it.
     */
    *rv = qe->recvdat;
    res = qe->res;
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
    if (array_init(&ctx->ops, sizeof(mrkrpc_msg_entry_t), MRKRPC_MAX_MSGS,
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
    ctx->nsent = 0;
    ctx->nrecvd = 0;

    return 0;
}

void
mrkrpc_ctx_close(mrkrpc_ctx_t *ctx)
{
    /* fd */
    if (ctx->fd != -1) {
        close(ctx->fd);
        ctx->fd = -1;
    }

    mrkthr_signal_send(&ctx->sendq.signal);
    mrkthr_signal_send(&ctx->recvq.signal);
}


int
mrkrpc_ctx_fini(mrkrpc_ctx_t *ctx)
{
    mrkrpc_queue_entry_t *e, *next;

    /* XXX clean up queue entries */
    trie_fini(&ctx->pending);
    ctx->nsent = 0;
    ctx->nrecvd = 0;

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

    /* node */
    mrkrpc_node_fini(&ctx->me);

    /* fd */
    mrkrpc_ctx_close(ctx);

    return 0;
}

int
mrkrpc_ctx_set_me(mrkrpc_ctx_t *ctx,
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
        TRRET(MRKRPC_CTX_SET_ME + 1);
    }

    for (ain = ai; ain != NULL; ain = ain->ai_next) {
        ctx->family = ain->ai_family;
        ctx->socktype = ain->ai_socktype;
        ctx->protocol = ain->ai_protocol;

        if ((ctx->fd = socket(ain->ai_family,
                              ain->ai_socktype,
                              ain->ai_protocol)) < 0) {
            continue;
        }

        if ((ctx->me.addr = malloc(ain->ai_addrlen)) == NULL) {
            FAIL("malloc");
        }
        memcpy(ctx->me.addr, ain->ai_addr, ain->ai_addrlen);
        ctx->me.addrlen = ain->ai_addrlen;

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
mrkrpc_ctx_register_msg(mrkrpc_ctx_t *ctx,
                       uint8_t op,
                       mrkdata_spec_t *reqspec,
                       mrkrpc_recv_handler_t reqhandler,
                       mrkdata_spec_t *respspec,
                       mrkrpc_recv_handler_t resphandler)
{
    mrkrpc_msg_entry_t *msge;

#ifndef __GNUC__
    assert(op < MRKRPC_MAX_MSGS);
#endif

    if ((msge = array_get(&ctx->ops, op)) == NULL) {
        FAIL("array_get");
    }
    msge->reqspec = reqspec;
    msge->reqhandler = reqhandler;
    msge->respspec = respspec;
    msge->resphandler = resphandler;
    return 0;
}

mrkrpc_msg_entry_t *
mrkrpc_ctx_get_msg(mrkrpc_ctx_t *ctx, uint8_t op)
{
    mrkrpc_msg_entry_t *p;

#ifndef __GNUC__
    assert(op < MRKRPC_MAX_MSGS);
#endif

    if ((p = array_get(&ctx->ops, op)) == NULL) {
        return NULL;
    }
    return p;
}

size_t
mrkrpc_ctx_get_pending_volume(mrkrpc_ctx_t *ctx)
{
    return trie_get_volume(&ctx->pending);
}

size_t
mrkrpc_ctx_get_pending_length(mrkrpc_ctx_t *ctx)
{
    return trie_get_nelems(&ctx->pending);
}


size_t
mrkrpc_ctx_compact_pending(mrkrpc_ctx_t *ctx, size_t threshold)
{
    size_t length;

    length = trie_get_volume(&ctx->pending);
    if (length > threshold) {
        trie_cleanup(&ctx->pending);
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

    /*
     * High 32 bits of sid will be random. Low 32 bits will be
     * incrementing.
     */
    srandomdev();

    sidbase = (((uint64_t)random()) << 32);

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
    mrkrpc_ctx_init(&ctx);
    n = mrkrpc_make_node_from_params(&ctx, 0, NULL, 0);
    tmp = make_queue_entry(n, n, 0, 0);
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

    mrkdata_fini();

    mflags &= ~MRKRPC_MFLAG_INITIALIZED;
}

