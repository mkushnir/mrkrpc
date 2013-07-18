#include <assert.h>
#include <stdlib.h>
#include <time.h>

#include "unittest.h"
#include "mrkcommon/dumpm.h"
#include "mrkcommon/util.h"
#include "mrkcommon/memdebug.h"
MEMDEBUG_DECLARE(testfoo);

#include <mrkdata.h>
#include <mrkthr.h>
#include <mrkrpc.h>

#ifndef NDEBUG
const char *_malloc_options = "AJ";
#endif

static mrkthr_ctx_t *listener_ctx;

UNUSED static void
test0(void)
{
    struct {
        long rnd;
        int in;
        int expected;
    } data[] = {
        {0, 0, 0},
    };
    UNITTEST_PROLOG_RAND;

    FOREACHDATA {
        TRACE("in=%d expected=%d", CDATA.in, CDATA.expected);
        assert(CDATA.in == CDATA.expected);
    }
}

static void
my_handler(UNUSED mrkrpc_ctx_t *ctx,
           UNUSED mrkrpc_queue_entry_t *qe)
{
    //D8(qe->peer->addr, qe->peer->addrlen);
    //TRACE("op=%02u nid=%016lx sid=%016lx", qe->op, qe->nid, qe->sid);
    //mrkdata_datum_dump(qe->recvdat);
    qe->senddat = qe->recvdat;
    qe->recvdat = NULL;
}

static int
listener(UNUSED int argc, UNUSED void *argv[])
{
    mrkrpc_ctx_t ctx;
    mrkrpc_op_entry_t op;

    op.reqspec = mrkdata_make_spec(MRKDATA_UINT64);
    op.reqhandler = my_handler;
    op.respspec = mrkdata_make_spec(MRKDATA_UINT64);
    op.resphandler = NULL;

    if (mrkrpc_ctx_init(&ctx) != 0) {
        assert(0);
    }

    mrkrpc_ctx_register_op(&ctx, 123, &op);

    if (mrkrpc_ctx_set_me(&ctx, 0x1235, "localhost", 0x1235) != 0) {
        assert(0);
    }

    if (mrkrpc_run(&ctx) != 0) {
        assert(0);
    }

    mrkrpc_serve(&ctx);

    if (mrkrpc_ctx_fini(&ctx) != 0) {
        assert(0);
    }

    return 0;
}

static void
test1(void)
{
    if ((listener_ctx = mrkthr_new("listener", listener, 0)) == NULL) {
        assert(0);
    }
    mrkthr_run(listener_ctx);
    //mrkthr_sleep(500);
}

static void
test2(void)
{
    int i;
    mrkrpc_ctx_t ctx;
    mrkdata_spec_t *spec;
    mrkdata_datum_t *dat;
    mrkdata_datum_t *rv = NULL;
    mrkrpc_node_t *rcpt;

    mrkrpc_op_entry_t op;

    op.reqspec = mrkdata_make_spec(MRKDATA_UINT64);
    op.reqhandler = my_handler;
    op.respspec = mrkdata_make_spec(MRKDATA_UINT64);
    op.resphandler = NULL;

    if (mrkrpc_ctx_init(&ctx) != 0) {
        assert(0);
    }

    mrkrpc_ctx_register_op(&ctx, 123, &op);

    if (mrkrpc_ctx_set_me(&ctx, 0x1234, "localhost", 0x1234) != 0) {
        assert(0);
    }

    if (mrkrpc_run(&ctx) != 0) {
        assert(0);
    }

    spec = mrkdata_make_spec(MRKDATA_UINT64);
    dat = mrkdata_datum_from_spec(spec, (void *)0x12345678, 0);

    rcpt = mrkrpc_make_node(0x1235, "localhost", 0x1235);

    for (i = 0; i < 3; ++i) {
        int res;

        dat->value.u64++;
        res = mrkrpc_call(&ctx, rcpt, 123, dat, &rv);
        CTRACE("Received (res=%d):", res);
        if (rv != NULL) {
            mrkdata_datum_dump(rv);
        }
        mrkdata_datum_destroy(&rv);
    }

    mrkdata_datum_destroy(&dat);
    mrkrpc_node_destroy(&rcpt);

    if (mrkrpc_ctx_fini(&ctx) != 0) {
        assert(0);
    }
    CTRACE("Call finished");
}

int main1(UNUSED int argc, UNUSED void *argv[])
{
    test1();
    //mrkthr_sleep(1000);
    test2();
    //mrkthr_sleep(1000);
    CTRACE("Interrupting listener ...");
    mrkthr_set_interrupt(listener_ctx);
    return 0;
}

int
main(void)
{
    int res;
    mrkthr_ctx_t *m;

    MEMDEBUG_REGISTER(testfoo);

    mrkthr_init();
    mrkrpc_init();

    m = mrkthr_new("main1", main1, 0);

    mrkthr_run(m);

    res = mrkthr_loop();

    mrkrpc_fini();
    mrkthr_fini();

    memdebug_print_stats();

    return res;
}
