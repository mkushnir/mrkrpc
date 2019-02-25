CLEANFILES = *.core
#CLEANFILES += *.in

noinst_PROGRAMS=testfoo testtimeout

distdir = ../../$(PACKAGE)-$(VERSION)/src/test
dist_HEADERS = unittest.h ../mrkrpc.h

BUILT_SOURCES = ../diag.c ../diag.h

noinst_HEADERS =

if DEBUG
DEBUG_FLAGS = -g -O0 @CLANG_DEBUG@
else
DEBUG_FLAGS = -DNDEBUG -O3
endif

nodist_testfoo_SOURCES = ../diag.c
testfoo_SOURCES = testfoo.c
testfoo_CFLAGS = $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I.. -I$(includedir)
testfoo_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkdata -lmrkthr -lmrkrpc

nodist_testtimeout_SOURCES = ../diag.c
testtimeout_SOURCES = testtimeout.c
testtimeout_CFLAGS = $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I.. -I$(includedir)
testtimeout_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkdata -lmrkthr -lmrkrpc

../diag.c ../diag.h: ../diag.txt
	$(AM_V_GEN) cat ../diag.txt | sort -u | /bin/sh ../gen-diag mrkrpc ..

testrun: all
	for i in $(noinst_PROGRAMS); do if test -x ./$$i; then LD_LIBRARY_PATH=$(libdir) ./$$i; fi; done;
