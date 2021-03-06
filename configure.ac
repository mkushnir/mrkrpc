AC_INIT(mrkrpc, 0.1, markiyan.kushnir@gmail.com)
if test -n "$USE_GCC"
then
    CC="cc"
    CXX="c++"
    CPP="cpp"
else
    if which clang >/dev/null
    then
        CC="clang -Qunused-arguments -fcolor-diagnostics"
        CXX="clang++ -Qunused-arguments -fcolor-diagnostics"
        CPP="clang-cpp"
        clang_version=`clang --version | grep version | awk '{print $4;}'`
        clang_version_mjr=`echo $clang_version | awk -F. '{print $1;}'`
        clang_version_mnr=`echo $clang_version | awk -F. '{print $2;}'`
        if test "$clang_version_mjr" = "3" -a "$clang_version_mnr" -lt 3
        then
            AC_SUBST(CLANG_DEBUG, ['-ftrapv -fcatch-undefined-behavior'])
        else
            #AC_SUBST(CLANG_DEBUG, ['-fsanitize=undefined-trap -fsanitize-undefined-trap-on-error'])
        fi
    else
        CC="cc"
        CXX="c++"
        CPP="cpp"
    fi
fi

AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([.ac-aux])
AC_CONFIG_FILES([Makefile src/Makefile src/test/Makefile man/Makefile])
AC_CONFIG_HEADERS(config.h)

#AC_LANG(C)

AC_PROG_CC
AC_PROG_CXX
AM_PROG_AR
AC_PROG_INSTALL

LT_INIT
AM_INIT_AUTOMAKE([foreign subdir-objects])
AM_SILENT_RULES([yes])

dnl AM_MAINTAINER_MODE

AC_CHECK_HEADERS([syslog.h])
AC_CHECK_HEADERS([netdb.h])
AC_CHECK_HEADERS([netinet/in.h])
AC_CHECK_HEADERS([sys/socket.h])
AC_CHECK_HEADERS([limits.h])
AC_CHECK_HEADERS([sys/limits.h])

AC_C_CONST
AC_C_INLINE
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([memset])
AC_CHECK_FUNCS([socket])
AC_CHECK_FUNCS([strdup])

AC_CHECK_TYPE([struct sf_hdtr],
                [AC_MSG_NOTICE([struct sf_hdtr is defined])
                 AC_DEFINE([HAVE_SF_HDTR], [1], [Define to 1 if struct sf_hdtr is defined])
                ],
                [ ], [[
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
]])

AC_PRESERVE_HELP_ORDER

AC_ARG_ENABLE(debug,
              AC_HELP_STRING([--enable-debug],
                             [Enable debug build (default=no)]),
              [AM_CONDITIONAL([DEBUG], [enable_debug=yes])],
              [AM_CONDITIONAL([DEBUG], [test "$enable_debug" = "yes"])])

# pass LDFLAGS=-L${LOCALBASE}/lib in the environment
AC_CHECK_LIB(mrkcommon, _fini, [], [AC_MSG_FAILURE(libmrkcommon.so is required. Please find it at git@github.com:mkushnir/mrkcommon.git)]) 
AC_CHECK_LIB(mrkdata, _fini, [], [AC_MSG_FAILURE(libmrkdata.so is required. Please find it at git@github.com:mkushnir/mrkdata.git)]) 
AC_CHECK_LIB(mrkthr, _fini, [], [AC_MSG_FAILURE(libmrkthr.so is required. Please find it at git@github.com:mkushnir/mrkthr.git)]) 

AC_OUTPUT
