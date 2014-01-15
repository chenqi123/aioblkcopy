dnl Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(aioblkcopy, 1.0.0)
AC_CONFIG_HEADERS([config.h])

AC_GNU_SOURCE 

AC_CANONICAL_SYSTEM
AM_INIT_AUTOMAKE()

AC_PROG_CC

AC_CHECK_HEADERS([fcntl.h stdlib.h string.h sys/param.h unistd.h sys/time.h])

AC_CHECK_LIB([rt], [aio_read])

AC_TYPE_SIZE_T

AC_FUNC_MALLOC
AC_CHECK_FUNCS([memset])
AC_CHECK_FUNCS([select])

AC_CONFIG_FILES(Makefile src/Makefile)
AC_OUTPUT




