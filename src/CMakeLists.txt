include(CheckSymbolExists)
include(CheckIncludeFile)

if (NOT DEFINED _GNU_SOURCE)
  CHECK_SYMBOL_EXISTS(__GNU_LIBRARY__ "features.h" _GNU_SOURCE)

  if (NOT _GNU_SOURCE)
    unset(_GNU_SOURCE CACHE)
    CHECK_SYMBOL_EXISTS(_GNU_SOURCE "features.h" _GNU_SOURCE)
  endif()

endif()

if (_GNU_SOURCE)
  add_definitions(-D_GNU_SOURCE=1)
endif()

CHECK_INCLUDE_FILE(stdio.h __HAVE_STDIO_H)
CHECK_INCLUDE_FILE(stdlib.h __HAVE_STDLIB_H)
CHECK_INCLUDE_FILE(errno.h __HAVE_ERRNO_H)
CHECK_INCLUDE_FILE(string.h __HAVE_STRING_H)
CHECK_INCLUDE_FILE(sys/param.h __HAVE_SYS_PARAM_H)
CHECK_INCLUDE_FILE(sys/types.h __HAVE_SYS_TYPES_H)
CHECK_INCLUDE_FILE(sys/stat.h __HAVE_SYS_STAT_H)
CHECK_INCLUDE_FILE(sys/time.h __HAVE_SYS_TIME_H)
CHECK_INCLUDE_FILE(signal.h __HAVE_SIGNAL_H)
CHECK_INCLUDE_FILE(unistd.h __HAVE_UNISTD_H)
CHECK_INCLUDE_FILE(fcntl.h __HAVE_FCNTL_H)
CHECK_INCLUDE_FILE(aio.h __HAVE_AIO_H)
CHECK_INCLUDE_FILE(getopt.h __HAVE_GETOPT_H)

add_executable (aioblkcopy aioblkcopy.c)

find_library(LIB_RT rt)

target_link_libraries(aioblkcopy ${LIB_RT})

