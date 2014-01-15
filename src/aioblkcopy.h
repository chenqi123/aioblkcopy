/*
 ============================================================================
 Name        : aioblkcopy.c
 Author      : Nikita Staroverov
 Version     : 1.0.0
 Copyright   : GPLv2
 Description : Asynchronous block copying tool
 ============================================================================
 */

/*
Copyright (C) 2014  Nikita Staroverov

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
*/

#define IO_SIGNAL SIGUSR1

#define EXIT_USAGE 1

#define DEFAULT_BLKSIZE 1048576
#define DEFAULT_MAXQUEUESIZE 8

#define QUEITEM_FREE 0
#define QUEITEM_READY 1
#define QUEITEM_INPROGRESS 2

#define CUSTOMERROR(errfunc) { \
fprintf(stderr, "Error occurred at file %s line(%d):\n", __FILE__, __LINE__); \
\
perror(errfunc); \
\
exit(EXIT_FAILURE); \
}


#include <aio.h>

struct blkqueitem {

	long long rqnum;

	int fd;

	int status;

	int retcode;

	char *buffer;

	size_t fdoffset;

	size_t readyb;

	struct aiocb *aiodata;

};

