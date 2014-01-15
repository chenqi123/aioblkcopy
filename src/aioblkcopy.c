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

//#define AIOBLKCOPY_DEBUG

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <aio.h>
#include <getopt.h>

#include "aioblkcopy.h"

/*
 * The program configuration parameters.
 */

struct globalparams {
    int blksize;       /* working block size -b and --blocksize*/
    int maxqsize;    /* maximum queue size -q and --maxqsize*/
    char *inputfile;   /* input file -i */
    char *outputfile;  /* output file -o */

    #ifdef _GNU_SOURCE

    int wo_di_inp;     /* disable O_DIRECT on input --without-directio-input*/
    int wo_di_out;     /* disable O_DIRECT on output --without-directio-output*/

    #endif

} globalparams;

static const char *optstr = "i:o:b:q:h";

static const struct option optarray[] = {
    { "input-file", required_argument, NULL, 'i' },
    { "output-file", required_argument, NULL, 'o' },
    { "blksize", required_argument, NULL, 'b' },
    { "maxqsize", required_argument, NULL, 'q' },

    #ifdef _GNU_SOURCE

    { "without-directio-input", required_argument, &globalparams.wo_di_inp, 1 },
    { "without-directio-output", required_argument, &globalparams.wo_di_out, 1 },

    #endif

    { "help", no_argument, NULL, 'h' },
    { NULL, no_argument, NULL, 0 }
};

/*
 * The AIO signal handler.
 */

static void aiosighandler( int sig, siginfo_t *si, void *ucontext ) {

	#ifdef AIOBLKCOPY_DEBUG
	write(STDERR_FILENO, "IO_SIGNAL received\n", 20);
	#endif

}

/*
 * Prints program usage.
 */

static void usage( void ) {

	fprintf(stderr,"Usage: aioblkcopy [OPTION]...\n\
Asynchronous block copying tool.\n\n\
Mandatory arguments to long options are mandatory for short options too.\n\
    -h, --help                    display this help and exit\n\
    -i, --input-file=FILENAME     source file\n\
    -o, --output-file=FILENAME    destination file\n\
    -q, --maxqsize=QUEUESIZE      maximum size of working queue\n\
    -b, --blocksize=BLOCKSIZE     size of working data block\n");

	#ifdef _GNU_SOURCE

	fprintf(stderr, "    --without-directio-input      do not use direct io for input file\n\
    --without-directio-output     do not use direct io for output file\n");

	#endif

	fprintf(stderr, "\n\
FILENAME can be any file. Output file created if not existed and truncated if existed without prompt.\n\
If no filenames given standard input and output used instead.\n\
QUEUESIZE must be positive decimal between 1 and 32 and determines maximum number of input and output simultaneous requests.\n\
BLOCKSIZE can be given in bytes, kilobytes(suffixes k or K needed) or megabytes(suffixes m or M needed).\n\
QUEUESIZE by default is %i, BLOCKSIZE by default is %i.\n", DEFAULT_MAXQUEUESIZE, DEFAULT_BLKSIZE);

}

int main( int argc, char *argv[] ) {

	/*
	 * Temporary file descriptors.
	 */
	int ifd = -1;
	int ofd = -1;

	/*
	 * Requests numbering needed for write ordering.
	 */
	long long irqnum = 0;
	long long orqnum = 0;

	size_t ioff = 0;
	size_t ooff = 0;

	/*
	 * Current queue size. Mostly needed for debugging.
	 */

	int iqsize = 0;
	int oqsize = 0;

	/*
	 * Maximum queues size.
	 */
	int imaxqsize;
	int omaxqsize;

	/*
	 * pipes, fifos, character devices can't do lseek(), so queueing on them is useless.
	 */
	int iseekable;
	int oseekable;

	/*
	 * Used for end of data detection.
	 */
	int eof = 0;

	/*
	 * Working queues.
	 */
	struct blkqueitem *ique;
	struct blkqueitem *oque;

	struct sigaction sa;
	struct timeval to;

	int i;
	int j;

	/*
	 * Only needed on initialization.
	 */
	struct stat statdata;
	int fflags;

	/*
	 * Variables for statistics.
	 */
	struct timeval starttime;
	struct timeval endtime;
	double	workingtime;

	/*
	 * Variables for command line parsing.
	 */
	int opt;
	int paramsindex;
	long long tint;
	char *bsuffix;


	/*
	 * Initialize default global parameters.
	 */

	globalparams.blksize = DEFAULT_BLKSIZE;
	globalparams.maxqsize = DEFAULT_MAXQUEUESIZE;
	globalparams.inputfile = NULL;
	globalparams.outputfile = NULL;

	#ifdef _GNU_SOURCE

	globalparams.wo_di_inp = 0;
	globalparams.wo_di_out = 0;

    #endif

	/*
	 * Parse command line.
	 */

	for(;;) {

		opt = getopt_long(argc, argv, optstr, optarray, &paramsindex);

		if (opt == -1) break;

		switch(opt) {

		case 'i':

			globalparams.inputfile = optarg;
			break;

		case 'o':

			globalparams.outputfile = optarg;
			break;

		case 'q':

			tint = strtoul(optarg, &bsuffix, 10);

			if (( tint < 1 ) || ( tint > 32 ) || (bsuffix[0] != '\0' ) ) {

				fprintf(stderr, "Wrong maximum queue size, must be positive decimal between 1 and 32!\n");

				exit(EXIT_USAGE);

			}

			globalparams.maxqsize = tint;

			break;

		case 'b':

			errno = 0;

			tint = strtoul(optarg, &bsuffix, 10);

			if (errno != 0) CUSTOMERROR("strtoul()");

			if (bsuffix[0] != '\0') {

				if (bsuffix[1] == '\0') {

					switch(bsuffix[0]) {

					case 'K':
					case 'k':

						tint = tint * 1024;

						break;

					case 'M':
					case 'm':

						tint = tint * 1024 * 1024;

						break;

					default:

						fprintf(stderr, "Block size suffix must be K for kilobytes or M for megabytes!\n");
						exit(EXIT_USAGE);
						break;

					}

				}
				else {

					fprintf(stderr, "Block size suffix must be K for kilobytes or M for megabytes!\n");
					exit(EXIT_USAGE);

				}

			}

			if ((tint % 512) != 0) {

				fprintf(stderr, "Block size must be multiple of 512!\n");
				exit(EXIT_USAGE);

			}

			if (tint > 1024*1024*16) {

				fprintf(stderr, "Block size too big! Must be less then 16 megabytes.\n");
				exit(EXIT_USAGE);

			}

			globalparams.blksize = tint;

			break;

		case 'h':

			usage();
			exit(EXIT_USAGE);
			break;

		default:

			break;

		}


	}

	imaxqsize = globalparams.maxqsize;
	omaxqsize = globalparams.maxqsize;

	/*
	 * Check if the input file is regular file or block device.
	 * I suppose that only on regular files and block devices are possible to do lseek() and
	 * submit simultaneous read/write requests.
	 * Also for these descriptors there is no point to do many requests simultaneously.
 	 */

	if (globalparams.inputfile != NULL) {

		if (stat(globalparams.inputfile, &statdata) == -1) CUSTOMERROR("stat()");

		if ( !( S_ISREG(statdata.st_mode) || S_ISBLK(statdata.st_mode) ) ) {

			iseekable = 0;

			imaxqsize = 1;

		}
		else {

			iseekable = 1;

		}

	}
	else {

		iseekable = 0;

		imaxqsize = 1;

		/*
		 * if a user doesn't give us a input file name we'll use STDIN.
		 */

		ifd = STDIN_FILENO;

	}

	/*
	 * The same for outputfile.
	 */

	if (globalparams.outputfile != NULL) {

		if (stat(globalparams.outputfile, &statdata) == -1)	if (errno != ENOENT) CUSTOMERROR("stat()");

		if ( !( S_ISREG(statdata.st_mode) || S_ISBLK(statdata.st_mode) ) ) {

			oseekable = 0;

			omaxqsize = 1;

		}
		else {

			oseekable = 1;

		}

	}
	else {

		oseekable = 0;

		omaxqsize = 1;

		/*
		 * if a user doesn't give us a output file name we'll use STDOUT.
		*/

		ofd = STDOUT_FILENO;


	}

	#ifdef AIOBLKCOPY_DEBUG
	printf("inputfile: %s\noutputfile: %s \niseekable: %i : %i imaxqsize: %i omaxqsize: %i maxqsize: %i blksize: %i\n", \
			globalparams.inputfile, globalparams.outputfile, iseekable, oseekable, imaxqsize, omaxqsize, globalparams.maxqsize, globalparams.blksize);
	#endif

	/*
	 * Initialize input and output queues.
	 */

	ique = malloc(sizeof(struct blkqueitem) * imaxqsize);

	if (ique == NULL) CUSTOMERROR("malloc()");

	oque = malloc(sizeof(struct blkqueitem) * omaxqsize);

	if (oque == NULL) CUSTOMERROR("malloc()");

	memset(ique, 0, sizeof(struct blkqueitem) * imaxqsize);
	memset(oque, 0, sizeof(struct blkqueitem) * omaxqsize);

	for(i = 0; i < imaxqsize; i++) {

		ique[i].aiodata = malloc(sizeof(struct aiocb));

		if (ique[i].aiodata == NULL) CUSTOMERROR("malloc");

		memset(ique[i].aiodata, 0, sizeof(struct aiocb));

		ique[i].status = QUEITEM_FREE;

		fflags = O_RDONLY;

		#ifdef _GNU_SOURCE

		if ((globalparams.wo_di_inp == 0) && (iseekable == 1)) {

			fflags = fflags | O_DIRECT;

		}

		#endif

		if ((iseekable == 0) && (ifd != -1)) {

			ique[i].fd = ifd;

		}

		else {

			/*
			 * We can't use fcntl DUP_FD, because we need independent seeking on descriptors.
			 * If someone knows that it's not true let me know.
			 */

			ique[i].fd = open(globalparams.inputfile, fflags);

			if (ique[i].fd == -1) CUSTOMERROR("open()");

		}

	}

	for(i = 0; i < omaxqsize; i++) {

		oque[i].aiodata = malloc(sizeof(struct aiocb));

		if (oque[i].aiodata == NULL) CUSTOMERROR("malloc");

		memset(oque[i].aiodata, 0, sizeof(struct aiocb));

		oque[i].status =  QUEITEM_FREE;

		fflags = O_WRONLY | O_CREAT | O_TRUNC;

		#ifdef _GNU_SOURCE

		if ((globalparams.wo_di_out == 0) && ( oseekable == 1)) {

			fflags = fflags | O_DIRECT ;

		}

		#endif

		if (( oseekable == 0) && (ofd != -1)) {

			oque[i].fd = ofd;

		}

		else {

			oque[i].fd = open(globalparams.outputfile, fflags,  S_IRUSR |  S_IWUSR | S_IRGRP );

			if (oque[i].fd == -1) CUSTOMERROR("open()");

		}

	}

	/*
	 * Installation of signals handlers;
	 */

	if (sigemptyset(&sa.sa_mask) == -1) {

			fprintf(stderr, "Error occurred at file %s line(%d):\n", __FILE__, __LINE__);

			perror("sigemptyset()");

			exit(EXIT_FAILURE);

		}

	sa.sa_flags = SA_RESTART | SA_SIGINFO;
	sa.sa_sigaction = aiosighandler;

	if (sigaction(IO_SIGNAL, &sa, NULL) == -1) {

		fprintf(stderr, "Error occurred at file %s line(%d):\n", __FILE__, __LINE__);

		perror("sigaction()");

		exit(EXIT_FAILURE);

	}

	/*
	 * Used only for statistics.
	 */

	if (gettimeofday(&starttime, NULL) == -1) CUSTOMERROR("gettimeofday()");

	/*
	 * Main loop.
	 */

	for(;;) {

		/*
		 * Check all input queue items and send unused items to AIO working queue.
		 */

		for(i = 0; i < imaxqsize; i++) {

			if (ique[i].status == QUEITEM_READY) continue;

			/*
			 * Check for input operations in progress.
			 */

			if (ique[i].status == QUEITEM_INPROGRESS) {

				ique[i].retcode = aio_error(ique[i].aiodata);

				switch(ique[i].retcode) {

				case 0:

					ique[i].retcode = aio_return(ique[i].aiodata);

					if (ique[i].retcode == 0) {

						eof = 1;

						#ifdef AIOBLKCOPY_DEBUG
						fprintf(stderr, "READ EOF rqnum: %lld fd: %i offset: %lld bytes: %i iqsize: %i\n", \
								ique[i].rqnum, ique[i].aiodata->aio_fildes, ique[i].aiodata->aio_offset, \
								ique[i].retcode, iqsize);
						#endif

						if (ique[i].readyb != 0) {

							ique[i].status = QUEITEM_READY;
							continue;

						}

						break;

					}

					ique[i].readyb += ique[i].retcode;

					/*
					 * If we haven't got full block we'll try again and again and again...
					 */

					if (ique[i].readyb != globalparams.blksize) {

						ique[i].aiodata->aio_fildes = ique[i].fd;

						ique[i].aiodata->aio_reqprio = 0;

						ique[i].aiodata->aio_buf = ique[i].buffer + ique[i].readyb;

						if (iseekable == 1)	{

							ique[i].aiodata->aio_offset = ique[i].fdoffset + ique[i].readyb;

						}
						else {

							ique[i].aiodata->aio_offset = 0;

						}

						ique[i].aiodata->aio_nbytes = globalparams.blksize - ique[i].readyb;

						ique[i].aiodata->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
						ique[i].aiodata->aio_sigevent.sigev_signo = IO_SIGNAL;
						ique[i].aiodata->aio_sigevent.sigev_value.sival_ptr = &ique[i];

						if (aio_read(ique[i].aiodata) == -1) CUSTOMERROR("aio_read()");

						#ifdef AIOBLKCOPY_DEBUG
						fprintf(stderr, "READ INPROGRESS rqnum: %lld fd: %i offset: %lld bytes: %i iqsize: %i\n", \
							ique[i].rqnum, ique[i].aiodata->aio_fildes, ique[i].aiodata->aio_offset, \
							ique[i].aiodata->aio_nbytes, iqsize);
						#endif

						continue;

					}

					ique[i].status = QUEITEM_READY;

					#ifdef AIOBLKCOPY_DEBUG
					fprintf(stderr, "READ COMPLETED rqnum: %lld fd: %i offset: %lld bytes: %i iqsize: %i\n", \
							ique[i].rqnum, ique[i].aiodata->aio_fildes, ique[i].aiodata->aio_offset, \
							ique[i].readyb, iqsize);
					#endif

					continue;

				case EINPROGRESS:

					continue;

				case ECANCELED:

					ique[i].retcode = aio_return(ique[i].aiodata);

					#ifdef AIOBLKCOPY_DEBUG
					fprintf(stderr, "READ CANCELED rqnum: %lld fd: %i offset:  %lld bytes: %i iqsize: %i\n", \
						ique[i].rqnum, ique[i].aiodata->aio_fildes, ique[i].aiodata->aio_offset, \
						ique[i].retcode, iqsize);
					#endif

					break;

				default:

					CUSTOMERROR("aio_error()");

					break;

				}

				ique[i].status = QUEITEM_FREE;

				free((void *)(ique[i].buffer));

				ique[i].buffer = NULL;

				iqsize-- ;

			}
			/*
			 * Prepare and send unused queitems to AIO working queue.
			 */
			else {

				if (eof == 1) continue;

				ique[i].retcode = posix_memalign((void **)&(ique[i].buffer), 512, globalparams.blksize);

				if (ique[i].retcode != 0) {

					fprintf(stderr, "Error occurred at file %s line(%d):\n", __FILE__, __LINE__);

					fprintf(stderr, "posix_memalign(): %s", strerror(ique[i].retcode));

					exit(EXIT_FAILURE);

				}

				ique[i].rqnum = ++irqnum;
				ique[i].status = QUEITEM_INPROGRESS;
				ique[i].retcode = EINPROGRESS;
				ique[i].readyb = 0;

				if (iseekable == 1) {

					ique[i].fdoffset = ioff;

				}
				else {

					ique[i].fdoffset = 0;

				}

				ique[i].aiodata->aio_fildes = ique[i].fd;
				ique[i].aiodata->aio_reqprio = 0;
				ique[i].aiodata->aio_buf = ique[i].buffer;
				ique[i].aiodata->aio_offset = ique[i].fdoffset;
				ique[i].aiodata->aio_nbytes = globalparams.blksize;
				ique[i].aiodata->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
				ique[i].aiodata->aio_sigevent.sigev_signo = IO_SIGNAL;
				ique[i].aiodata->aio_sigevent.sigev_value.sival_ptr = &ique[i];

				if (aio_read(ique[i].aiodata) == -1) CUSTOMERROR("aio_read()");

				ioff += globalparams.blksize;

				iqsize++ ;

				#ifdef AIOBLKCOPY_DEBUG
				fprintf(stderr, "READ QUEUED rqnum: %lld fd: %i offset: %lld bytes: %i iqsize: %i\n", \
						ique[i].rqnum, ique[i].aiodata->aio_fildes, ique[i].aiodata->aio_offset, \
						ique[i].aiodata->aio_nbytes, iqsize);
				#endif

			}

		}

		/*
		 * Check all output queue items for write completion.
		 * Check all input queue items for read completion then put completed to AIO working queue for writing.
		 */

		j = 0;

		for(i = 0; i < omaxqsize; i++) {

			if (oque[i].status == QUEITEM_INPROGRESS) {

				oque[i].retcode = aio_error(oque[i].aiodata);

				switch(oque[i].retcode) {

					case 0:

						oque[i].retcode = aio_return(oque[i].aiodata);

						#ifdef AIOBLKCOPY_DEBUG
						fprintf(stderr, "WRITE COMPLETED orqnum: %lld fd : %i offset: %li bytes: %i oqsize: %i\n", \
								oque[i].rqnum, oque[i].aiodata->aio_fildes, oque[i].aiodata->aio_offset, \
								oque[i].retcode, oqsize-1);
						#endif

						if (oque[i].retcode == 0) eof = 1;

						break;

					case EINPROGRESS:

						continue;

					case ECANCELED:

						#ifdef AIOBLKCOPY_DEBUG
						fprintf(stderr, "WRITE CANCELED orqnum: %lld fd : %i offset: %li bytes: %i oqsize: %i\n", \
								oque[i].rqnum, oque[i].aiodata->aio_fildes, oque[i].aiodata->aio_offset, \
								oque[i].aiodata->aio_nbytes, oqsize-1);
						#endif

						break;

					case EFBIG:

						/*
						 * It's possible that output device smaller than input data size.
						 */
						eof = 1;
						#ifdef AIOBLKCOPY_DEBUG
						fprintf(stderr, "WRITE EOF orqnum: %lld fd : %i offset: %li bytes: %i oqsize: %i\n", \
							oque[i].rqnum, oque[i].aiodata->aio_fildes, oque[i].aiodata->aio_offset, \
							oque[i].aiodata->aio_nbytes, oqsize-1);
						#endif
						break;

					default:

						CUSTOMERROR("aio_error()");
						break;

				}

				oque[i].status = QUEITEM_FREE;

				free((void *)(oque[i].buffer));

				oque[i].buffer = NULL;

				oqsize-- ;


			}

			else {
				/*
				 * Check input queue for completed data and send it to output.
				 */
				while(j < imaxqsize) {

					switch(ique[j].status) {

					case QUEITEM_FREE:
					case QUEITEM_INPROGRESS:

						break;

					case QUEITEM_READY:

						/*
						 * If output isn't seekable we must wait for the next completed request one by one.
						 */
						if (( oseekable == 0 ) && ( ique[j].rqnum != (orqnum+1) ) ) break;

						oque[i].status = QUEITEM_INPROGRESS;

						oque[i].rqnum = ++orqnum;

						oque[i].buffer = ique[j].buffer;

						if (iseekable == 0) oque[i].fdoffset = ooff;
						else oque[i].fdoffset = ique[j].fdoffset;

						oque[i].aiodata->aio_nbytes = ique[j].readyb;

						oque[i].aiodata->aio_fildes = oque[i].fd;
						oque[i].aiodata->aio_buf = oque[i].buffer;
						oque[i].aiodata->aio_offset = oque[i].fdoffset;
						oque[i].aiodata->aio_reqprio = 0;

						oque[i].aiodata->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
						oque[i].aiodata->aio_sigevent.sigev_signo = IO_SIGNAL;
						oque[i].aiodata->aio_sigevent.sigev_value.sival_ptr = &oque[i];

						if (aio_write(oque[i].aiodata) == -1) CUSTOMERROR("aio_write");

						oqsize++ ;
						ooff += ique[j].readyb;

						#ifdef AIOBLKCOPY_DEBUG
						fprintf(stderr, "WRITE QUEUED orqnum: %lld fd : %i offset: %li bytes: %i oqsize: %i\n", \
							oque[i].rqnum, oque[i].aiodata->aio_fildes, oque[i].aiodata->aio_offset, \
							oque[i].aiodata->aio_nbytes, oqsize);
						#endif

						ique[j].aiodata->aio_buf = NULL;

						ique[j].status = QUEITEM_FREE;

						iqsize-- ;

						break;

					default:

						CUSTOMERROR("aio_error()");
						break;

					}

					j++;

					/*
					 * Go to the next free output request.
					 */
					if (oque[i].status ==  QUEITEM_INPROGRESS) break;

				}

			}


		}

		/*
		 * if we complete all requests and end of data detected we can break main loop.
		 */

		if ((iqsize == 0) && (oqsize == 0) && (eof == 1)) break;

		/*
		 *  Wait for signals.
		 *  Timer delay calculated with help of russian PPP method.
		 */

		to.tv_sec = 0;
		to.tv_usec = 100;

		#ifdef AIOBLKCOPY_DEBUG

		to.tv_sec = 1;
		to.tv_usec = 0;

		fprintf(stderr, "select(): tv_sec: %i tv_usec:%i \n", to.tv_sec, to.tv_usec);

		#endif

		select(0, NULL,NULL,NULL, &to);

		#ifdef AIOBLKCOPY_DEBUG
		fprintf(stderr, "iqsize: %i oqsize:%i eof: %i \n", iqsize , oqsize,  eof);
		#endif

	}

	/*
	 * Write some statistics in dd-like format.
	 */
	if (gettimeofday(&endtime, NULL) == -1) CUSTOMERROR("gettimeofday()");

	workingtime =  (double) (endtime.tv_sec * 1000000 + endtime.tv_usec - starttime.tv_sec * 1000000 - starttime.tv_usec) / 1000000;

	fprintf(stderr, "%lld bytes copied, %.2f s, %.2f MB/s\n", ooff , workingtime, ooff / workingtime / 1024 / 1024);

	for(i = 0; i < imaxqsize; i++) {

		close(ique[i].fd);
		free(ique[i].aiodata);

	}

	free(ique);

	for(i = 0; i < omaxqsize; i++) {

		close(oque[i].fd);
		free(oque[i].aiodata);

	}

	free(oque);

	return EXIT_SUCCESS;

}
