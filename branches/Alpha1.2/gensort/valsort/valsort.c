/* valsort.c - Sort output data validator
 *
 * Version 1.0  8 Apr 2009  Chris Nyberg <chris.nyberg@ordinal.com>
 */

/* This software is provided 'as-is', without any express or implied
 * warranty.  In no event will the author be held liable for any damages
 * arising from the use of this software.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
//#define PRINT_CRC32 1
#ifdef PRINT_CRC32
#include <zlib.h>   /* use crc32() function in zlib */
#endif
#include "rand16.h"

#if WIN32
#define strcasecmp _stricmp
#define bcopy(b1,b2,len) (memmove((b2), (b1), (len)), (void) 0)
#endif

#define REC_SIZE 100

static char usage_str[] =
    "usage: valsort [-i] FILE_NAME\n"
    "-i        Use case insensitive ascii comparisons (optional).\n"
    "FILE_NAME The name of the sort output file to verify.\n";

void usage(void)
{
    fprintf(stderr, usage_str);
    exit(1);
}


int main(int argc, char *argv[])
{
    int                 diff;
    u16                 num_recs = {0LL, 0LL};
    u16                 num_dups = {0LL, 0LL};
    u16                 num_unordereds = {0LL, 0LL};
    u16                 one = {0LL, 1LL};
    unsigned char       rec_buf[REC_SIZE];
    unsigned char       prev[REC_SIZE];
    FILE                *in;
    int                 (*compare)(const char *a, const char *b, size_t n);
    u16                 temp16 = {0LL, 0LL};
    u16                 sum16 = {0LL, 0LL};
    char                sumbuf[U16_ASCII_BUF_SIZE];
    char                prntbuf[U16_ASCII_BUF_SIZE];

    compare = (int (*)(const char *a, const char *b, size_t n))memcmp;
    
    while (argc > 1 && argv[1][0] == '-')
    {
        if (argv[1][1] == 'i')
            compare =
                (int (*)(const char *a, const char *b, size_t n))strcasecmp;
        else
            usage();
        argc--;
        argv++;
    }
    if (argc != 2)
        usage();

    if ((in = fopen(argv[1], "rb")) == NULL)
    {
        perror(argv[2]);
        exit(1);
    }
    
    while (fread(rec_buf, REC_SIZE, 1, in))
    {
#ifdef PRINT_CRC32
        temp16.lo8 = crc32(0, rec_buf, REC_SIZE);
#endif
        sum16 = add16(sum16, temp16);

        if (!(num_recs.hi8 == 0 && num_recs.lo8 == 0))
        {
            diff = (*compare)(prev, rec_buf, 10);
            if (diff == 0)
                num_dups = add16(num_dups, one);
            else if (diff > 0)
            {
                if (num_unordereds.hi8 == 0 && num_unordereds.lo8 ==0)
                    fprintf(stderr, "First unordered record is record %s\n",
                            u16_to_dec(num_recs, sumbuf));
                num_unordereds = add16(num_unordereds, one);
            }
        }

        num_recs = add16(num_recs, one);
        bcopy(rec_buf, prev, REC_SIZE);
    }
    
    fprintf(stdout, "Records: %s\n", u16_to_dec(num_recs, sumbuf));
    fprintf(stdout, "Checksum: %s\n", u16_to_hex(sum16, sumbuf));
    if (num_unordereds.hi8 | num_unordereds.lo8)
    {
        fprintf(stderr, "ERROR - there are %s unordered records\n",
                u16_to_dec(num_unordereds, sumbuf));
        return (1);
    }
    else
    {
        fprintf(stdout, "Duplicate keys: %s\n", u16_to_dec(num_dups, sumbuf));
        fprintf(stdout, "SUCCESS - all records are in order\n");
        return (0);
    }
}
