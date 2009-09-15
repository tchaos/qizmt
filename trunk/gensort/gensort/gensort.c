/* gensort.c - Generator program for sort benchmarks.
 *
 * Version 1.2  8 April 2009  Chris Nyberg <chris.nyberg@ordinal.com>
 */

/* This software is provided 'as-is', without any express or implied
 * warranty.  In no event will the author be held liable for any damages
 * arising from the use of this software.
 */

#include <stdio.h>
#include <stdlib.h>
#include "rand16.h"
//#define PRINT_CRC32 1
#ifdef PRINT_CRC32
# include <zlib.h>   /* use crc32() function in zlib */
#endif

#define REC_SIZE 100
#define HEX_DIGIT(x) ((x) >= 10 ? 'A' + (x) - 10 : '0' + (x))


/* gen_rec = generate a "binary" record suitable for all sort
 *              benchmarks *except* PennySort.
 */
void gen_rec(unsigned char *rec_buf, u16 rand, u16 rec_number)
{
    int i;
    
    /* generate the 10-byte key using the high 10 bytes of the 128-bit
     * random number
     */
    rec_buf[0] = (rand.hi8 >> 56) & 0xFF;
    rec_buf[1] = (rand.hi8 >> 48) & 0xFF;
    rec_buf[2] = (rand.hi8 >> 40) & 0xFF;
    rec_buf[3] = (rand.hi8 >> 32) & 0xFF;
    rec_buf[4] = (rand.hi8 >> 24) & 0xFF;
    rec_buf[5] = (rand.hi8 >> 16) & 0xFF;
    rec_buf[6] = (rand.hi8 >>  8) & 0xFF;
    rec_buf[7] = (rand.hi8 >>  0) & 0xFF;
    rec_buf[8] = (rand.lo8 >> 56) & 0xFF;
    rec_buf[9] = (rand.lo8 >> 48) & 0xFF;

    /* add 2 bytes of "break" */
    rec_buf[10] = 0x00;
    rec_buf[11] = 0x11;
    
    /* convert the 128-bit record number to 32 bits of ascii hexadecimal
     * as the next 32 bytes of the record.
     */
    for (i = 0; i < 16; i++)
        rec_buf[12 + i] = HEX_DIGIT((rec_number.hi8 >> (60 - 4 * i)) & 0xF);
    for (i = 0; i < 16; i++)
        rec_buf[28 + i] = HEX_DIGIT((rec_number.lo8 >> (60 - 4 * i)) & 0xF);

    /* add 4 bytes of "break" data */
    rec_buf[44] = 0x88;
    rec_buf[45] = 0x99;
    rec_buf[46] = 0xAA;
    rec_buf[47] = 0xBB;

    /* add 48 bytes of filler based on low 48 bits of random number */
    rec_buf[48] = rec_buf[49] = rec_buf[50] = rec_buf[51] = 
        HEX_DIGIT((rand.lo8 >> 44) & 0xF);
    rec_buf[52] = rec_buf[53] = rec_buf[54] = rec_buf[55] = 
        HEX_DIGIT((rand.lo8 >> 40) & 0xF);
    rec_buf[56] = rec_buf[57] = rec_buf[58] = rec_buf[59] = 
        HEX_DIGIT((rand.lo8 >> 36) & 0xF);
    rec_buf[60] = rec_buf[61] = rec_buf[62] = rec_buf[63] = 
        HEX_DIGIT((rand.lo8 >> 32) & 0xF);
    rec_buf[64] = rec_buf[65] = rec_buf[66] = rec_buf[67] = 
        HEX_DIGIT((rand.lo8 >> 28) & 0xF);
    rec_buf[68] = rec_buf[69] = rec_buf[70] = rec_buf[71] = 
        HEX_DIGIT((rand.lo8 >> 24) & 0xF);
    rec_buf[72] = rec_buf[73] = rec_buf[74] = rec_buf[75] = 
        HEX_DIGIT((rand.lo8 >> 20) & 0xF);
    rec_buf[76] = rec_buf[77] = rec_buf[78] = rec_buf[79] = 
        HEX_DIGIT((rand.lo8 >> 16) & 0xF);
    rec_buf[80] = rec_buf[81] = rec_buf[82] = rec_buf[83] = 
        HEX_DIGIT((rand.lo8 >> 12) & 0xF);
    rec_buf[84] = rec_buf[85] = rec_buf[86] = rec_buf[87] = 
        HEX_DIGIT((rand.lo8 >>  8) & 0xF);
    rec_buf[88] = rec_buf[89] = rec_buf[90] = rec_buf[91] = 
        HEX_DIGIT((rand.lo8 >>  4) & 0xF);
    rec_buf[92] = rec_buf[93] = rec_buf[94] = rec_buf[95] = 
        HEX_DIGIT((rand.lo8 >>  0) & 0xF);

    /* add 4 bytes of "break" data */
    rec_buf[96] = 0xCC;
    rec_buf[97] = 0xDD;
    rec_buf[98] = 0xEE;
    rec_buf[99] = 0xFF;
}


/* gen_ascii_rec = generate an ascii record suitable for all sort
 *              benchmarks including PennySort.
 */
void gen_ascii_rec(unsigned char *rec_buf, u16 rand, u16 rec_number)
{
    int         i;
    u8          temp;
    
    /* generate the 10-byte ascii key using mostly the high 64 bits.
     */
    temp = rand.hi8;
    rec_buf[0] = ' ' + (temp % 95);
    temp /= 95;
    rec_buf[1] = ' ' + (temp % 95);
    temp /= 95;
    rec_buf[2] = ' ' + (temp % 95);
    temp /= 95;
    rec_buf[3] = ' ' + (temp % 95);
    temp /= 95;
    rec_buf[4] = ' ' + (temp % 95);
    temp /= 95;
    rec_buf[5] = ' ' + (temp % 95);
    temp /= 95;
    rec_buf[6] = ' ' + (temp % 95);
    temp /= 95;
    rec_buf[7] = ' ' + (temp % 95);
    temp = rand.lo8;
    rec_buf[8] = ' ' + (temp % 95);
    temp /= 95;
    rec_buf[9] = ' ' + (temp % 95);
    temp /= 95;

    /* add 2 bytes of "break" */
    rec_buf[10] = ' ';
    rec_buf[11] = ' ';
    
    /* convert the 128-bit record number to 32 bits of ascii hexadecimal
     * as the next 32 bytes of the record.
     */
    for (i = 0; i < 16; i++)
        rec_buf[12 + i] = HEX_DIGIT((rec_number.hi8 >> (60 - 4 * i)) & 0xF);
    for (i = 0; i < 16; i++)
        rec_buf[28 + i] = HEX_DIGIT((rec_number.lo8 >> (60 - 4 * i)) & 0xF);

    /* add 2 bytes of "break" data */
    rec_buf[44] = ' ';
    rec_buf[45] = ' ';

    /* add 52 bytes of filler based on low 48 bits of random number */
    rec_buf[46] = rec_buf[47] = rec_buf[48] = rec_buf[49] = 
        HEX_DIGIT((rand.lo8 >> 48) & 0xF);
    rec_buf[50] = rec_buf[51] = rec_buf[52] = rec_buf[53] = 
        HEX_DIGIT((rand.lo8 >> 44) & 0xF);
    rec_buf[54] = rec_buf[55] = rec_buf[56] = rec_buf[57] = 
        HEX_DIGIT((rand.lo8 >> 40) & 0xF);
    rec_buf[58] = rec_buf[59] = rec_buf[60] = rec_buf[61] = 
        HEX_DIGIT((rand.lo8 >> 36) & 0xF);
    rec_buf[62] = rec_buf[63] = rec_buf[64] = rec_buf[65] = 
        HEX_DIGIT((rand.lo8 >> 32) & 0xF);
    rec_buf[66] = rec_buf[67] = rec_buf[68] = rec_buf[69] = 
        HEX_DIGIT((rand.lo8 >> 28) & 0xF);
    rec_buf[70] = rec_buf[71] = rec_buf[72] = rec_buf[73] = 
        HEX_DIGIT((rand.lo8 >> 24) & 0xF);
    rec_buf[74] = rec_buf[75] = rec_buf[76] = rec_buf[77] = 
        HEX_DIGIT((rand.lo8 >> 20) & 0xF);
    rec_buf[78] = rec_buf[79] = rec_buf[80] = rec_buf[81] = 
        HEX_DIGIT((rand.lo8 >> 16) & 0xF);
    rec_buf[82] = rec_buf[83] = rec_buf[84] = rec_buf[85] = 
        HEX_DIGIT((rand.lo8 >> 12) & 0xF);
    rec_buf[86] = rec_buf[87] = rec_buf[88] = rec_buf[89] = 
        HEX_DIGIT((rand.lo8 >>  8) & 0xF);
    rec_buf[90] = rec_buf[91] = rec_buf[92] = rec_buf[93] = 
        HEX_DIGIT((rand.lo8 >>  4) & 0xF);
    rec_buf[94] = rec_buf[95] = rec_buf[96] = rec_buf[97] = 
        HEX_DIGIT((rand.lo8 >>  0) & 0xF);

    /* add 2 bytes of "break" data */
    rec_buf[98] = '\r';	/* nice for Windows */
    rec_buf[99] = '\n';
}

static char usage_str[] =
    "usage: gensort [-a] [-c] [-bSTARTING_REC_NUM] NUM_RECS FILE_NAME\n"
    "-a        Generate ascii records required for PennySort or JouleSort.\n"
    "          These records are also an alternative input for the other\n"
    "          sort benchmarks.  Without this flag, binary records will be\n"
    "          generated that contain the highest density of randomness in\n"
    "          the 10-byte key.\n"
    "-c        Calculate the sum of the crc32 checksums of each of the\n"
    "          generated records and send it to standard error.\n"
    "-bN       Set the beginning record generated to N. By default the\n"
    "          first record generated is record 0.\n"
    "NUM_RECS  The number of sequential records to generate.\n"
    "FILE_NAME The name of the file to write the records to.\n"
    "\n"
    "Example 1 - to generate 1000000 ascii records starting at record 0 to\n"
    "the file named \"pennyinput\":\n"
    "    gensort -a 1000000 pennyinput\n"
    "\n"
    "Example 2 - to generate 1000 binary records beginning with record 2000\n"
    "to the file named \"partition2\":\n"
    "    gensort -b2000 1000 partition2\n";

void usage(void)
{
    fprintf(stderr, usage_str);
    exit(1);
}


int main(int argc, char *argv[])
{
    u8                  j;                      /* should be a u16 someday */
    u16                 starting_rec_number;
    u16                 num_recs;
    u16                 rec_number;
    u16                 rand;
    int                 print_checksum = 0;
    unsigned char       rec_buf[REC_SIZE];
    FILE                *out;
    void                (*gen)(unsigned char *buf, u16 rand, u16 number);
    u16                 temp16 = {0LL, 0LL};
    u16                 sum16 = {0LL, 0LL};
    char                sumbuf[U16_ASCII_BUF_SIZE];
    char                prntbuf[U16_ASCII_BUF_SIZE];

    starting_rec_number.hi8 = 0;
    starting_rec_number.lo8 = 0;
    gen = gen_rec;

    while (argc > 1 && argv[1][0] == '-')
    {
        if (argv[1][1] == 'b')
            starting_rec_number = dec_to_u16(argv[1] + 2);
        else if (argv[1][1] == 'c')
#ifdef PRINT_CRC32
            print_checksum = 1;
#else
            fprintf(stderr, "checksum not implementeed\n");
#endif
        else if (argv[1][1] == 'a')
            gen = gen_ascii_rec;
        else
            usage();
        argc--;
        argv++;
    }
    if (argc != 3)
        usage();
    num_recs = dec_to_u16(argv[1]);

    if ((out = fopen(argv[2], "wb")) == NULL)
    {
        perror(argv[2]);
        exit(1);
    }
    
    rand = skip_ahead_rand(starting_rec_number);
    rec_number = starting_rec_number;
    if (starting_rec_number.hi8 | starting_rec_number.lo8)  /* if non-zero */
    {
        u16_to_dec(starting_rec_number, sumbuf);
        fprintf(stderr, "starting rec number: %s\n", sumbuf);
    }
    
    for (j = 0; j < num_recs.lo8; j++)
    {
        rand = next_rand(rand);
        (*gen)(rec_buf, rand, rec_number);
        if (print_checksum)
        {
#ifdef PRINT_CRC32
            temp16.lo8 = crc32(0, rec_buf, REC_SIZE);
#endif
            sum16 = add16(sum16, temp16);
        }
        if(1 != fwrite(rec_buf, REC_SIZE, 1, out))
		{
			fprintf(stderr, "Unable to write record");
		}
        if (++rec_number.lo8 == 0)
            ++rec_number.hi8;
    }
	fclose(out);
    if (print_checksum)
        fprintf(stderr, "%s\n", u16_to_hex(sum16, sumbuf));
    return (0);
}
