/**************************************************************************************
 *  MySpace’s Mapreduce Framework is a mapreduce framework for distributed computing  *
 *  and developing distributed computing applications on large clusters of servers.   *
 *                                                                                    *
 *  Copyright (C) 2008  MySpace Inc. <http://qizmt.myspace.com/>                      *
 *                                                                                    *
 *  This program is free software: you can redistribute it and/or modify              *
 *  it under the terms of the GNU General Public License as published by              *
 *  the Free Software Foundation, either version 3 of the License, or                 *
 *  (at your option) any later version.                                               *
 *                                                                                    *
 *  This program is distributed in the hope that it will be useful,                   *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of                    *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                     *
 *  GNU General Public License for more details.                                      *
 *                                                                                    *
 *  You should have received a copy of the GNU General Public License                 *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.             *
***************************************************************************************/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects
{
    public struct UTFConverter
    {
        private static int UNI_SUR_HIGH_START = 0xD800;
        private static int UNI_SUR_HIGH_END = 0xDBFF;
        private static int UNI_SUR_LOW_START = 0xDC00;
        private static int UNI_SUR_LOW_END = 0xDFFF;
        private static int UNI_REPLACEMENT_CHAR = 0x0000FFFD;
        private static int UNI_MAX_BMP = 0x0000FFFF;
        private static int UNI_MAX_UTF16 = 0x0010FFFF;
        private static int halfMask = 0x3FF;
        private static int halfShift = 10;
        private static int halfBase = 0x0010000;
        private static byte[] bs = new byte[4];
      
        /*
         * Once the bits are split out into bytes of UTF-8, this is a mask OR-ed
         * into the first byte, depending on how many bytes follow.  There are
         * as many entries in this table as there are UTF-8 sequence types.
         * (I.e., one byte sequence, two byte... etc.). Remember that sequencs
         * for *legal* UTF-8 will be 4 or fewer bytes total.
         */
        private static int[] firstByteMark = { 0x00, 0x00, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC };

        public static int ConvertUTF16ToUTF8(IList<byte> bytes, int byteIndex, int byteCount, bool strictConversion)
        {
            int source = byteIndex;
            int sourceEnd = byteIndex + byteCount;
            int stackPos = Stack.CurrentPos;

            while (source < sourceEnd)
            {
                int ch;
                int bytesToWrite = 0;
                int byteMask = 0xBF;
                int byteMark = 0x80;

                ch = GetCodeValueUTF16(bytes[source], bytes[source + 1]);
             
                source += 2;

                /* If we have a surrogate pair, convert to UTF32 first. */
                if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END)
                {
                    /* If the 16 bits following the high surrogate are in the source buffer... */
                    if (source < sourceEnd)
                    {
                        int ch2 = GetCodeValueUTF16(bytes[source], bytes[source + 1]);
                        /* If it's a low surrogate, convert to UTF32. */
                        if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END)
                        {
                            ch =((ch - UNI_SUR_HIGH_START) << halfShift) + (ch2 - UNI_SUR_LOW_START) + halfBase;
                            source+=2;
                        }
                        else if (strictConversion)
                        { /* it's an unpaired high surrogate */
                            //--source; /* return to the illegal value itself */
                            throw new Exception("Unpaired high surrogate.");                           
                        }
                    }
                    else
                    { /* We don't have the 16 bits following the high surrogate. */
                        //--source; /* return to the high surrogate */
                        throw new Exception("We don't have the 16 bits following the high surrogate.");                       
                    }
                }
                else if (strictConversion)
                {
                    /* UTF-16 surrogate values are illegal in UTF-32 */
                    if (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END)
                    {
                        //--source; /* return to the illegal value itself */
                        throw new Exception("UTF-16 surrogate values are illegal in UTF-32");                        
                    }
                }

                /* Figure out how many bytes the result will require */
                if (ch < 0x80)
                {
                    bytesToWrite = 1;                  
                }
                else if (ch < 0x800)
                {
                    bytesToWrite = 2;                  
                }
                else if (ch < 0x10000)
                {
                    bytesToWrite = 3;                   
                }
                else if (ch < 0x110000)
                {
                    bytesToWrite = 4;                 
                }
                else
                {
                    bytesToWrite = 3;
                    ch = UNI_REPLACEMENT_CHAR;                   
                }

                for (int i = bytesToWrite - 1; i > 0; i--)
                {
                    bs[i] = (byte)((ch | byteMark) & byteMask);
                    ch >>= 6;
                }

                if (bytesToWrite > 0)
                {
                    bs[0] = (byte)(ch | firstByteMark[bytesToWrite]);

                    for (int i = 0; i < bytesToWrite; i++)
                    {
                        Stack.PutByte(bs[i]);
                    }
                }
            }
            return Stack.CurrentPos - stackPos;
        }

        public static int ConvertUTF8ToUTF16(IList<byte> bytes, int byteIndex, int byteCount, bool strictConversion)
        {
            int source = byteIndex;
            int sourceEnd = byteIndex + byteCount;
            int stackPos = Stack.CurrentPos;

            while (source < sourceEnd) 
            {
                int ch = 0;
        
	            int trailingBytesToRead = GetUTF8TrailingBytesCount(bytes, source);

                if (source + trailingBytesToRead > sourceEnd - 1)
                {
                    throw new Exception("UTF-8 string is corrupted.");
                }

               

                switch (trailingBytesToRead)
                {
                    case 3:
                        ch = GetCodeValueUTF8(bytes[source], bytes[source + 1], bytes[source + 2], bytes[source + 3]);
                        break;
                    case 2:
                        ch = GetCodeValueUTF8(bytes[source], bytes[source + 1], bytes[source + 2]);
                        break;
                    case 1:
                        ch = GetCodeValueUTF8(bytes[source], bytes[source + 1]);
                        break;
                    case 0:
                        ch = bytes[source];
                        break;
                }

                int target0 = 0;
                int target1 = 0;
                int bytesToWrite = 2;

                if (ch <= UNI_MAX_BMP)
                { /* Target is a character <= 0xFFFF */
                    /* UTF-16 surrogate values are illegal in UTF-32 */
                    if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_LOW_END)
                    {
                        if (strictConversion)
                        {
                            throw new Exception("Surrogate is illegal in UTF8");
                        }
                        else
                        {
                            target0 = UNI_REPLACEMENT_CHAR;
                        }
                    }
                    else
                    {
                        target0 = ch; /* normal case */
                    }
                }
                else if (ch > UNI_MAX_UTF16)
                {
                    if (strictConversion)
                    {
                        throw new Exception("Illegal char");
                    }
                    else
                    {
                        target0 = UNI_REPLACEMENT_CHAR;
                    }
                }
                else
                {
                    /* target is a character in range 0xFFFF - 0x10FFFF. */
                    bytesToWrite = 4;
                    ch -= halfBase;
                    target0 = ((ch >> halfShift) + UNI_SUR_HIGH_START);
                    target1 = ((ch & halfMask) + UNI_SUR_LOW_START);
                }

                byte b0 = 0x0;
                byte b1 = 0x0;

                ConvertCodeValueToBytesUTF16(target0, ref b0, ref b1);

                Stack.PutByte(b0);
                Stack.PutByte(b1);

                if (bytesToWrite == 4)
                {
                    ConvertCodeValueToBytesUTF16(target1, ref b0, ref b1);

                    Stack.PutByte(b0);
                    Stack.PutByte(b1);
                }

                source += trailingBytesToRead + 1;
            }
            return Stack.CurrentPos - stackPos;
        }

        public static void ConvertCodeValueToBytesUTF16(int codeValue, ref byte b0, ref byte b1)
        {
            b0 = (byte)codeValue;
            b1 = (byte)(codeValue >> 8);
        }

        public static void ConvertCodeValueToBytesUTF8(int codeValue, ref byte b0, ref byte b1, ref byte b2, ref byte b3)
        {
            b0 = 0;
            b1 = 0;
            b2 = 0;
            b3 = 0;

            if (0xD800 <= codeValue && codeValue <= 0xDFFF)
            {
                //These are surrogates, reserved for UTF16.
                return;
            }

            if (0 <= codeValue && codeValue <= 0x7F)
            {
                b0 = (byte)codeValue;
            }
            else
            {
                if (0x80 <= codeValue && codeValue <= 0x7FF)
                {
                    byte t = (byte)(codeValue >> 6);
                    int x = t & (byte)0x1F;
                    x = x | (byte)0xC0;
                    b0 = (byte)x;

                    t = (byte)codeValue;
                    x = t & (byte)0x3F;
                    x = x | (byte)0x80;
                    b1 = (byte)x;
                }
                else
                {
                    if (0x800 <= codeValue && codeValue <= 0xFFFF)
                    {
                        byte t = (byte)(codeValue >> 12);
                        int x = t & (byte)0x0F;
                        x = x | (byte)0xE0;
                        b0 = (byte)x;

                        t = (byte)(codeValue >> 6);
                        x = t & (byte)0x3F;
                        x = x | (byte)0x80;
                        b1 = (byte)x;

                        t = (byte)codeValue;
                        x = t & (byte)0x3F;
                        x = x | (byte)0x80;
                        b2 = (byte)x;
                    }
                    else
                    {
                        if (0x10000 <= codeValue && codeValue <= 0x10FFFF)
                        {
                            byte t = (byte)(codeValue >> 18);
                            int x = t & (byte)0x07;
                            x = x | (byte)0xF0;
                            b0 = (byte)x;

                            t = (byte)(codeValue >> 12);
                            x = t & (byte)0x3F;
                            x = x | (byte)0x80;
                            b1 = (byte)x;

                            t = (byte)(codeValue >> 6);
                            x = t & (byte)0x3F;
                            x = x | (byte)0x80;
                            b2 = (byte)x;

                            t = (byte)codeValue;
                            x = t & (byte)0x3F;
                            x = x | (byte)0x80;
                            b3 = (byte)x;
                        }
                    }
                }
            }
        }

        public static int GetCodeValueUTF16(byte a, byte b)
        {
            int xch = a;
            xch |= (int)b << 8; //256b + a
            return xch;
        }

        public static bool IsLowSurrogate(byte a, byte b)
        {
            int x = GetCodeValueUTF16(a, b);
            return (x >= UNI_SUR_LOW_START && x <= UNI_SUR_LOW_END); 
        }

        public static bool IsHighSurrogate(byte a, byte b)
        {
            int x = GetCodeValueUTF16(a, b);
            return (x >= UNI_SUR_HIGH_START && x <= UNI_SUR_HIGH_END);
        }

        private static bool IsUTF8SimpleByte(byte b)
        {
            byte mask = 0x80;
            byte comp = 0x0;

            return ((b & mask) == comp);
        }

        private static bool IsUTF8HeadByte(byte b)
        {
            byte mask = 0xC0;

            return ((b & mask) == mask);
        }

        private static bool IsUTF8TailByte(byte b)
        {
            byte mask = 0xC0;
            byte comp = 0x80;

            return ((b & mask) == comp);
        }

        private static int GetCodeValueUTF8(byte a, byte b)
        {
            int x = a & (byte)0x1F;
            int y = b & (byte)0x3F;

            x = x << 6;

            return x | y;
        }

        private static int GetCodeValueUTF8(byte a, byte b, byte c)
        {
            int x = a & 0x0F;
            int y = b & 0x3F;
            int z = c & 0x3F;

            x = x << 12;
            y = y << 6;

            return x | y | z;
        }

        private static int GetCodeValueUTF8(byte a, byte b, byte c, byte d)
        {
            int w = a & 0x07;
            int x = b & 0x3F;
            int y = c & 0x3F;
            int z = d & 0x3F;

            w = w << 18;
            x = x << 12;
            y = y << 6;

            return w | x | y | z;
        }

        private static int GetUTF8TrailingBytesCount(IList<byte> bytes, int startIndex)
        {
            int trailingBytesCount = 0;
            int b = bytes[startIndex];

            if(IsUTF8SimpleByte(bytes[startIndex]))
            {
                trailingBytesCount = 0;
            }
            else
            {
                if ((b & 0xE0) == 0xC0)
                {
                    trailingBytesCount = 1;
                }
                else
                {
                    if ((b & 0xF0) == 0xE0)
                    {
                        trailingBytesCount = 2;
                    }
                    else
                    {
                        if ((b & 0xF8) == 0xF0)
                        {
                            trailingBytesCount = 3;
                        }
                    }
                }
            }

            return trailingBytesCount;
        }
    }
}
