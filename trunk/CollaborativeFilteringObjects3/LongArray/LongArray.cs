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
using System.Text;

namespace MySpace.DataMining.CollaborativeFilteringObjects3
{
    public class LongByteArray : IList<byte>
    {
        public LongByteArray(long numelements)
        {
            int nblocks = (int)(numelements / BLOCK_SIZE);
            int remain = (int)(numelements % BLOCK_SIZE);
            int lastblocksize = BLOCK_SIZE;
            if (0 != remain)
            {
                nblocks++;
                lastblocksize = remain;
            }
            llength = numelements;
            blocks = new byte[nblocks][];
            for (int i = 0; i < (nblocks - 1); i++)
            {
                blocks[i] = new byte[BLOCK_SIZE];
            }
            if (nblocks > 0)
            {
                blocks[nblocks - 1] = new byte[lastblocksize];
            }            
        }


        public int Length
        {
            get
            {
#if DEBUG
                if (llength > (long)int.MaxValue)
                {
                    throw new OverflowException("LongByteArray.Length overflow (need LongLength)");
                }
#endif
                return (int)llength;
            }
        }

        public long LongLength
        {
            get
            {
                return llength;
            }
        }


        public byte this[int index]
        {
            get
            {
                int xblock = index / BLOCK_SIZE;
                int xblockoffset = index % BLOCK_SIZE;
                return blocks[xblock][xblockoffset];
            }

            set
            {
                int xblock = index / BLOCK_SIZE;
                int xblockoffset = index % BLOCK_SIZE;
                blocks[xblock][xblockoffset] = value;
            }
        }


        public byte this[long index]
        {
            get
            {
                int xblock = (int)(index / BLOCK_SIZE);
                int xblockoffset = (int)(index % BLOCK_SIZE);
                return blocks[xblock][xblockoffset];
            }

            set
            {
                int xblock = (int)(index / BLOCK_SIZE);
                int xblockoffset = (int)(index % BLOCK_SIZE);
                blocks[xblock][xblockoffset] = value;
            }
        }


        public int IndexOf(byte item)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, byte item)
        {
            throw new NotImplementedException();
        }

        public void RemoveAt(int index)
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get
            {
                return Length;
            }
        }

        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

        public void Add(byte item)
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public bool Contains(byte item)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(byte[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public bool Remove(byte item)
        {
            throw new NotImplementedException();
        }

        IEnumerator<byte> IEnumerable<byte>.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        long llength;
        byte[][] blocks;

        const int BLOCK_SIZE = 536870912; // Power of 2.
    }


    public struct DInt
    {
        public static void Int32ToBytes(Int32 x, LongByteArray resultbuf, long bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)x;
            resultbuf[bufoffset + 1] = (byte)((UInt32)x >> 8);
            resultbuf[bufoffset + 2] = (byte)((UInt32)x >> 16);
            resultbuf[bufoffset + 3] = (byte)((UInt32)x >> 24);
        }


        public static Int32 BytesToInt32(LongByteArray x, long offset)
        {
            Int32 result = 0;
            result |= x[offset + 0];
            result |= (Int32)((UInt32)x[offset + 1] << 8);
            result |= (Int32)((UInt32)x[offset + 2] << 16);
            result |= (Int32)((UInt32)x[offset + 3] << 24);
            return result;
        }

        public static Int32 BytesToInt32(LongByteArray x)
        {
            return BytesToInt32(x, 0);
        }


        public static void Int64ToBytes(Int64 x, LongByteArray resultbuf, long bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)x;
            resultbuf[bufoffset + 1] = (byte)((UInt64)x >> 8);
            resultbuf[bufoffset + 2] = (byte)((UInt64)x >> 16);
            resultbuf[bufoffset + 3] = (byte)((UInt64)x >> 24);
            resultbuf[bufoffset + 4] = (byte)((UInt64)x >> 32);
            resultbuf[bufoffset + 5] = (byte)((UInt64)x >> 40);
            resultbuf[bufoffset + 6] = (byte)((UInt64)x >> 48);
            resultbuf[bufoffset + 7] = (byte)((UInt64)x >> 56);
        }


        public static Int64 BytesToInt64(LongByteArray x, long offset)
        {
            Int64 result = 0;
            result |= x[offset + 0];
            result |= (Int64)((UInt64)x[offset + 1] << 8);
            result |= (Int64)((UInt64)x[offset + 2] << 16);
            result |= (Int64)((UInt64)x[offset + 3] << 24);
            result |= (Int64)((UInt64)x[offset + 4] << 32);
            result |= (Int64)((UInt64)x[offset + 5] << 40);
            result |= (Int64)((UInt64)x[offset + 6] << 48);
            result |= (Int64)((UInt64)x[offset + 7] << 56);
            return result;
        }

        public static Int64 BytesToInt64(LongByteArray x)
        {
            return BytesToInt64(x, 0);
        }
    }


    public struct OneDInt32
    {
        public Int32 this[long x]
        {
            get
            {
                return DInt.BytesToInt32(btable, x << 2);
            }

            set
            {
                DInt.Int32ToBytes(value, btable, x << 2);
            }
        }


        public OneDInt32(long numelements)
        {
            btable = new LongByteArray(numelements << 2);
        }


        LongByteArray btable;


        public long LongLength
        {
            get
            {
                return btable.LongLength >> 2;
            }
        }

        public int Length
        {
            get
            {
#if DEBUG
                if ((btable.LongLength >> 2) > (long)int.MaxValue)
                {
                    throw new OverflowException("OneDInt32.Length overflow (need LongLength)");
                }
#endif
                return (int)(btable.LongLength >> 2);
            }
        }


        public long LongByteLength
        {
            get
            {
                return btable.LongLength;
            }
        }

        public int ByteLength
        {
            get
            {
#if DEBUG
                if (btable.LongLength > (long)int.MaxValue)
                {
                    throw new OverflowException("OneDInt32.ByteLength overflow (need LongByteLength)");
                }
#endif
                return (int)btable.LongLength;
            }
        }


        public bool IsNull
        {
            get
            {
                return null == btable;
            }
        }

    }


    public struct OneDInt64
    {
        public Int64 this[long x]
        {
            get
            {
                return DInt.BytesToInt64(btable, x << 3);
            }

            set
            {
                DInt.Int64ToBytes(value, btable, x << 3);
            }
        }


        public OneDInt64(long numelements)
        {
            btable = new LongByteArray(numelements << 3);
        }


        LongByteArray btable;


        public long LongLength
        {
            get
            {
                return btable.LongLength >> 3;
            }
        }

        public int Length
        {
            get
            {
#if DEBUG
                if ((btable.LongLength >> 3) > (long)int.MaxValue)
                {
                    throw new OverflowException("OneDInt64.Length overflow (need LongLength)");
                }
#endif
                return (int)(btable.LongLength >> 3);
            }
        }


        public long LongByteLength
        {
            get
            {
                return btable.LongLength;
            }
        }

        public int ByteLength
        {
            get
            {
#if DEBUG
                if (btable.LongLength > (long)int.MaxValue)
                {
                    throw new OverflowException("OneDInt32.ByteLength overflow (need LongByteLength)");
                }
#endif
                return (int)btable.LongLength;
            }
        }


        public bool IsNull
        {
            get
            {
                return null == btable;
            }
        }

    }


    public struct TwoDInt32
    {
        public Int32 this[long x, long y]
        {
            get
            {
                return DInt.BytesToInt32(btable, ((x * rowsize) << 2) + (y << 2));
            }

            set
            {
                DInt.Int32ToBytes(value, btable, ((x * rowsize) << 2) + (y << 2));
            }
        }


        public TwoDInt32(long x, long y)
        {
            btable = new LongByteArray((x * y) << 2);
            rowsize = y;
        }


        long rowsize;
        LongByteArray btable;


        public long LongByteLength
        {
            get
            {
                return btable.LongLength;
            }
        }

        public int ByteLength
        {
            get
            {
#if DEBUG
                if (btable.LongLength > (long)int.MaxValue)
                {
                    throw new OverflowException("TwoDInt32.ByteLength overflow (need LongByteLength)");
                }
#endif
                return (int)btable.LongLength;
            }
        }


        public bool IsNull
        {
            get
            {
                return null == btable;
            }
        }
    }

}

