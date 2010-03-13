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


namespace MySpace.DataMining.DistributedObjects
{
    public class EntriesInput
    {
        public List<byte[]> net;
        public List<Entry> entries;
        public int KeyLength;
    }


    public struct Entry
    {
        public int NetIndex; // Which index of net.
        public int NetEntryOffset; // Where in the net's buffer is this entry.


        // Copies this key into buffer starting at bufferoffset.
        public void CopyKey(EntriesInput input, byte[] buffer, int bufferoffset)
        {
            byte[] netbuf = input.net[NetIndex];
            for (int i = 0; i != input.KeyLength; i++)
            {
                buffer[bufferoffset + i] = netbuf[NetEntryOffset + i];
            }
        }

        public void CopyKey(EntriesInput input, byte[] buffer)
        {
            CopyKey(input, buffer, 0);
        }

        public void AppendKey(EntriesInput input, List<byte> list)
        {
            byte[] netbuf = input.net[NetIndex];
            for (int i = 0; i != input.KeyLength; i++)
            {
                list.Add(netbuf[NetEntryOffset + i]);
            }
        }


        public void LocateKey(EntriesInput input, out byte[] netbuf, out int offset, out int length)
        {
            netbuf = input.net[NetIndex];
            offset = NetEntryOffset;
            length = input.KeyLength;
        }


        // Get the value length for this entry.
        public int GetValueLength(EntriesInput input)
        {
            byte[] netbuf = input.net[NetIndex];
            return Entry.BytesToInt(netbuf, NetEntryOffset + input.KeyLength);
        }


        // Copies this value into buffer starting at bufferoffset.
        public void CopyValue(EntriesInput input, byte[] buffer, int bufferoffset)
        {
            byte[] netbuf = input.net[NetIndex];
            //int valuelen = GetValueLength(input);
            int valuelen = Entry.BytesToInt(netbuf, NetEntryOffset + input.KeyLength);
            for (int i = 0; i != valuelen; i++)
            {
                buffer[bufferoffset + i] = netbuf[NetEntryOffset + input.KeyLength + 4 + i];
            }
        }

        public void CopyValue(EntriesInput input, byte[] buffer)
        {
            CopyValue(input, buffer, 0);
        }

        public void AppendValue(EntriesInput input, List<byte> list)
        {
            int valuelen = GetValueLength(input);
            byte[] netbuf = input.net[NetIndex];
            for (int i = 0; i != valuelen; i++)
            {
                list.Add(netbuf[NetEntryOffset + input.KeyLength + 4 + i]);
            }
        }


        public void LocateValue(EntriesInput input, out byte[] netbuf, out int offset, out int length)
        {
            netbuf = input.net[NetIndex];
            offset = NetEntryOffset + input.KeyLength + 4;
            length = GetValueLength(input);
        }

        public static ulong ToUInt64(long x)
        {
            return (ulong)(x + long.MaxValue + 1);
        }

        public static long ToInt64(ulong x)
        {
            return (long)(x - long.MaxValue - 1);
        }

		  public static UInt32 ToUInt32(Int32 x)
		  {
			  return (uint)(x + int.MaxValue + 1);
		  }

		  public static Int32 ToInt32(UInt32 x)
		  {
			  return (int)(x - int.MaxValue - 1);
		  }

		  public static UInt16 ToUInt16(Int16 x)
		  {
			  return (UInt16)(x + Int16.MaxValue + 1);
		  }

		  public static Int16 ToInt16(UInt16 x)
		  {
			  return (Int16)(x - Int16.MaxValue - 1);
		  }

		  //NOTE: bytes reversed: now big endian so sorting works better.
		  public static void UInt32ToBytes(UInt32 x, byte[] buffer, int offset)
		  {
			  buffer[0 + offset] = (byte)(x >> 24);
			  buffer[1 + offset] = (byte)(x >> 16);
			  buffer[2 + offset] = (byte)(x >> 8);
			  buffer[3 + offset] = (byte)x;
		  }

		  public static byte[] UInt32ToBytes(UInt32 x)
		  {
			  byte[] buffer = new byte[4];
			  UInt32ToBytes(x, buffer, 0);
			  return buffer;
		  }

		  public static void UInt32ToBytesAppend(UInt32 x, List<byte> buffer)
		  {
			  buffer.Add((byte)(x >> 24));
			  buffer.Add((byte)(x >> 16));
			  buffer.Add((byte)(x >> 8));
			  buffer.Add((byte)x);
		  }

          public static UInt32 BytesToUInt32(IList<byte> x, int offset)
          {
              UInt32 result = 0;
              result |= (UInt32)x[offset + 0] << 24;
              result |= (UInt32)x[offset + 1] << 16;
              result |= (UInt32)x[offset + 2] << 8;
              result |= x[offset + 3];
              return result;
          }

		  public static UInt32 BytesToUInt32(IList<byte> x)
		  {
              return BytesToUInt32(x, 0);
		  }

        // NOTE: bytes reversed: now big endian so sorting works better.
        public static Int32 BytesToInt(IList<byte> x, int offset)
        {
            int result = 0;
            result |= (int)x[offset + 0] << 24;
            result |= (int)x[offset + 1] << 16;
            result |= (int)x[offset + 2] << 8;
            result |= x[offset + 3];
            return result;
        }

        public static Int32 BytesToInt(IList<byte> x)
        {
            return BytesToInt(x, 0);
        }

        // NOTE: bytes reversed: now big endian so sorting works better.
        public static long BytesToLong(IList<byte> x, int offset)
        {
            long result = 0;
            result |= (long)x[offset + 0] << 56;
            result |= (long)x[offset + 1] << 48;
            result |= (long)x[offset + 2] << 40;
            result |= (long)x[offset + 3] << 32;
            result |= (long)x[offset + 4] << 24;
            result |= (long)x[offset + 5] << 16;
            result |= (long)x[offset + 6] << 8;
            result |= x[offset + 7];
            return result;
        }

        public static Int64 BytesToLong(IList<byte> x)
        {
            return BytesToLong(x, 0);
        }

        public static ulong BytesToULong(IList<byte> x, int offset)
        {
            ulong result = 0;
            result |= (ulong)x[offset + 0] << 56;
            result |= (ulong)x[offset + 1] << 48;
            result |= (ulong)x[offset + 2] << 40;
            result |= (ulong)x[offset + 3] << 32;
            result |= (ulong)x[offset + 4] << 24;
            result |= (ulong)x[offset + 5] << 16;
            result |= (ulong)x[offset + 6] << 8;
            result |= x[offset + 7];
            return result;
        }

        public static ulong BytesToULong(IList<byte> x)
        {
            return BytesToULong(x, 0);
        }

        public static double BytesToDouble(IList<byte> buf, int offset)
        {
            if (buf[offset] == 0x66)
            {
                long l = BytesToLong(buf, offset + 1);
                return BitConverter.Int64BitsToDouble(l);
            }

            if (buf[offset] == 0x63)
            {
                long l = BytesToLong(buf, offset + 1);
                return BitConverter.Int64BitsToDouble(~l);
            }

            if (buf[offset] == 0x65)
            {
                return 0;
            }

            if (buf[offset] == 0x61)
            {
                return double.NaN;
            }

            if (buf[offset] == 0x62)
            {
                return double.NegativeInfinity;
            }

            if (buf[offset] == 0x67)
            {
                return double.PositiveInfinity;
            }

            return -0d;
        }

        public static double BytesToDouble(IList<byte> buf)
        {
            return BytesToDouble(buf, 0);
        }

        internal static bool isNegativeZero(double x)
        {
            return x == 0 && 1 / x < 0;
        }

        public static void DoubleToBytes(double x, byte[] buffer, int offset)
        {
            if (double.IsNaN(x))
            {
                buffer[offset] = 0x61;
                for (int i = 1; i < 9; i++)
                {
                    buffer[i + offset] = 0x0;
                }
                return;
            }

            if (double.IsNegativeInfinity(x))
            {
                buffer[offset] = 0x62;
                for (int i = 1; i < 9; i++)
                {
                    buffer[i + offset] = 0x0;
                }
                return;
            }

            if (isNegativeZero(x))
            {
                buffer[offset] = 0x64;
                for (int i = 1; i < 9; i++)
                {
                    buffer[i + offset] = 0x0;
                }
                return;
            }

            if (x == 0)
            {
                buffer[offset] = 0x65;
                for (int i = 1; i < 9; i++)
                {
                    buffer[i + offset] = 0x0;
                }
                return;
            }

            if (double.IsPositiveInfinity(x))
            {
                buffer[offset] = 0x67;
                for (int i = 1; i < 9; i++)
                {
                    buffer[i + offset] = 0x0;
                }
                return;
            }

            long l = BitConverter.DoubleToInt64Bits(x);

            if (x > 0)
            {
                buffer[offset] = 0x66;
                LongToBytes(l, buffer, offset + 1);
                return;
            }

            buffer[offset] = 0x63;
            LongToBytes(~l, buffer, offset + 1);
        }

        public static void ToBytesAppendDouble(double x, List<byte> buffer)
        {
            if (double.IsNaN(x))
            {
                buffer.Add(0x61);
                for (int i = 1; i < 9; i++)
                {
                    buffer.Add(0x0);
                }
                return;
            }

            if (double.IsNegativeInfinity(x))
            {
                buffer.Add(0x62);
                for (int i = 1; i < 9; i++)
                {
                    buffer.Add(0x0);
                }
                return;
            }

            if (isNegativeZero(x))
            {
                buffer.Add(0x64);
                for (int i = 1; i < 9; i++)
                {
                    buffer.Add(0x0);
                }
                return;
            }

            if (x == 0)
            {
                buffer.Add(0x65);
                for (int i = 1; i < 9; i++)
                {
                    buffer.Add(0x0);
                }
                return;
            }

            if (double.IsPositiveInfinity(x))
            {
                buffer.Add(0x67);
                for (int i = 1; i < 9; i++)
                {
                    buffer.Add(0x0);
                }
                return;
            }

            long l = BitConverter.DoubleToInt64Bits(x);

            if (x > 0)
            {
                buffer.Add(0x66);
                ToBytesAppend64(l, buffer);
                return;
            }

            buffer.Add(0x63);
            ToBytesAppend64(~l, buffer);
        }

        // NOTE: new to DO5
        public static void LongToBytes(long x, byte[] resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)(x >> 56);
            resultbuf[bufoffset + 1] = (byte)(x >> 48);
            resultbuf[bufoffset + 2] = (byte)(x >> 40);
            resultbuf[bufoffset + 3] = (byte)(x >> 32);
            resultbuf[bufoffset + 4] = (byte)(x >> 24);
            resultbuf[bufoffset + 5] = (byte)(x >> 16);
            resultbuf[bufoffset + 6] = (byte)(x >> 8);
            resultbuf[bufoffset + 7] = (byte)x;
        }

        public static void ULongToBytes(ulong x, byte[] resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)(x >> 56);
            resultbuf[bufoffset + 1] = (byte)(x >> 48);
            resultbuf[bufoffset + 2] = (byte)(x >> 40);
            resultbuf[bufoffset + 3] = (byte)(x >> 32);
            resultbuf[bufoffset + 4] = (byte)(x >> 24);
            resultbuf[bufoffset + 5] = (byte)(x >> 16);
            resultbuf[bufoffset + 6] = (byte)(x >> 8);
            resultbuf[bufoffset + 7] = (byte)x;
        }

        public static void ULongToBytesAppend(ulong x, List<byte> buffer)
        {
            buffer.Add((byte)(x >> 56));
            buffer.Add((byte)(x >> 48));
            buffer.Add((byte)(x >> 40));
            buffer.Add((byte)(x >> 32));
            buffer.Add((byte)(x >> 24));
            buffer.Add((byte)(x >> 16));
            buffer.Add((byte)(x >> 8));
            buffer.Add((byte)x);
        }

        // NOTE: bytes reversed: now big endian so sorting works better.
        public static void ToBytes(Int32 x, byte[] resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)(x >> 24);
            resultbuf[bufoffset + 1] = (byte)(x >> 16);
            resultbuf[bufoffset + 2] = (byte)(x >> 8);
            resultbuf[bufoffset + 3] = (byte)x;
        }

        public static byte[] ToBytes(Int32 x)
        {
            byte[] result = new byte[4];
            ToBytes(x, result, 0);
            return result;
        }

        public static void ToBytesAppend64(Int64 x, List<byte> abuf)
        {
            abuf.Add((byte)(x >> 56));
            abuf.Add((byte)(x >> 48));
            abuf.Add((byte)(x >> 40));
            abuf.Add((byte)(x >> 32));
            abuf.Add((byte)(x >> 24));
            abuf.Add((byte)(x >> 16));
            abuf.Add((byte)(x >> 8));
            abuf.Add((byte)x);
        }

        public static void ToBytesAppend(Int32 x, List<byte> abuf)
        {
            abuf.Add((byte)(x >> 24));
            abuf.Add((byte)(x >> 16));
            abuf.Add((byte)(x >> 8));
            abuf.Add((byte)x);
        }


        public static void Int16ToBytes(Int16 x, byte[] resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)(x >> 8);
            resultbuf[bufoffset + 1] = (byte)x;
        }

        public static void Int16ToBytesAppend(Int16 x, List<byte> resultbuf)
        {
            resultbuf.Add((byte)(x >> 8));
            resultbuf.Add((byte)x);
        }

        public static void UInt16ToBytes(UInt16 x, byte[] resultbuf, int bufoffset)
        {
          resultbuf[bufoffset + 0] = (byte)(x >> 8);
          resultbuf[bufoffset + 1] = (byte)x;
        }

        public static void UInt16ToBytesAppend(UInt16 x, List<byte> resultbuf)
        {
          resultbuf.Add((byte)(x >> 8));
          resultbuf.Add((byte)x);
        }

        public static byte[] Int16ToBytes(Int16 x)
        {
            byte[] result = new byte[2];
            Int16ToBytes(x, result, 0);
            return result;
        }

        public static byte[] UInt16ToBytes(UInt16 x)
        {
          byte[] result = new byte[2];
          UInt16ToBytes(x, result, 0);
          return result;
        }

        public static Int16 BytesToInt16(IList<byte> x, int offset)
        {
            Int16 result = 0;
            result |= (Int16)((UInt16)x[offset + 0] << 8);
            result |= (Int16)(UInt16)x[offset + 1];
            return result;
        }

        public static UInt16 BytesToUInt16(IList<byte> x, int offset)
        {
          UInt16 result = 0;
          result |= (UInt16)(x[offset + 0] << 8);
          result |= (UInt16)x[offset + 1];
          return result;
        }

        public static Int16 BytesToInt16(IList<byte> x)
        {
            return BytesToInt16(x, 0);
        }

		  public static UInt16 BytesToUInt16(IList<byte> x)
		  {
			  return BytesToUInt16(x, 0);
		  }

        public static void AsciiToBytesAppend(StringBuilder x, List<byte> abuf)
        {
            for (int i = 0; i != x.Length; i++)
            {
                ushort y = x[i];
                if (y >= 0x80)
                {
                    //throw new Exception("Input text not ASCII");
                    abuf.Add((byte)'?');
                    continue;
                }
                abuf.Add((byte)y);
            }
        }

        public static void AsciiToBytesAppend(string x, List<byte> abuf)
        {
            for (int i = 0; i != x.Length; i++)
            {
                ushort y = x[i];
                if (y >= 0x80)
                {
                    //throw new Exception("Input text not ASCII");
                    abuf.Add((byte)'?');
                    continue;
                }
                abuf.Add((byte)y);
            }
        }

        public static void AsciiToBytes(string x, byte[] buf, int offset)
        {
            for (int i = 0; i != x.Length; i++)
            {
                ushort y = x[i];
                if (y >= 0x80)
                {
                    //throw new Exception("Input text not ASCII");
                    buf[offset + i] = (byte)'?';
                    continue;
                }
                buf[offset + i] = (byte)y;
            }
        }


        public static void BytesToAsciiAppend(IList<byte> x, StringBuilder buf, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++)
            {
                if (x[i] >= 0x80)
                {
                    //throw new Exception("Input text not ASCII");
                    buf.Append('?');
                    continue;
                }
                buf.Append((char)x[i]);
            }
        }

        public static void BytesToAsciiAppend(IList<byte> x, StringBuilder buf)
        {
            BytesToAsciiAppend(x, buf, 0, x.Count);
        }

        public static string BytesToAscii(IList<byte> x, int offset, int length)
        {
            StringBuilder buf = new StringBuilder(x.Count);
            BytesToAsciiAppend(x, buf, offset, length);
            return buf.ToString();
        }

        public static string BytesToAscii(IList<byte> x)
        {
            return BytesToAscii(x, 0, x.Count);
        }


        public static Int32 Round2Power(Int32 x)
        {
            if (x <= 0)
            {
                return 1;
            }
            x--;
            x |= x >> 1;
            x |= x >> 2;
            x |= x >> 4;
            x |= x >> 8;
            x |= x >> 16;
            x++;
            return x;
        }

        public static long Round2PowerLong(long x)
        {
            if (x <= 0)
            {
                return 1;
            }
            x--;
            x |= x >> 1;
            x |= x >> 2;
            x |= x >> 4;
            x |= x >> 8;
            x |= x >> 16;
            x |= x >> 32;
            x++;
            return x;
        }

    }


    public abstract class EntriesOutput
    {
        const int REDUCE_ADD_MAX_BYTES = 1073741824;

        byte[] buf;
        int bufpos = 0;
        bool flushed = false;
        bool nogrow = false;


        public bool CheckFlushedReset()
        {
            bool result = flushed;
            flushed = false;
            return result;
        }


        public int BatchedByteCount
        {
            get
            {
                return bufpos;
            }
        }


        // Note: buf needs to be mutable, even immediately after flushing (so can't be network buffer).
        public EntriesOutput(byte[] buf)
        {
            this.buf = buf;
        }


        public void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
        {
            if (keylength + 4 + valuelength > buf.Length - bufpos)
            {
                if (!flushed) // Note! if already flushed, cannot flush again yet.
                {
                    doflush();
                }
            }

            for (; ; )
            {
                // 1 GB limit - enter no grow state.
                if (bufpos + keylength + 4 + valuelength > buf.Length)
                {
                    if (nogrow)
                    {
                        return;
                    }
                    int lnew = Entry.Round2Power(buf.Length + keylength + 4 + valuelength);
                    if (lnew > REDUCE_ADD_MAX_BYTES)
                    {
                        nogrow = true;
                        lnew = REDUCE_ADD_MAX_BYTES;
                    }
                    byte[] newbuf = new byte[lnew];
                    Buffer.BlockCopy(buf, 0, newbuf, 0, bufpos);
                    buf = newbuf;
                    if (nogrow)
                    {
                        continue; // Since it hit the reduce max and cannot grow, see if this key/value actually fits now.
                    }
                }
                break;
            }

            for (int i = 0; i != keylength; i++)
            {
                buf[bufpos + i] = keybuf[keyoffset + i];
            }
            Entry.ToBytes(valuelength, buf, bufpos + keylength);
            for (int i = 0; i != valuelength; i++)
            {
                buf[bufpos + keylength + 4 + i] = valuebuf[valueoffset + i];
            }
            bufpos += keylength + 4 + valuelength;
        }

        public void Add(ByteSlice key, ByteSlice value)
        {
            Add(key.buf, key.offset, key.length, value.buf, value.offset, value.length);
        }

        public void Add(IList<byte> key, IList<byte> value)
        {
            Add(key, 0, key.Count, value, 0, value.Count);
        }


        public abstract bool SendBatchedEntriesOutput(byte[] buf, int length);

        public abstract bool EndBatch(); // Returns false if at end, true if more.


        bool doflush()
        {
            if (flushed)
            {
                throw new Exception("Flushed twice in a row");
            }

            if (bufpos > 0)
            {
                if (SendBatchedEntriesOutput(this.buf, bufpos))
                {
                    flushed = true;
                    bufpos = 0;
                    return true;
                }
            }
            return false;
        }


        public void Finish()
        {
            if (!flushed && doflush())
            {
                if (0 != bufpos)
                {
                    throw new Exception("bufpos != 0");
                }
            }
            flushed = false; // Explicitly finished; starting over.
        }
    }


    public abstract class FixedEntriesOutput
    {
        const int REDUCE_ADD_MAX_BYTES = 1073741824;

        byte[] buf;
        int bufpos = 0;
        bool flushed = false;
        bool nogrow = false;


        public bool CheckFlushedReset()
        {
            bool result = flushed;
            flushed = false;
            return result;
        }


        public int BatchedByteCount
        {
            get
            {
                return bufpos;
            }
        }


        // Note: buf needs to be mutable, even immediately after flushing (so can't be network buffer).
        public FixedEntriesOutput(byte[] buf)
        {
            this.buf = buf;
        }


        public void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength)
        {
            if (keylength + valuelength > buf.Length - bufpos)
            {
                if (!flushed) // Note! if already flushed, cannot flush again yet.
                {
                    doflush();
                }
            }

            for (; ; )
            {
                // 1 GB limit - enter no grow state.
                if (bufpos + keylength + valuelength > buf.Length)
                {
                    if (nogrow)
                    {
                        return;
                    }
                    int lnew = Entry.Round2Power(buf.Length + keylength + valuelength);
                    if (lnew > REDUCE_ADD_MAX_BYTES)
                    {
                        nogrow = true;
                        lnew = REDUCE_ADD_MAX_BYTES;
                    }
                    byte[] newbuf = new byte[lnew];
                    Buffer.BlockCopy(buf, 0, newbuf, 0, bufpos);
                    buf = newbuf;
                    if (nogrow)
                    {
                        continue; // Since it hit the reduce max and cannot grow, see if this key/value actually fits now.
                    }
                }
                break;
            }

            for (int i = 0; i != keylength; i++)
            {
                buf[bufpos + i] = keybuf[keyoffset + i];
            }
            for (int i = 0; i != valuelength; i++)
            {
                buf[bufpos + keylength + i] = valuebuf[valueoffset + i];
            }
            bufpos += keylength + valuelength;
        }

        public void Add(ByteSlice key, ByteSlice value)
        {
            Add(key.buf, key.offset, key.length, value.buf, value.offset, value.length);
        }

        public void Add(IList<byte> key, IList<byte> value)
        {
            Add(key, 0, key.Count, value, 0, value.Count);
        }


        public abstract bool SendBatchedEntriesOutput(byte[] buf, int length);

        public abstract bool EndBatch(); // Returns false if at end, true if more.


        bool doflush()
        {
            if (flushed)
            {
                throw new Exception("Flushed twice in a row");
            }

            if (bufpos > 0)
            {
                if (SendBatchedEntriesOutput(this.buf, bufpos))
                {
                    flushed = true;
                    bufpos = 0;
                    return true;
                }
            }
            return false;
        }


        public void Finish()
        {
            if (!flushed && doflush())
            {
                if (0 != bufpos)
                {
                    throw new Exception("bufpos != 0");
                }
            }
            flushed = false; // Explicitly finished; starting over.
        }
    }


    public interface IBeforeReduce
    {
        // Returns true to continue enumerating; false to stop.
        bool OnGetEnumerator(EntriesInput input, EntriesOutput output);
    }


    public interface IBeforeReduceOrd
    {
        void SetReduceOrd(int ord);
        int ReduceOrd { get; }
    }


    public interface IReducedToFile
    {
        // If appendsizes is not null, sizes of all the reduced files are appended, in order.
        // Note: sizes should exclude the header data!
        int GetReducedFileCount(int n, IList<long> appendsizes);
    }


    public interface IBeforeReduceFixed
    {
        // Returns true to continue enumerating; false to stop.
        bool OnGetEnumerator(EntriesInput input, FixedEntriesOutput output);
    }


    public interface IAfterReduce
    {
        void OnEnumeratorFinished();
    }


    public class EntryEnumerator : IBeforeReduce
    {
        int i = 0;

        public bool OnGetEnumerator(EntriesInput input, EntriesOutput output)
        {
            int count = input.entries.Count;
            if (i >= count)
            {
                i = 0;
                return false; // At end; stop.
            }

            Entry entry = input.entries[i++];

            byte[] keybuf;
            int keyoffset;
            int keylength;
            entry.LocateKey(input, out keybuf, out keyoffset, out keylength);

            byte[] valuebuf;
            int valueoffset;
            int valuelength;
            entry.LocateValue(input, out valuebuf, out valueoffset, out valuelength);

            output.Add(keybuf, keyoffset, keylength, valuebuf, valueoffset, valuelength);
            return true; // Continue.
        }
    }


    public class FixedEntryEnumerator : IBeforeReduceFixed
    {
        int i = 0;

        public bool OnGetEnumerator(EntriesInput input, FixedEntriesOutput output)
        {
            int count = input.entries.Count;
            if (i >= count)
            {
                i = 0;
                return false; // At end; stop.
            }

            Entry entry = input.entries[i++];

            byte[] keybuf;
            int keyoffset;
            int keylength;
            entry.LocateKey(input, out keybuf, out keyoffset, out keylength);

            byte[] valuebuf;
            int valueoffset;
            int valuelength;
            entry.LocateValue(input, out valuebuf, out valueoffset, out valuelength);

            output.Add(keybuf, keyoffset, keylength, valuebuf, valueoffset, valuelength);
            return true; // Continue.
        }
    }


    //public struct ReduceInput : IEnumerator<Entry>
    public struct ReduceInput
    {
        EntriesInput einput;
        int eindex;
        int estartindex;


        public void Dispose()
        {
        }


        /*private ReduceInput()
        {
        }*/

        public static ReduceInput Start(EntriesInput einput)
        {
            ReduceInput result;
            result.einput = einput;
            result.eindex = -1; // Start one before.
            result.estartindex = 0;
            return result;
        }


        bool IsStillSameKey()
        {
            //System.Diagnostics.Debug.Assert(estartindex < einput.entries.Count);
            //System.Diagnostics.Debug.Assert(eindex >= estartindex);

            if (eindex >= einput.entries.Count)
            {
                return false;
            }

            Entry startentry = einput.entries[estartindex];
            byte[] startkeybuf;
            int startkeyoffset;
            int startkeylength;
            startentry.LocateKey(einput, out startkeybuf, out startkeyoffset, out startkeylength);

            Entry entry = einput.entries[eindex];
            byte[] keybuf;
            int keyoffset;
            int keylength;
            entry.LocateKey(einput, out keybuf, out keyoffset, out keylength);

            if (startkeylength != keylength) // Shouldn't happen.
            {
                return false;
            }

            for (int i = 0; i != startkeylength; i++)
            {
                if (startkeybuf[startkeyoffset + i] != keybuf[keyoffset + i])
                {
                    return false;
                }
            }
            return true;
        }


        internal bool MoveNextKey()
        {
            while (MoveNext())
            {
                // Skip the same ones.
            }
            estartindex = eindex;
            eindex--; // Start one before.
            return eindex < einput.entries.Count;
        }


        public ReduceEntry Current
        {
            get
            {
                if (eindex < estartindex)
                {
                    throw new InvalidOperationException("The enumerator is positioned before the first element of the collection");
                }

                /*if (!IsStillSameKey())
                {
                    throw new InvalidOperationException("The enumerator is positioned after the last element of the collection");
                }*/

                //return einput.entries[eindex];
                ReduceEntry result;
                result.inp = einput;
                result.ent = einput.entries[eindex];
                return result;
            }
        }


        public bool MoveNext()
        {
            eindex++;
            if (!IsStillSameKey())
            {
                eindex--;
                return false;
            }
            return true;
        }


        public void Reset()
        {
            eindex = estartindex - 1;
        }
    }

    public struct Blob
    {
        public string name;
        public byte[] data;
        public static readonly int padSize = 260;

        public static Blob Prepare(string name, byte[] data)
        {
            Blob b;
            b.name = name;
            b.data = data;
            return b;
        }

        public override string ToString()
        {
            return "[name] " + (name == null ? "null" : name) + " [data.Length] " + (data == null ? "0" : data.Length.ToString());
        }

        public string ToDfsLine()
        {
            string sbuf = Convert.ToBase64String(data);
            string padname = name;

            if (name.Length < padSize)
            {
                padname = name.PadRight(padSize);
            }
            else if (name.Length > padSize)
            {
                padname = name.Substring(0, padSize);
            }
            
            return padname + sbuf;
        }

        public static Blob FromDfsLine(string dfsline)
        {
            string name = dfsline.Substring(0, padSize).Trim();
            string sbuf = dfsline.Substring(padSize);
            byte[] buf = Convert.FromBase64String(sbuf);
            return Blob.Prepare(name, buf);
        }
    }

    public struct ByteSlice
    {
        //internal byte[] buf;
        internal IList<byte> buf;
        internal int offset;
        internal int length;


        public void GetComponents(out IList<byte> buf, out int offset, out int length)
        {
            buf = this.buf;
            offset = this.offset;
            length = this.length;
        }


        public override string ToString()
        {
            //return Entry.BytesToAscii(buf, offset, length);
            byte[] babuf = new byte[length];
            for (int i = 0; i < length; i++)
            {
                babuf[i] = buf[offset + i];
            }
            return Encoding.UTF8.GetString(babuf);
        }

        public Blob ReadBinary()
        {
            string s = ToString();
            return Blob.FromDfsLine(s);    
        }

        public ByteSlice FlipAllBits(IList<byte> buffer)
        {
            for (int i = 0; i < length; i++)
            {
                buffer[i] = (byte)(~buf[i + offset]);
            }
            return ByteSlice.Prepare(buffer, 0, length);
        }

        /*
         public string ToStringUTF16()
         {
             byte[] babuf = new byte[length];
             for (int i = 0; i < length; i++)
             {
                 babuf[i] = buf[offset + i];
             }
             return Encoding.Unicode.GetString(babuf);
         }
        */


        static System.Reflection.Assembly asmPOS = null;

        delegate string _TagPartsOfSpeechDelegate(string s);

        static _TagPartsOfSpeechDelegate _TagPartsOfSpeech = null;

        static void _EnsurePOS()
        {
            if (null == asmPOS)
            {
                asmPOS = System.Reflection.Assembly.LoadFrom("CSharpNLP.dll");
                Type CSharpNlp_Type = asmPOS.GetType("CSharpNLP_ns.CSharpNlp", true); // throwOnError=true
                System.Reflection.MethodInfo TagPartsOfSpeech_MethodInfo = CSharpNlp_Type.GetMethod(
                    "TagPartsOfSpeech",
                    System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public
                    | System.Reflection.BindingFlags.InvokeMethod);
                if (null == TagPartsOfSpeech_MethodInfo)
                {
                    throw new Exception("Unable to find method CSharpNLP_ns.CSharpNlp.TagPartsOfSpeech");
                }
                _TagPartsOfSpeech = (_TagPartsOfSpeechDelegate)Delegate.CreateDelegate(
                    typeof(_TagPartsOfSpeechDelegate), TagPartsOfSpeech_MethodInfo, true);
            }
        }

        public static System.Xml.XmlDocument ToPOS(string text)
        {
            string pos;
            try
            {
                lock ("ToPOS(37947A47-ECEF-4bc5-8422-915DF180DF62)")
                {
                    _EnsurePOS();
                    pos = _TagPartsOfSpeech(text);
                }
            }
            catch (Exception e)
            {
                throw new Exception("ToPOS(): MySpace.DataMining.NLP must be installed on all sub processes before using this method", e);
            }
            System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
            xd.LoadXml("<words>" + pos + "</words>");
            return xd;
        }

        public static string ToPOSString(string text)
        {
            string pos;
            try
            {
                lock ("ToPOS(37947A47-ECEF-4bc5-8422-915DF180DF62)")
                {
                    _EnsurePOS();
                    pos = _TagPartsOfSpeech(text);
                }
            }
            catch (Exception e)
            {
                throw new Exception("ToPOS(): MySpace.DataMining.NLP must be installed on all sub processes before using this method", e);
            }
            return pos;
        }

        public System.Xml.XmlDocument ToPOS()
        {
            return ToPOS(ToString());
        }

        public string ToPOSString()
        {
            return ToPOSString(ToString());
        }


        public static ByteSlice Prepare(string x)
        {
            /*
            List<byte> buf = new List<byte>(x.Length);
            Entry.AsciiToBytesAppend(x, buf);
            return ByteSlice.Prepare(buf);
             * */
            byte[] babuf = Encoding.UTF8.GetBytes(x);
            return ByteSlice.Prepare(babuf);
        }
		 /*
		  public static ByteSlice PrepareUTF16(string x)
		  {
			  byte[] babuf = Encoding.Unicode.GetBytes(x);
			  return ByteSlice.Prepare(babuf);
		  }
		 */
        public static ByteSlice PreparePaddedStringAscii(string x, int size)
        {
            int xByteLen = x.Length;
            int min = (size < xByteLen) ? size : xByteLen;
            byte[] buffer = new byte[size];

            Encoding.ASCII.GetBytes(x, 0, min, buffer, 0);

            return ByteSlice.Prepare(buffer);
        }

        public static ByteSlice PreparePaddedStringUTF8(string x, int size)
        {
            return ByteSlice.Prepare(PreparePaddedStringUTF8_Array(x, size), 0, size);
        }

        public static ByteSlice PreparePaddedMString(mstring x, int size)
        {
            return x.ToByteSlice(size);
        }

        private static List<byte[]> PreparePaddedStringUTF8_buffers = null;
        private static int PreparePaddedStringUTF8_curBufferPos = 0;
        private static long PreparePaddedStringUTF8_curMapIteration = 0;

        private static byte[] PreparePaddedStringUTF8_Alloc(int size)
        {
            if (StaticGlobals.ExecutionContext == ExecutionContextType.MAP && size == StaticGlobals.DSpace_KeyLength)
            {
                if (PreparePaddedStringUTF8_buffers == null)
                {
                    PreparePaddedStringUTF8_buffers = new List<byte[]>(100);
                    PreparePaddedStringUTF8_curBufferPos = 0;
                    PreparePaddedStringUTF8_curMapIteration = 0;

                    for (int i = 0; i < 100; i++)
                    {
                        PreparePaddedStringUTF8_buffers.Add(new byte[StaticGlobals.DSpace_KeyLength]);
                    }
                }

                if (PreparePaddedStringUTF8_curMapIteration != StaticGlobals.MapIteration)
                {
                    PreparePaddedStringUTF8_curMapIteration = StaticGlobals.MapIteration;
                    PreparePaddedStringUTF8_curBufferPos = 0;
                }

                if (PreparePaddedStringUTF8_curBufferPos > PreparePaddedStringUTF8_buffers.Count - 1)
                {
                    PreparePaddedStringUTF8_buffers.Add(new byte[StaticGlobals.DSpace_KeyLength]);
                }

                return PreparePaddedStringUTF8_buffers[PreparePaddedStringUTF8_curBufferPos++];
            }
            else
            {
                return new byte[size];
            }
        }

        private static byte[] PreparePaddedStringUTF8_Array(string x, int size)
        {
            int xByteLen = Encoding.UTF8.GetByteCount(x);
            byte[] buffer = null;

            if (xByteLen > size)
            {
                buffer = Encoding.UTF8.GetBytes(x);

                if (IsExtByteUTF8(buffer[size]))
                {
                    int len = size - 1;

                    for (; len >= 0; len--)
                    {
                        if (!IsExtByteUTF8(buffer[len]))
                        {
                            break;
                        }
                    }

                    if (len < 0) len = 0;

                    for (int i = size - 1; i >= len; i--)
                    {
                        buffer[i] = 0x0;
                    }
                }
            }
            else
            {
                buffer = PreparePaddedStringUTF8_Alloc(size);
                Encoding.UTF8.GetBytes(x, 0, x.Length, buffer, 0);

                for (int i = xByteLen; i < size; i++)
                {
                    buffer[i] = 0x0;
                }
            }

            return buffer;
        }
     
        public static byte[] PreparePaddedUTF8Bytes(byte[] bytes, int byteIndex, int byteCount, int size)
        {
            byte[] buffer = PreparePaddedStringUTF8_Alloc(size);
            int min = byteCount > size ? size : byteCount;

            for (int i = 0; i < min; i++)
            {
                buffer[i] = bytes[i + byteIndex];
            }

            if (byteCount > size)
            {
                if (IsExtByteUTF8(bytes[byteIndex + size]))
                {
                    bool found = false;

                    for (int i = size - 1; i >= 0; i--)
                    {
                        found = !IsExtByteUTF8(buffer[i]);
                        buffer[i] = 0x0;

                        if (found)
                        {
                            break;
                        }
                    }                    
                }
            }
            else
            {                
                for (int i = byteCount; i < size; i++)
                {
                    buffer[i] = 0x0;
                }
            }

            return buffer;
        }

        private static bool IsExtByteUTF8(byte b)
        {
            byte mask = 0xC0;
            byte comp = 0x80;

            return ((mask & b) == comp);
        }
        
        public static ByteSlice Prepare(StringBuilder x)
        {
            /*
            List<byte> buf = new List<byte>(x.Length);
            Entry.AsciiToBytesAppend(x, buf);
            return ByteSlice.Create(buf);
             * */
            return Prepare(x.ToString());
        }
		 /*
		  public static ByteSlice PrepareUTF16(StringBuilder x)
		  {
			  return PrepareUTF16(x.ToString());
		  }
		 */
        public static ByteSlice Prepare(IList<byte> data)
        {
            ByteSlice result;
            result.buf = data;
            result.offset = 0;
            result.length = data.Count;
            return result;
        }

        public static ByteSlice Prepare(IList<byte> data, int offset, int length)
        {
            ByteSlice result;
            result.buf = data;
            result.offset = offset;
            result.length = length;
            return result;
        }

        public static ByteSlice Prepare(ByteSlice data, int offset, int length)
        {
            if (offset < 0 || length < 0 || offset + length > data.length)
            {
                IndexOutOfRangeException ior = new IndexOutOfRangeException("ByteSlice.Prepare(ByteSlice{offset=" + data.offset + ", length=" + data.length + "}, offset=" + offset + ", length=" + length + ")");
                throw new ArgumentOutOfRangeException("Specified argument was out of the range of valid values. Index out of bounds: " + ior.Message, ior); // Preserve the old exception type.
            }
            ByteSlice result;
            result.buf = data.buf;
            result.offset = data.offset + offset;
            result.length = length;
            return result;
        }

        public static ByteSlice Prepare(mstring ms)
        {
            return ms.ToByteSlice();
        }

        public static ByteSlice Prepare()
        {
            ByteSlice result;
            result.buf = null;
            result.offset = 0;
            result.length = 0;
            return result;
        }
        
        public static ByteSlice Create(string x)
        {
            return Prepare(x);
        }

        public static ByteSlice Create(StringBuilder x)
        {
            return Prepare(x);
        }

        public static ByteSlice Create(IList<byte> data)
        {
            return Prepare(data);
        }

        public static ByteSlice Create(IList<byte> data, int offset, int length)
        {
            return Prepare(data, offset, length);
        }

        public static ByteSlice Create(ByteSlice data, int offset, int length)
        {
            return Prepare(data, offset, length);
        }

        public static ByteSlice Create()
        {
            return Prepare();
        }


        public byte this[int index]
        {
            get
            {
                if (index < 0 || index >= length)
                {
                    throw new ArgumentOutOfRangeException();
                }
                return buf[offset + index];
            }

            set
            {
                if (index < 0 || index >= length)
                {
                    throw new ArgumentOutOfRangeException();
                }
                buf[offset + index] = value;
            }
        }


        public int Length
        {
            get
            {
                return length;
            }
        }


        // Copies this slice into buffer starting at bufferoffset.
        public void CopyTo(byte[] buffer, int bufferoffset)
        {
            for (int i = 0; i != length; i++)
            {
                buffer[bufferoffset + i] = this.buf[offset + i];
            }
        }

        public void CopyTo(byte[] buffer)
        {
            CopyTo(buffer, 0);
        }

        public void AppendTo(List<byte> list)
        {
            for (int i = 0; i != length; i++)
            {
                list.Add(this.buf[offset + i]);
            }
        }


        public byte[] ToBytes()
        {
            byte[] result = new byte[length];
            CopyTo(result, 0);
            return result;
        }
    }


    public struct ReduceEntry
    {
        internal EntriesInput inp;
        internal Entry ent;


        public static ReduceEntry Create(EntriesInput inp, Entry ent)
        {
            ReduceEntry result;
            result.inp = inp;
            result.ent = ent;
            return result;
        }


        public ByteSlice Key
        {
            get
            {
                ByteSlice result;
                byte[] keybuf;
                ent.LocateKey(inp, out keybuf, out result.offset, out result.length);
                result.buf = keybuf;
                return result;
            }
        }


        // Copies this key into buffer starting at bufferoffset.
        public void CopyKey(byte[] buffer, int bufferoffset)
        {
            ent.CopyKey(inp, buffer, bufferoffset);
        }

        public void CopyKey(byte[] buffer)
        {
            ent.CopyKey(inp, buffer);
        }

        public void AppendKey(List<byte> list)
        {
            ent.AppendKey(inp, list);
        }


        public ByteSlice Value
        {
            get
            {
                ByteSlice result;
                byte[] valuebuf;
                ent.LocateValue(inp, out valuebuf, out result.offset, out result.length);
                result.buf = valuebuf;
                return result;
            }
        }


        // Copies this value into buffer starting at bufferoffset.
        public void CopyValue(byte[] buffer, int bufferoffset)
        {
            ent.CopyValue(inp, buffer, bufferoffset);
        }

        public void CopyValue(byte[] buffer)
        {
            ent.CopyValue(inp, buffer);
        }

        public void AppendValue(List<byte> list)
        {
            ent.AppendValue(inp, list);
        }
    }


    public class GZipWriter
    {
        public static System.IO.Stream Create(string path)
        {
            return new System.IO.Compression.GZipStream(new System.IO.FileStream(path, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read), System.IO.Compression.CompressionMode.Compress);
        }

        public static System.IO.Stream Create(System.IO.Stream stm)
        {
            return new System.IO.Compression.GZipStream(stm, System.IO.Compression.CompressionMode.Compress);
        }
    }


    public class GzipFastLoader : IDisposable
    {
        public GzipFastLoader(string gzipfilenames, int linebuffersize, int filebuffersize)
        {
            this.buffersize = filebuffersize;
            this.linebuf = new byte[linebuffersize];
            this.agzipfilenames = gzipfilenames.Split(';');

            loadnextfile();
        }

        public GzipFastLoader(string gzipfilenames, int linebuffersize)
            : this(gzipfilenames, linebuffersize, -1)
        {
        }


        public int Read(byte[] buffer, int offset, int count)
        {
            int result = 0;
            while (result < count && null != cstm)
            {
                int x = cstm.Read(buffer, offset, count);
                if (0 == x)
                {
                    loadnextfile();
                    continue;
                }
                result += x;
            }
            return result;
        }

        public int ReadByte()
        {
            throw new Exception("ReadByte not implemented");
        }


        public bool EOF
        {
            get
            {
                return cstm == null;
            }
        }


        public int DefaultValueInt
        {
            get
            {
                return defaultvalueint;
            }

            set
            {
                defaultvalueint = value;
            }
        }


        public bool ReadLine(out int a)
        {
            int len = BufferLine();
            int offset = 0;
            a = GetNextInt(ref offset, len);
            return 0 != len;
        }

        public bool ReadLine(out int a, out int b)
        {
            int len = BufferLine();
            int offset = 0;
            a = GetNextInt(ref offset, len);
            b = GetNextInt(ref offset, len);
            return 0 != len;
        }

        public bool ReadLine(out int a, out int b, out int c)
        {
            int len = BufferLine();
            int offset = 0;
            a = GetNextInt(ref offset, len);
            b = GetNextInt(ref offset, len);
            c = GetNextInt(ref offset, len);
            return 0 != len;
        }

        public bool ReadLine(out int a, out int b, out int c, out int d)
        {
            int len = BufferLine();
            int offset = 0;
            a = GetNextInt(ref offset, len);
            b = GetNextInt(ref offset, len);
            c = GetNextInt(ref offset, len);
            d = GetNextInt(ref offset, len);
            return 0 != len;
        }


        public bool ReadLineAppend(List<int> outs)
        {
            int prevoffset = 0;
            int offset = 0;
            int len = BufferLine();
            for (; ; )
            {
                int x = GetNextInt(ref offset, len);
                if (offset == prevoffset)
                {
                    break;
                }
                outs.Add(x);
                prevoffset = offset;
            }
            return 0 != len;
        }


        public string ReadLine()
        {
            int len = BufferLine();
            if (0 == len)
            {
                return null;
            }
            while (len > 0)
            {
                if ('\n' == linebuf[len - 1] || '\r' == linebuf[len - 1])
                {
                    len--;
                }
                else
                {
                    break;
                }
            }
            return Encoding.UTF8.GetString(linebuf, 0, len);
        }


        // Returns length; includes line terminators; lines longer than linebuf are clipped. 0 return is EOF.
        public int BufferLine()
        {
            if (null == cstm)
            {
                return 0;
            }
            int offset = 0;
            int x;
            bool gotany = false;
            while (offset < linebuf.Length)
            {
                // Only buffer a line from one file.
                x = cstm.ReadByte();
                if (-1 == x)
                {
                    loadnextfile(); // Always skip ahead in this case so that EOF property works.
                    if (!gotany)
                    {
                        if (cstm == null)
                        {
                            break;
                        }
                        continue;
                    }
                    break;
                }
                gotany = true;
                if ('\n' == x)
                {
                    break;
                }
                if (offset < linebuf.Length)
                {
                    linebuf[offset++] = (byte)x;
                }
            }
            return offset;
        }


        protected int GetNextInt(ref int offset, int length)
        {
            unchecked
            {
                // Skip leading non-digits.
                for (; ; offset++)
                {
                    if (offset >= length)
                    {
                        return defaultvalueint;
                    }
                    if ('-' == linebuf[offset]
                        || (linebuf[offset] >= '0' && linebuf[offset] <= '9'))
                    {
                        break;
                    }
                }

                if (offset >= length)
                {
                    return defaultvalueint;
                }
                bool neg = false;
                if ('-' == linebuf[offset])
                {
                    neg = true;
                    offset++;
                }

                int result = 0;
                for (; offset < length; offset++)
                {
                    byte by = linebuf[offset];
                    if (by >= '0' && by <= '9')
                    {
                        result *= 10;
                        result += (byte)by - '0';
                    }
                    else
                    {
                        offset++; // ...
                        break;
                    }
                }
                if (neg)
                {
                    result = -result;
                }
                return result;
            }
        }


        public void Close()
        {
            if (null != cstm)
            {
                cstm.Close();
            }
        }

        public void Dispose()
        {
            Close();
        }


        string[] agzipfilenames;
        int currentfileindex = 0;
        int buffersize = -1;
        System.IO.Stream cstm;
        byte[] linebuf;
        int defaultvalueint = 0;

        void loadnextfile()
        {
            if (null != cstm)
            {
                cstm.Close();
            }
            cstm = null;
            for (; ; )
            {
                if (currentfileindex < agzipfilenames.Length)
                {
                    agzipfilenames[currentfileindex] = agzipfilenames[currentfileindex].Trim();
                    if (agzipfilenames[currentfileindex].Length == 0)
                    {
                        currentfileindex++;
                        continue;
                    }
                    System.IO.Stream stm;
                    try
                    {
                        if (-1 != buffersize)
                        {
                            stm = new System.IO.FileStream(agzipfilenames[currentfileindex], System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, buffersize);
                        }
                        else
                        {
                            stm = new System.IO.FileStream(agzipfilenames[currentfileindex], System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
                        }
                        if (agzipfilenames[currentfileindex].EndsWith(".txt"))
                        {
                            cstm = stm;
                        }
                        else
                        {
                            cstm = new System.IO.Compression.GZipStream(stm, System.IO.Compression.CompressionMode.Decompress);
                        }
                    }
                    catch (Exception e)
                    {
                        MapReduceUtils.LogLine("GzipFastLoader unable to load file '" + agzipfilenames[currentfileindex] + "': " + e.ToString() + " ((skipping to next file))");
                        currentfileindex++;
                        continue;
                    }
                    currentfileindex++;
                }
                break;
            }
        }

    }


    public class IOUtils
    {
        public static string SafeTextPath(string s)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char ch in s)
            {
                if (sb.Length >= 150)
                {
                    sb.Append('`');
                    sb.Append(s.GetHashCode());
                    break;
                }
                if ('.' == ch)
                {
                    if (0 == ch)
                    {
                        sb.Append("%2E");
                        continue;
                    }
                }
                if (!char.IsLetterOrDigit(ch)
                    && '_' != ch
                    && '-' != ch
                    && '.' != ch)
                {
                    sb.Append('%');
                    if (ch > 0xFF)
                    {
                        sb.Append('u');
                        sb.Append(((int)ch).ToString().PadLeft(4, '0'));
                    }
                    else
                    {
                        sb.Append(((int)ch).ToString().PadLeft(2, '0'));
                    }
                }
                else
                {
                    sb.Append(ch);
                }
            }
            if (0 == sb.Length)
            {
                return "_";
            }
            return sb.ToString();
        }

        private static string tempdir = null;
        public static string GetTempDirectory()
        {
            if (tempdir == null)
            {
                tempdir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\usertemp";
                if (!System.IO.Directory.Exists(tempdir))
                {
                    System.IO.Directory.CreateDirectory(tempdir);
                }
            }
            return tempdir;
        }


        private static Random _rrt = new Random(unchecked(
            System.Threading.Thread.CurrentThread.ManagedThreadId
            + DateTime.Now.Millisecond));
        public static int RealRetryTimeout(int timeout)
        {
            if (timeout <= 3)
            {
                return timeout;
            }
            lock (_rrt)
            {
                return _rrt.Next(timeout / 4, timeout + 1);
            }
        }

    }


    public class NetUtils
    {

        public class ActiveConnection
        {
            public string Protocol;
            public string LocalAddress;
            public int LocalPort;
            public string ForeignAddress;
            public int ForeignPort;
            public string State;

            public override string ToString()
            {
                string slp = "";
                if (LocalPort >= 0)
                {
                    slp = ":" + LocalPort;
                }
                string sfp = "";
                if (ForeignPort >= 0)
                {
                    sfp = ":" + ForeignPort;
                }
                return Protocol
                    + "\t" + LocalAddress + slp
                    + "\t" + ForeignAddress + sfp
                    + "\t" + State;
            }

            public override int GetHashCode()
            {
                return unchecked(Protocol.GetHashCode()
                    + LocalAddress.GetHashCode()
                    + LocalPort.GetHashCode()
                    + ForeignAddress.GetHashCode()
                    + ForeignPort.GetHashCode()
                    + State.GetHashCode());

            }

            public override bool Equals(object obj)
            {
                ActiveConnection ac = obj as ActiveConnection;
                if (null == ac)
                {
                    return false;
                }
                return Equals(ac);
            }

            public bool Equals(ActiveConnection that)
            {
                return this.Protocol == that.Protocol
                    && this.LocalAddress == that.LocalAddress
                    && this.LocalPort == that.LocalPort
                    && this.ForeignAddress == that.ForeignAddress
                    && this.ForeignPort == that.ForeignPort
                    && this.State == that.State;
            }

        }

        public static ActiveConnection[] GetActiveConnections()
        {
            List<ActiveConnection> results = new List<ActiveConnection>();
            string[] lines = Exec.Shell("netstat -n").Split(new char[] { '\r', '\n' },
                StringSplitOptions.RemoveEmptyEntries);
            char[] sp = new char[] { ' ', '\t' };
            bool StartedActiveConnections = false;
            bool StartedProtoHeader = false;
            foreach (string line in lines)
            {
                if (!StartedActiveConnections)
                {
                    if (line.StartsWith("Active Connections"))
                    {
                        StartedActiveConnections = true;
                    }
                }
                else if(!StartedProtoHeader)
                {
                    string tline = line.Trim();
                    if (tline.StartsWith("Proto"))
                    {
                        StartedProtoHeader = true;
                    }
                }
                else
                {
                    if (0 == line.Length
                        || (' ' != line[0] && '\t' != line[0]))
                    {
                        break;
                    }
                    string[] parts = line.Split(sp, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length < 4)
                    {
                        break;
                    }
                    ActiveConnection ac = new ActiveConnection();
                    ac.Protocol = parts[0];
                    GetAddressParts(parts[1], out ac.LocalAddress, out ac.LocalPort);
                    GetAddressParts(parts[2], out ac.ForeignAddress, out ac.ForeignPort);
                    ac.State = parts[3];
                    results.Add(ac);
                }

            }
            return results.ToArray();
        }


        public static void GetAddressParts(string input, out string host, out int port)
        {
            int ilc = input.LastIndexOf(':');
            string sport = "";
            if (-1 != ilc)
            {
                host = input.Substring(0, ilc);
                sport = input.Substring(ilc + 1);
            }
            else
            {
                host = input;
            }
            if (!int.TryParse(sport, out port))
            {
                port = -1;
            }
        }

    }


    public class MemoryUtils
    {
        static int _deffbsz = 0;

        public static int DefaultFileBufferSize
        {
            get
            {
                return 0x1000;
            }
        }


        static int _ncpus = 0;

        public static int NumberOfProcessors
        {
            get
            {
                if (_ncpus < 1)
                {
                    try
                    {
                        _ncpus = int.Parse(Environment.GetEnvironmentVariable("NUMBER_OF_PROCESSORS"));
                    }
                    catch
                    {
                        _ncpus = 1;
                    }
                }
                return _ncpus;
            }
        }
    }


    public class MapReduceUtils
    {
        internal static System.Threading.Mutex logmutex = new System.Threading.Mutex(false, "do5mrlog");

        public static void LogLine(string line)
        {
            try
            {
                logmutex.WaitOne();
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            try
            {
                System.IO.StreamWriter fstm = System.IO.File.AppendText("do5mapreduce.txt");
                string build = "";
                try
                {
                    System.Reflection.Assembly asm = System.Reflection.Assembly.GetExecutingAssembly();
                    System.Reflection.AssemblyName an = asm.GetName();
                    int bn = an.Version.Build;
                    int rv = an.Version.Revision;
                    build = "(do5." + bn.ToString() + "." + rv.ToString() + ") ";
                }
                catch
                {
                }
                fstm.WriteLine("[{0} {1}ms] {2}{3}", System.DateTime.Now.ToString(), System.DateTime.Now.Millisecond, build, line);
                fstm.Close();
            }
            finally
            {
                logmutex.ReleaseMutex();
            }
        }
    }


    public abstract class LoadOutput
    {
        public abstract void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength);

        public void Add(ByteSlice key, ByteSlice value)
        {
            Add(key.buf, key.offset, key.length, value.buf, value.offset, value.length);
        }

        public void Add(IList<byte> key, IList<byte> value)
        {
            Add(key, 0, key.Count, value, 0, value.Count);
        }
    }


    public interface IBeforeLoad
    {
        void OnBeforeLoad(LoadOutput output);
    }


    public abstract class MapOutput
    {
        public abstract void Add(IList<byte> keybuf, int keyoffset, int keylength, IList<byte> valuebuf, int valueoffset, int valuelength);

        public void Add(ByteSlice key, ByteSlice value)
        {
            Add(key.buf, key.offset, key.length, value.buf, value.offset, value.length);
        }

        public void Add(IList<byte> key, IList<byte> value)
        {
            Add(key, 0, key.Count, value, 0, value.Count);
        }
        
		public void Add(string key, string value)
		{
            Add(ByteSlice.PreparePaddedStringUTF8(key, StaticGlobals.DSpace_KeyLength), ByteSlice.Prepare(value));
		}

        public void Add(recordset key, recordset value)
        {
            if (key.ContainsString)
            {
                throw new Exception("Key recordset cannot contain string.");
            }
           
            Add(key.ToByteSlice(StaticGlobals.DSpace_KeyLength), value.ToByteSlice());
        }

        public void Add(recordset key, mstring value)
        {
            if (key.ContainsString)
            {
                throw new Exception("Key recordset cannot contain string.");
            }

            Add(key.ToByteSlice(StaticGlobals.DSpace_KeyLength), value.ToByteSlice());
        }
        
        public void Add(mstring key, mstring value)
        {
            ByteSlice bKey = key.ToByteSlice(StaticGlobals.DSpace_KeyLength);
            ByteSlice bValue = value.ToByteSlice();
            Add(bKey, bValue);
        }
        
        public void Add(mstring key, recordset value)
        {
            Add(key.ToByteSlice(StaticGlobals.DSpace_KeyLength), value.ToByteSlice());
        }        
    }

    public abstract class MapInput
    {
        public string Name; // Not necessarily a file name; for tracking purposes.
        public System.IO.Stream Stream; // Relative to this input; may be a logical EndOfStream!

        // Position from the original source file as loaded into DFS.
        public abstract long Position
        {
            get;
        }

        // Length of this input, in bytes. e.g. number of bytes that can be read from Stream.
        public abstract long Length
        {
            get;
        }

        public abstract long Fixup(long position, long length, byte[] buf, int bufoffset);
        public abstract void Reopen(long position);
    }


    // OnMap may be called several times (sequentially) if multiple inputs for one slave.
    // inputname is not necessarily a file name.
    // input limitations: no seeking, no writing, no async methods, no wait handle, no close/dispose.
    public interface IMap
    {
        void OnMap(MapInput input, MapOutput output);
    }


    public interface IRemote
    {
        void OnRemote();
        int GetOutputFileCount(int n, IList<long> appendsizes);
    }


}
