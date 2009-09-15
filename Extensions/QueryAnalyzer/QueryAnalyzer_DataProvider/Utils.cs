using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_DataProvider
{
    public class Utils
    {
        public static Int16 BytesToInt16(IList<byte> x, int offset)
        {
            Int16 result = 0;
            result |= (Int16)((UInt16)x[offset + 0] << 8);
            result |= (Int16)(UInt16)x[offset + 1];
            return result;
        }

        public static Int16 BytesToInt16(IList<byte> x)
        {
            return BytesToInt16(x, 0);
        }

        public static UInt16 BytesToUInt16(IList<byte> x, int offset)
        {
            UInt16 result = 0;
            result |= (UInt16)(x[offset + 0] << 8);
            result |= (UInt16)x[offset + 1];
            return result;
        }

        public static UInt16 BytesToUInt16(IList<byte> x)
        {
            return BytesToUInt16(x, 0);
        }

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

        public static long BytesToLong(IList<byte> x)
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

        public static Int32 ToInt32(UInt32 x)
        {
            return (int)(x - int.MaxValue - 1);
        }

        public static long ToInt64(UInt64 x)
        {
            return (long)(x - long.MaxValue - 1);
        }

        public static void Int64ToBytes(long x, byte[] resultbuf, int bufoffset)
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

    }
}
