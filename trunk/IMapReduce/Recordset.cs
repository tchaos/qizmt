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
    public struct recordset
    {
        [ThreadStatic]
        private static List<List<byte>> Buffers;
        [ThreadStatic]
        private static int CurrentBufPos;
        [ThreadStatic]
        private static bool OwnBuffers;

        private static List<List<byte>> OneBuffers;

        private int CurrentGetPos;
        public bool ContainsString;
        internal List<byte> Buffer;
        List<char> Dummy;

        private static List<byte> GetBuffer()
        {
            if (Buffers == null)
            {
                if (!OwnBuffers)
                {
                    lock ("recordsetOneBuffers{9BE31A43-0FA6-4f13-9CCA-203B5B24E62B}")
                    {
                        if (null == OneBuffers)
                        {
                            OneBuffers = new List<List<byte>>();
                        }
                    }
                    Buffers = OneBuffers;
                }
                else
                {
                    Buffers = new List<List<byte>>();
                }
                CurrentBufPos = 0;               
            }

            if (CurrentBufPos > Buffers.Count - 1)
            {
                Buffers.Add(new List<byte>());
            }
            else
            {
                Buffers[CurrentBufPos].Clear();
            }

            return Buffers[CurrentBufPos++];
        }

        public static void OwnThreadBuffers()
        {
#if DEBUG
            if (OwnBuffers)
            {
                throw new Exception("DEBUG:  Stack.OwnThreadBuffers: (OwnBuffers)");
            }
            if (Buffers != null)
            {
                throw new Exception("DEBUG:  recordset.OwnThreadBuffers: (Buffers != null)");
            }
#endif
            OwnBuffers = true;
        }

        public static recordset Prepare()
        {
            recordset rs;           
            rs.CurrentGetPos = 0;
            rs.ContainsString = false;
            rs.Buffer = GetBuffer();

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                rs.Dummy = new List<char>();
            }
            else
            {
                rs.Dummy = null;
            }

            return rs;
        }

        public static recordset PrepareRow(ByteSlice b)
        {
            recordset rs = recordset.Prepare();
            rs.PutByteSlice(b);
            return rs;
        }

        public static recordset Prepare(ByteSlice b)
        {
            recordset rs = recordset.Prepare();

            for (int i = 0; i < b.Length ; i++)
            {
                rs.Buffer.Add(b[i]);
            }
                                 
            return rs;
        }

        public ByteSlice ToRow()
        {
            return ByteSlice.Prepare(Buffer, 0, Buffer.Count);
        }

        public ByteSlice ToByteSlice()
        {            
            if (Buffer.Count == 0)
            {
                return ByteSlice.Prepare();
            }

            return ByteSlice.Prepare(Buffer, 0, Buffer.Count);            
        }

        public ByteSlice ToByteSlice(int size)
        {            
            if (size < Buffer.Count)
            {
                throw new ArgumentException("Your recordset is too big.  Your recordset has " + Buffer.Count.ToString() + " bytes and key length = " + size.ToString() + " bytes.");
            }

            Pad(size);
            return ByteSlice.Prepare(Buffer, 0, size);
        }

        public static void ResetBuffers()
        {
            CurrentBufPos = 0;
        }

        public static void ResetBuffers(int context)
        {
            CurrentBufPos = context;
        }

        public static int StartBuffers()
        {
            return CurrentBufPos;
        }

        public int Length
        {
            get
            {
                return Buffer.Count;
            }
        }

        public void PutInt32(Int32 x)
        {
            Entry.ToBytesAppend(x, Buffer);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(Int32) " + x.ToString() + "]");
            }
        }

        public void PutInt(Int32 x)
        {
            PutInt32(x);
        }

        public void PutInt16(Int16 x)
        {          
            Entry.Int16ToBytesAppend(x, Buffer);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(Int16) " + x.ToString() + "]");
            }
        }

        public void PutShort(Int16 x)
        {
            PutInt16(x);
        }

        public void PutUInt16(UInt16 x)
        {          
            Entry.UInt16ToBytesAppend(x, Buffer);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(UInt16) " + x.ToString() + "]");
            }
        }

        public void PutUShort(UInt16 x)
        {
            PutUInt16(x);
        }

        public void PutUInt32(UInt32 x)
        {
            Entry.UInt32ToBytesAppend(x, Buffer);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(UInt32) " + x.ToString() + "]");
            }
        }

        public void PutUInt(UInt32 x)
        {
            PutUInt32(x);
        }

        public void PutUInt64(UInt64 x)
        {
            Entry.ULongToBytesAppend(x, Buffer);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(UInt64) " + x.ToString() + "]");
            }
        }

        public void PutULong(UInt64 x)
        {
            PutUInt64(x);
        }

        private void PutToDummy(string s)
        {
            foreach (char c in s)
            {
                Dummy.Add(c);
            }
        }

        public void PutInt64(Int64 x)
        {
            Entry.ToBytesAppend64(x, Buffer);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(Int64) " + x.ToString() + "]");
            }
        }

        public void PutLong(Int64 x)
        {
            PutInt64(x);
        }

        public void PutDateTime(DateTime x)
        {
            Entry.ToBytesAppend64(x.Ticks, Buffer);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(DateTime) " + x.ToString() + "]");
            }
        }

        public void PutDouble(double x)
        {
            Entry.ToBytesAppendDouble(x, Buffer);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(Double) " + x.ToString() + "]");
            }
        }

        public void PutByte(byte x)
        {
            Buffer.Add(x);
        }

        public void PutBytes(IList<byte> bytes, int byteIndex, int byteCount)
        {
            for (int i = 0; i < byteCount; i++)
            {
                Buffer.Add(bytes[i + byteIndex]);
            }
        }

        public void PutByteSlice(ByteSlice bytes)
        {
            for (int i = 0; i < bytes.length; i++)
            {
                Buffer.Add(bytes[i]);
            }
        }

        public void PutString(mstring s)
        {          
            ContainsString = true;

            int endIndex = s.StartIndex + s.ByteCount - 1;

            for (int i = s.StartIndex; i <= endIndex; i++)
            {
                Buffer.Add(s.Buffer[i]);
            }

            Buffer.Add(0x0);
            Buffer.Add(0x0);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(mstring) " + s.ToString() + "]");
            }
        }

        public void PutString(string s)
        {
            mstring ms = mstring.Prepare(s);
            PutString(ms);
        }

        public void PutChar(char c)
        {
            byte b0 = 0x0;
            byte b1 = 0x0;
            UTFConverter.ConvertCodeValueToBytesUTF16((int)c, ref b0, ref b1);
            Buffer.Add(b0);
            Buffer.Add(b1);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(char) " + c + "]");
            }
        }

        public void PutBool(bool o)
        {           
            byte b = (byte)(o ? 0x1 : 0x2);
            Buffer.Add(b);

            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                PutToDummy("[(bool) " + o.ToString() + "]");
            }
        }

        public Int32 GetInt32()
        {
            int x = Entry.BytesToInt(Buffer, CurrentGetPos);
            CurrentGetPos += 4;
            return x;
        }

        public Int32 GetInt()
        {
            return GetInt32();
        }

        public UInt32 GetUInt32()
        {
            UInt32 x = Entry.BytesToUInt32(Buffer, CurrentGetPos);
            CurrentGetPos += 4;
            return x;
        }

        public UInt32 GetUInt()
        {
            return GetUInt32();
        }

        public Int16 GetInt16()
        {
            Int16 x = Entry.BytesToInt16(Buffer, CurrentGetPos);
            CurrentGetPos += 2;
            return x;
        }

        public Int16 GetShort()
        {
            return GetInt16();
        }

        public UInt16 GetUInt16()
        {
            UInt16 x = Entry.BytesToUInt16(Buffer, CurrentGetPos);
            CurrentGetPos += 2;
            return x;
        }

        public UInt16 GetUShort()
        {
            return GetUInt16();
        }

        public Int64 GetInt64()
        {
            Int64 x = Entry.BytesToLong(Buffer, CurrentGetPos);
            CurrentGetPos += 8;
            return x;
        }

        public Int64 GetLong()
        {
            return GetInt64();
        }

        public DateTime GetDateTime()
        {
            long ticks = GetInt64();
            return new DateTime(ticks);
        }

        public UInt64 GetUInt64()
        {
            UInt64 x = Entry.BytesToULong(Buffer, CurrentGetPos);
            CurrentGetPos += 8;
            return x;
        }

        public UInt64 GetULong()
        {
            return GetUInt64();
        }

        public double GetDouble()
        {
            double x = Entry.BytesToDouble(Buffer, CurrentGetPos);
            CurrentGetPos += 9;
            return x;
        }

        public void GetBytes(IList<byte> buffer, int offset, int byteCount)
        {
            if (CurrentGetPos + byteCount > Buffer.Count)
            {
                throw new ArgumentException("byteCount has exceeded the number of bytes to read from.");
            }

            for (int i = 0; i < byteCount; i++)
            {
                buffer[i + offset] = Buffer[CurrentGetPos++];
            }
        }

        public mstring GetString()
        {
            int endIndex = Buffer.Count - 1;
            int strEnd = -1;

            for (int i = CurrentGetPos; i <= endIndex; i = i + 2)
            {
                if (i + 1 > endIndex)
                {
                    throw new Exception("UTF-16 encoded string is missing some bytes.");
                }

                if (Buffer[i] == 0x0 && Buffer[i+1] == 0x0)
                {
                    strEnd = i;
                    break;
                }
            }

            if (strEnd > -1)
            {
                mstring ms = mstring.Prepare(Buffer, CurrentGetPos, strEnd - CurrentGetPos);
                CurrentGetPos = strEnd + 2;
                return ms;
            }
            else
            {
                throw new Exception("String end is not found.");
            }     
        }

        public char GetChar()
        {
            char c = (char)UTFConverter.GetCodeValueUTF16(Buffer[CurrentGetPos], Buffer[CurrentGetPos + 1]);           
            CurrentGetPos += 2;
            return c;
        }

        public bool GetBool()
        {
            return (Buffer[CurrentGetPos++] == 0x1);          
        }

        public void Pad(int size)
        {
            if (size > Buffer.Count)
            {
                for (int i = Buffer.Count; i < size; i++)
                {
                    Buffer.Add(0x0);
                }
            }
        }

        public override string ToString()
        {
            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                return new String(Dummy.ToArray());
            }
            else
            {
                return "";
            }
        }
    }
}
