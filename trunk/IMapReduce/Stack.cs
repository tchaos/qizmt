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
    public struct Stack
    {
        [ThreadStatic]
        internal static byte[] _Buffer;
        [ThreadStatic]
        internal static int CurrentPos;
        [ThreadStatic]
        internal static int OwnSize;

        internal const int DefaultBufferSize = 100000;

        static byte[] OneBuffer;

        internal static byte[] Buffer
        {
            get
            {
                if (_Buffer == null)
                {
                    if (0 == OwnSize)
                    {
                        lock ("StackOneBuffer{27CF77C6-E0AD-4c83-83B8-946D2B1B6D50}")
                        {
                            if (Stack.OneBuffer == null)
                            {
                                Stack.OneBuffer = new byte[DefaultBufferSize];
                            }
                        }
                        Stack._Buffer = Stack.OneBuffer;
                    }
                    else
                    {
                        Stack._Buffer = new byte[Stack.OwnSize];
                    }
                    Stack.CurrentPos = 0;
                }
                return Stack._Buffer;
            }
        }

        public static void OwnThreadStack(int buffersize)
        {
#if DEBUG
            if (Stack.OwnSize != 0)
            {
                throw new Exception("DEBUG:  Stack.OwnThreadStack: (Stack.OwnSize != 0)");
            }
            if (Stack._Buffer != null)
            {
                throw new Exception("DEBUG:  Stack.OwnThreadStack: (_Buffer != null)");
            }
#endif
            Stack.OwnSize = buffersize;
        }

        public static void OwnThreadStack()
        {
            OwnThreadStack(DefaultBufferSize);
        }

        public static void ResetStack()
        {
            Stack.CurrentPos = 0;
        }

        public static void ResetStack(int context)
        {
            Stack.CurrentPos = context;
        }

        public static int StartStack()
        {
            return Stack.CurrentPos;
        }

        public static void CheckStack(int delta)
        {          
            if (delta == 0)
            {
                return;
            }

            int lastIndex = Stack.CurrentPos + delta - 1;

            if (lastIndex < 0)
            {
                throw new Exception("Stack.Buffer cannot store your buffer.  CurrentPos=" + CurrentPos.ToString() + " delta=" + delta.ToString());
            }

            if (lastIndex < Stack.Buffer.Length)
            {
                return;
            }

            if (Stack.Buffer.Length == Int32.MaxValue)
            {
                throw new Exception("Cannot expand Stack.Buffer.  It has reached its limit size=" + Buffer.Length.ToString());
            }

            int newLength = Stack.Buffer.Length;

            for (; ; )
            {
                newLength = newLength * 2;

                if (lastIndex < newLength)
                {
                    break;
                }

                if (newLength < 0)
                {
                    newLength = Int32.MaxValue;
                    break;
                }
            }

            try
            {
                byte[] newBuffer = new byte[newLength];

                for (int i = 0; i < Buffer.Length; i++)
                {
                    newBuffer[i] = _Buffer[i];
                }

                Stack._Buffer = newBuffer;
            }
            catch (OutOfMemoryException)
            {
                throw new Exception("mstring cannot accommodate your string operations.  Please use C# string or StringBuilder instead.");
            }
        }

        public static int PutString(string s)
        {
            int delta = s.Length * 2; // Encoding.Unicode.GetByteCount(s);
            Stack.CheckStack(delta);

            Encoding.Unicode.GetBytes(s, 0, s.Length, Stack.Buffer, Stack.CurrentPos);

            Stack.MovePos(delta);

            return delta;
        }

        public static int PutChar(char c)
        {
            byte b0 = 0x0;
            byte b1 = 0x0;
            UTFConverter.ConvertCodeValueToBytesUTF16((int)c, ref b0, ref b1);
            Stack.PutByte(b0);
            Stack.PutByte(b1);
            return 2;
        }

        public static int PutString(mstring s)
        {
            Stack.PutBytes(Stack.Buffer, s.StartIndex, s.ByteCount);
            return s.ByteCount;
        }

        public static void PutBytes(IList<byte> bytes, int byteIndex, int byteCount)
        {
            Stack.CheckStack(byteCount);

            for (int i = byteIndex; i < byteIndex + byteCount; i++)
            {
                Stack.Buffer[Stack.CurrentPos] = bytes[i];
                Stack.MovePos(1);
            }
        }

        public static void PutByte(byte b)
        {
            Stack.CheckStack(1);
            Stack.Buffer[Stack.CurrentPos] = b;
            Stack.MovePos(1);
        }

        public static void PutInt32(Int32 x)
        {
            Stack.CheckStack(4);
            Entry.ToBytes(x, Stack.Buffer, Stack.CurrentPos);
            Stack.MovePos(4);
        }

        public static void PutInt64(Int64 x)
        {
            Stack.CheckStack(8);
            Entry.LongToBytes(x, Stack.Buffer, Stack.CurrentPos);
            Stack.MovePos(8);
        }

        public static void PutBytes(byte[] bytes)
        {
            Stack.PutBytes(bytes, 0, bytes.Length);
        }

        public static void MovePos(int delta)
        {
            Stack.CurrentPos += delta;
        }

        internal static void ReplaceByte(int index, byte b)
        {
            if (index < Stack.CurrentPos)
            {
                Stack.Buffer[index] = b;
            }
            else
            {
                throw new ArgumentException("index supplied is not occupied yet.  Can't replace byte.");
            }
        }

        internal static int ReplaceChar(int index, char c)
        {
            if (index + 1 < Stack.CurrentPos)
            {
                byte b0 = 0x0;
                byte b1 = 0x0;
                UTFConverter.ConvertCodeValueToBytesUTF16((int)c, ref b0, ref b1);
                Stack.ReplaceByte(index, b0);
                Stack.ReplaceByte(index + 1, b1);
                return 2;
            }
            else
            {
                throw new ArgumentException("index supplied is not occupied yet.  Can't replace byte.");
            }           
        }

        internal static void ReplaceBytes(int index, byte[] bytes, int byteIndex, int byteCount)
        {
            if (index + byteCount - 1 < Stack.CurrentPos)
            {         
                for (int i = 0; i < byteCount; i++)
                {
                    Stack.ReplaceByte(index + i, bytes[byteIndex + i]);
                }                
            }
            else
            {
                throw new ArgumentException("index supplied is not occupied yet.  Can't replace byte.");
            }
        }

        internal static int SetChar(int index, char c)
        {
            if (index + 1 < Stack.CurrentPos)
            {
                return ReplaceChar(index, c);
            }
            else
            {
                return PutChar(c);
            }
        }
    }
}
