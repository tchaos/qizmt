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
    public struct mstring
    {
        internal int StartIndex;
        internal int ByteCount;       
        private int CurrentDelPos;
        const int DEFAULT_DEL_POS = -1;

        public int Length
        {
            get
            {
                return ByteCount / 2;
            }
        }

        public static mstring Prepare()
        {
            mstring ms;
            ms.StartIndex = 0;
            ms.ByteCount = 0;
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            return ms;
        }

        public static mstring Prepare(string s)
        {
            if (s == null || s.Length == 0)
            {
                return mstring.Prepare();
            }

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.ByteCount = Stack.PutString(s);
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            return ms;
        }

        public static mstring Prepare(char c)
        {
            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.ByteCount = Stack.PutChar(c);
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            return ms;
        }

        public static mstring Prepare(ByteSlice b)
        {
            if (b.Length == 0)
            {
                return mstring.Prepare();
            }

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.ByteCount = UTFConverter.ConvertUTF8ToUTF16(b.buf, b.offset, b.Length, true);
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            return ms;
        }

        public static mstring PrepareUTF16(ByteSlice b)
        {
            if (b.length == 0)
            {
                return mstring.Prepare();
            }

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.ByteCount = b.length;
            Stack.PutBytes(b.buf, b.offset, b.length);
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            return ms;
        }

        public static mstring Prepare(IList<byte> bytes, int byteIndex, int byteCount)
        {
            if (byteCount == 0)
            {
                return mstring.Prepare();
            }

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.ByteCount = byteCount;
            Stack.PutBytes(bytes, byteIndex, byteCount);
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            return ms;
        }

        public static mstring Prepare(Int32 x)
        {
            bool addOne = false;
            int byteCount = 0;

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.CurrentDelPos = DEFAULT_DEL_POS;

            if (x == Int32.MinValue)
            {
                x++;
                addOne = true;
            }

            if (x < 0)
            {
                x = x * (-1);
                byteCount += Stack.PutChar('-');
            }

            double d = x / (double)10;
            int n = 0;

            while (d >= 1)
            {
                d = d / (double)10;
                ++n;
            }

            for (int i = n; i >= 0; i--)
            {
                int div = (int)Math.Pow(10, i);
                int rem = 0;
                int quo = (int)Math.DivRem(x, div, out rem);

                if (i == 0 && addOne)
                {
                    quo++;
                }

                x = rem;

                char ch = (char)('0' + quo);
               
                byteCount += Stack.PutChar(ch);
            }

            ms.ByteCount = byteCount;
            return ms;
        }

        public static mstring Prepare(UInt32 x)
        {
            //bool addOne = false;
            int byteCount = 0;

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            /*
            if (x == Int32.MinValue)
            {
                x++;
                addOne = true;
            }*/
            
            double d = x / (double)10;
            int n = 0;

            while (d >= 1)
            {
                d = d / (double)10;
                ++n;
            }

            for (int i = n; i >= 0; i--)
            {
                uint div = (uint)Math.Pow(10, i);                
                uint quo = x / div;  //(int)Math.DivRem(x, div, out rem);
                uint rem = x - div * quo;

                /*if (i == 0 && addOne)
                {
                    quo++;
                }*/

                x = rem;

                char ch = (char)('0' + quo);

                byteCount += Stack.PutChar(ch);
            }

            ms.ByteCount = byteCount;
            return ms;
        }

        public static mstring Prepare(Int16 x)
        {
            bool addOne = false;
            int byteCount = 0;

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.CurrentDelPos = DEFAULT_DEL_POS;

            if (x == Int16.MinValue)
            {
                x++;
                addOne = true;
            }

            if (x < 0)
            {
                x = (Int16)(x * (-1));
                byteCount += Stack.PutChar('-');
            }

            double d = x / (double)10;
            int n = 0;

            while (d >= 1)
            {
                d = d / (double)10;
                ++n;
            }

            for (int i = n; i >= 0; i--)
            {
                int div = (int)Math.Pow(10, i);
                int rem = 0;
                int quo = (int)Math.DivRem(x, div, out rem);

                if (i == 0 && addOne)
                {
                    quo++;
                }

                x = (Int16)rem;

                char ch = (char)('0' + quo);

                byteCount += Stack.PutChar(ch);
            }

            ms.ByteCount = byteCount;
            return ms;
        }

        public static mstring Prepare(UInt16 x)
        {
            int byteCount = 0;

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.CurrentDelPos = DEFAULT_DEL_POS;           

            double d = x / (double)10;
            int n = 0;

            while (d >= 1)
            {
                d = d / (double)10;
                ++n;
            }

            for (int i = n; i >= 0; i--)
            {
                int div = (int)Math.Pow(10, i);
                int quo = x / div;
                int rem = x - div * quo;

                x = (UInt16)rem;

                char ch = (char)('0' + quo);

                byteCount += Stack.PutChar(ch);
            }

            ms.ByteCount = byteCount;
            return ms;
        }

        public static mstring Prepare(Int64 x)
        {
            bool addOne = false;
            int byteCount = 0;

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.CurrentDelPos = DEFAULT_DEL_POS;

            if (x == Int64.MinValue)
            {
                x++;
                addOne = true;
            }

            if (x < 0)
            {               
                x = x * (-1);
                byteCount += Stack.PutChar('-');
            }

            decimal d = x / (decimal)10;
            int n = 0;

            while (d >= 1)
            {
                d = d / (decimal)10;
                ++n;
            }

            for (int i = n; i >= 0; i--)
            {
                long div = (long)Math.Pow(10, i);
                long rem = 0;
                int digit = (int)Math.DivRem(x, div, out rem);

                if (i == 0 && addOne)
                {
                    digit++;
                }

                x = rem;

                char ch = (char)('0' + digit);

                byteCount += Stack.PutChar(ch);
            }

            ms.ByteCount = byteCount;
            return ms;
        }

        public static mstring Prepare(UInt64 x)
        {
            //bool addOne = false;
            int byteCount = 0;

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.CurrentDelPos = DEFAULT_DEL_POS;

            /*if (x == Int64.MinValue)
            {
                x++;
                addOne = true;
            }

            if (x < 0)
            {
                x = x * (-1);
                byteCount += Stack.PutChar('-');
            }*/

            decimal d = x / (decimal)10;
            int n = 0;

            while (d >= 1)
            {
                d = d / (decimal)10;
                ++n;
            }

            for (int i = n; i >= 0; i--)
            {
                ulong div = (ulong)Math.Pow(10, i);                
                ulong digit = x / div;
                ulong rem = x - digit * div;

                /*
                if (i == 0 && addOne)
                {
                    digit++;
                }*/

                x = rem;

                char ch = (char)('0' + digit);

                byteCount += Stack.PutChar(ch);
            }

            ms.ByteCount = byteCount;
            return ms;
        }

        internal static mstring Prepare(int startIndex, int byteCount)
        {
            if (byteCount == 0)
            {
                return mstring.Prepare();
            }

            mstring ms;
            ms.StartIndex = startIndex;
            ms.ByteCount = byteCount;
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            return ms;
        }

        public void ResetGetPosition()
        {
            CurrentDelPos = DEFAULT_DEL_POS;
        }

        public ByteSlice ToByteSlice(bool strictConversion)
        {
            if (ByteCount == 0)
            {
                return ByteSlice.Prepare();
            }

            int offset = Stack.CurrentPos;
            int length = UTFConverter.ConvertUTF16ToUTF8(Stack.Buffer, StartIndex, ByteCount, strictConversion);
            return ByteSlice.Prepare(Stack.Buffer, offset, length);
        }

        public ByteSlice ToByteSlice()
        {
            return ToByteSlice(true);
        }

        public ByteSlice ToByteSlice(int size, bool strictConversion)
        {
            int offset = Stack.CurrentPos;
            int length = UTFConverter.ConvertUTF16ToUTF8(Stack.Buffer, StartIndex, ByteCount, strictConversion);
            byte[] buf = ByteSlice.PreparePaddedUTF8Bytes(Stack.Buffer, offset, length, size);
            return ByteSlice.Prepare(buf, 0, size);
        }

        public ByteSlice ToByteSlice(int size)
        {
            return ToByteSlice(size, true);
        }

        public ByteSlice ToByteSliceUTF16()
        {
            return ByteSlice.Prepare(this.Buffer, this.StartIndex, this.ByteCount);
        }

        internal byte[] Buffer
        {
            get
            {
                return Stack.Buffer;
            }
        }

        public static mstring Copy(mstring s)
        {
            if (s.ByteCount == 0)
            {
                return mstring.Prepare();
            }

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;            
            ms.ByteCount = Stack.PutString(s);
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            return ms;
        }

        public override string ToString()
        {
            return Encoding.Unicode.GetString(Stack.Buffer, StartIndex, ByteCount);
        }

        public Int32 ToInt32()
        {
            int si = StartIndex;
            return GetNextInt32UTF16(Stack.Buffer, ref si, si + ByteCount - 1);
        }

        public Int32 ToInt()
        {
            return ToInt32();
        }

        public UInt32 ToUInt32()
        {
            int si = StartIndex;
            return GetNextUInt32UTF16(Stack.Buffer, ref si, si + ByteCount - 1);
        }

        public UInt32 ToUInt()
        {
            return ToUInt32();
        }

        public Int16 ToInt16()
        {
            int si = StartIndex;
            return GetNextInt16UTF16(Stack.Buffer, ref si, si + ByteCount - 1);
        }

        public UInt16 ToUInt16()
        {
            int si = StartIndex;
            return GetNextUInt16UTF16(Stack.Buffer, ref si, si + ByteCount - 1);
        }

        public Int16 ToShort()
        {
            return ToInt16();
        }

        public UInt16 ToUShort()
        {
            return ToUInt16();
        }

        public Int64 ToInt64()
        {
            int si = StartIndex;
            return GetNextInt64UTF16(Stack.Buffer, ref si, si + ByteCount - 1);
        }

        public Int64 ToLong()
        {
            return ToInt64();
        }

        public UInt64 ToUInt64()
        {
            int si = StartIndex;
            return GetNextUInt64UTF16(Stack.Buffer, ref si, si + ByteCount - 1);
        }

        public UInt64 ToULong()
        {
            return ToUInt64();
        }

        public double ToDouble()
        {
            int si = StartIndex;
            return GetNextDoubleUTF16(Stack.Buffer, ref si, si + ByteCount - 1);
        }
        /*
        public mstring Pad(int size)
        {
            if (size % 2 == 1)
            {
                throw new Exception("UTF-16 encoded string must have byteCount mutiple of 2.");
            }

            if (size == 0)
            {
                return mstring.Prepare();
            }

            mstring ms;

            if (ByteCount > size)
            {
                ms = mstring.Prepare(Stack.Buffer, StartIndex, size);

                if (UTFConverter.IsHighSurrogate(Stack.Buffer[ms.StartIndex + size - 2], Stack.Buffer[ms.StartIndex + size - 1]))
                {
                    Stack.Buffer[ms.StartIndex + size - 2] = 0x0;
                    Stack.Buffer[ms.StartIndex + size - 1] = 0x0;
                }
            }
            else
            {     
                if (ByteCount > 0)
                {
                    ms = mstring.Copy(this);                   
                }
                else
                {
                    ms = mstring.Prepare("\0");
                }

                for (int i = ms.ByteCount; i < size; i++)
                {
                    Stack.PutByte(0x0);
                    ++ms.ByteCount;
                }
            }

            return ms;
        }*/
        
        public mstring MPadRight(int totalLength, char paddingChar)
        {
            if (totalLength <= Length)
            {
                return this;
            }

            if (Stack.CurrentPos != StartIndex + ByteCount)
            {
                if (ByteCount > 0)
                {
                    mstring ms = mstring.Copy(this);
                    StartIndex = ms.StartIndex;
                    ByteCount = ms.ByteCount;
                }
                else
                {
                    StartIndex = Stack.CurrentPos;
                }                               
            }

            byte b0 = 0x0;
            byte b1 = 0x0;
            UTFConverter.ConvertCodeValueToBytesUTF16((int)paddingChar, ref b0, ref b1);
            int delta = totalLength - Length;

            for (int i = 0; i < delta; i++)
            {
                Stack.PutByte(b0);
                Stack.PutByte(b1);
                ByteCount += 2;
            }

            return this;
        }

        public mstring PadRightM(int totalLength, char paddingChar)
        {
            return MPadRight(totalLength, paddingChar);
        }

        public mstring MPadLeft(int totalLength, char paddingChar)
        {            
            if (totalLength <= Length)
            {
                return this;
            }

            int delta = totalLength - Length;
            int startIndex = Stack.CurrentPos;
            int byteCount = 0;
            byte b0 = 0x0;
            byte b1 = 0x0;
            UTFConverter.ConvertCodeValueToBytesUTF16((int)paddingChar, ref b0, ref b1);

            for (int i = 0; i < delta; i++)
            {
                Stack.PutByte(b0);
                Stack.PutByte(b1);
                byteCount += 2;
            }

            if (ByteCount > 0)
            {
                mstring ms = mstring.Copy(this);
                byteCount += ms.ByteCount;
            }

            StartIndex = startIndex;
            ByteCount = byteCount;
            return this;
        }

        public mstring PadLeftM(int totalLength, char paddingChar)
        {
            return MPadLeft(totalLength, paddingChar);
        }

        public mstring MReplace(char oldChar, char newChar)
        {
            if (ByteCount == 0)
            {
                return this;
            }

            int charIndex = StartIndex;
            byte b0 = 0x0;
            byte b1 = 0x0;
            UTFConverter.ConvertCodeValueToBytesUTF16((int)oldChar, ref b0, ref b1);
            int bytesLeft = ByteCount;

            for (; ; )
            {
                charIndex = IndexOf(b0, b1, Stack.Buffer, charIndex, bytesLeft);

                if (charIndex == -1)
                {
                    break;
                }

                Stack.ReplaceChar(charIndex, newChar);

                charIndex += 2;
                bytesLeft = ByteCount - charIndex + StartIndex;

                if (bytesLeft == 0)
                {
                    break;
                }
            }

            return this;
        }

        public mstring ReplaceM(char oldChar, char newChar)
        {
            return MReplace(oldChar, newChar);
        }
        
        private mstring MReplace_SameLength(ref mstring oldString, ref mstring newString)
        {
            if (oldString.ByteCount == 0)
            {
                throw new ArgumentException("The input parameter oldString cannot be of zero length.");
            }

            if (ByteCount == 0)
            {
                return this;
            }

            int stringIndex = IndexOf(oldString, Stack.Buffer, StartIndex, ByteCount);

            if (stringIndex != -1)
            {
                int endIndex = StartIndex + ByteCount - 1;
                int oEndIndex = oldString.StartIndex + oldString.ByteCount - 1;
                int nEndIndex = newString.StartIndex + newString.ByteCount - 1;

                if ((oldString.StartIndex >= StartIndex && oldString.StartIndex <= endIndex) || (oEndIndex >= StartIndex && oEndIndex <= endIndex))
                {
                    oldString = mstring.Copy(oldString);
                }

                if ((newString.StartIndex >= StartIndex && newString.StartIndex <= endIndex) || (nEndIndex >= StartIndex && nEndIndex <= endIndex))
                {
                    newString = mstring.Copy(newString);
                }

                int bytesLeft = ByteCount;

                for (; ;)
                {     
                    Stack.ReplaceBytes(stringIndex, Stack.Buffer, newString.StartIndex, newString.ByteCount);

                    stringIndex += oldString.ByteCount;
                    bytesLeft = ByteCount - stringIndex + StartIndex;

                    if (bytesLeft < oldString.ByteCount)
                    {
                        break;
                    }

                    stringIndex = IndexOf(oldString, Stack.Buffer, stringIndex, bytesLeft);

                    if (stringIndex == -1)
                    {
                        break;
                    }
                }
            }

            return this;
        }

        public mstring MReplace(ref mstring oldString, ref mstring newString)
        {
            if (oldString.ByteCount == 0)
            {
                throw new ArgumentException("The input parameter oldString cannot be of zero length.");
            }

            if (ByteCount == 0)
            {
                return this;
            }

            if (oldString.ByteCount == newString.ByteCount)
            {
                return MReplace_SameLength(ref oldString, ref newString);
            }

            int stringIndex = IndexOf(oldString, Stack.Buffer, StartIndex, ByteCount);

            if (stringIndex == -1)
            {
                return this;
            }

            int wordStart = StartIndex;
            int startIndex = Stack.CurrentPos;
            int byteCount = 0;
            int bytesLeft = ByteCount;

            for (; ;)
            {
                if (stringIndex > -1)
                {
                    Stack.PutBytes(Stack.Buffer, wordStart, stringIndex - wordStart);
                    byteCount += stringIndex - wordStart;

                    Stack.PutBytes(Stack.Buffer, newString.StartIndex, newString.ByteCount);
                    byteCount += newString.ByteCount;

                    wordStart = stringIndex + oldString.ByteCount;
                }
                else
                {
                    Stack.PutBytes(Stack.Buffer, wordStart, ByteCount - wordStart + StartIndex);
                    byteCount += ByteCount - wordStart + StartIndex;

                    break;
                }

                stringIndex += oldString.ByteCount;
                bytesLeft = ByteCount - stringIndex + StartIndex;               

                stringIndex = IndexOf(oldString, Stack.Buffer, stringIndex, bytesLeft);              
            }

            if (byteCount == 0)
            {
                startIndex = 0;
            }

            StartIndex = startIndex;
            ByteCount = byteCount;
            return this;
        }

        public mstring ReplaceM(ref mstring oldString, ref mstring newString)
        {
            return MReplace(ref oldString, ref newString);
        }

        public mstring MReplace(ref mstring oldString, string newString)
        {
            mstring ms = mstring.Prepare(newString);
            return MReplace(ref oldString, ref ms);
        }

        public mstring ReplaceM(ref mstring oldString, string newString)
        {
            return MReplace(ref oldString, newString);
        }

        public mstring MReplace(string oldString, ref mstring newString)
        {
            mstring ms = mstring.Prepare(oldString);
            return MReplace(ref ms, ref newString);
        }

        public mstring ReplaceM(string oldString, ref mstring newString)
        {
            return MReplace(oldString, ref newString);
        }

        public mstring MReplace(string oldString, string newString)
        {
            mstring o = mstring.Prepare(oldString);
            mstring n = mstring.Prepare(newString);            
            return MReplace(ref o, ref n);
        }

        public mstring ReplaceM(string oldString, string newString)
        {
            return MReplace(oldString, newString);
        }

        public mstring MSubstring(int startIndex, int length)
        {
            if (ByteCount == 0 || length == 0)
            {
                return mstring.Prepare();
            }

            if (startIndex < 0)
            {
                throw new ArgumentOutOfRangeException("startIndex cannot be negative.");
            }

            if (startIndex + length > Length)
            {
                throw new ArgumentOutOfRangeException("startIndex and length must refer to a location within the string.");
            }

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.CurrentDelPos = DEFAULT_DEL_POS;

            Stack.PutBytes(Stack.Buffer, StartIndex + startIndex * 2, length * 2);

            ms.ByteCount = length * 2;

            return ms;
        }        

        public mstring MSubstring(int startIndex)
        {
            return MSubstring(startIndex, Length - startIndex);
        }

        public mstring SubstringM(int startIndex, int length)
        {
            return MSubstring(startIndex, length);
        }

        public mstring SubstringM(int startIndex)
        {
            return MSubstring(startIndex);
        }

        public mstring MAppend(mstring s)
        {
            if (ByteCount == 0 && s.ByteCount == 0)
            {
                return this;
            }

            if (StartIndex + ByteCount == Stack.CurrentPos)
            {
                mstring s2 = mstring.Copy(s);
                Consume(ref s2);
            }
            else
            {
                mstring ms = Append(s);
                StartIndex = ms.StartIndex;
                ByteCount = ms.ByteCount;
            }

            return this;
        }        

        public mstring MAppend(Int32 x)
        {           
            mstring s = mstring.Prepare(x);
            Consume(ref s);
            return this;
        }

        public mstring MAppend(UInt32 x)
        {
            mstring s = mstring.Prepare(x);
            Consume(ref s);
            return this;
        }

        public mstring MAppend(Int16 x)
        {
            mstring s = mstring.Prepare(x);
            Consume(ref s);
            return this;
        }

        public mstring MAppend(UInt16 x)
        {
            mstring s = mstring.Prepare(x);
            Consume(ref s);
            return this;
        }

        public mstring MAppend(Int64 x)
        {
            mstring s = mstring.Prepare(x);
            Consume(ref s);
            return this;
        }

        public mstring MAppend(UInt64 x)
        {
            mstring s = mstring.Prepare(x);
            Consume(ref s);
            return this;
        }  

        public mstring MAppend(double x)
        {
            mstring s = mstring.Prepare(x);
            Consume(ref s);
            return this;
        }       

        public mstring MAppend(string s)
        {
            mstring ms = mstring.Prepare(s);
            Consume(ref ms);
            return this;
        }        

        public mstring MAppend(char c)
        {
            mstring s = mstring.Prepare(c);
            Consume(ref s);
            return this;
        }

        public mstring AppendM(mstring s)
        {
            return MAppend(s);
        }

        public mstring AppendM(Int32 x)
        {
            return MAppend(x);
        }

        public mstring AppendM(UInt32 x)
        {
            return MAppend(x);
        }

        public mstring AppendM(Int16 x)
        {
            return MAppend(x);
        }

        public mstring AppendM(UInt16 x)
        {
            return MAppend(x);
        }

        public mstring AppendM(Int64 x)
        {
            return MAppend(x);
        }

        public mstring AppendM(UInt64 x)
        {
            return MAppend(x);
        }

        public mstring AppendM(double x)
        {
            return MAppend(x);
        }

        public mstring AppendM(string s)
        {
            return MAppend(s);
        }

        public mstring AppendM(char c)
        {
            return MAppend(c);
        }

        private mstring Append(mstring s)
        {
            if (ByteCount == 0 && s.ByteCount == 0)
            {
                return this;
            }

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            Stack.PutBytes(Stack.Buffer, StartIndex, ByteCount);
            Stack.PutBytes(Stack.Buffer, s.StartIndex, s.ByteCount);
            ms.ByteCount = ByteCount + s.ByteCount;
            return ms;
        }

        public mstring Consume(ref mstring s)
        {
            if (s.ByteCount == 0)
            {
                return this;
            }

            if (ByteCount == 0)
            {
                StartIndex = s.StartIndex;
                ByteCount = s.ByteCount;
            }
            else
            {
                if (s.StartIndex == StartIndex + ByteCount)
                {
                    ByteCount += s.ByteCount;
                }
                else
                {
                    mstring ms = Append(s);                    
                    StartIndex = ms.StartIndex;
                    ByteCount = ms.ByteCount;
                }
            }
            
            s.Clear();

            return this;
        }

        public mstring Consume(ref Int32 x)
        {
            mstring ms = MAppend(x);
            x = 0;
            return ms;
        }

        public mstring Consume(ref UInt32 x)
        {
            mstring ms = MAppend(x);
            x = 0;
            return ms;
        }

        public mstring Consume(ref Int16 x)
        {
            mstring ms = MAppend(x);
            x = 0;
            return ms;
        }

        public mstring Consume(ref UInt16 x)
        {
            mstring ms = MAppend(x);
            x = 0;
            return ms;
        }

        public mstring Consume(ref Int64 x)
        {
            mstring ms = MAppend(x);
            x = 0;
            return ms;
        }

        public mstring Consume(ref UInt64 x)
        {
            mstring ms = MAppend(x);
            x = 0;
            return ms;
        }

        public mstring Consume(ref double x)
        {
            mstring ms = MAppend(x);
            x = 0;
            return ms;
        }

        public mstring Consume(ref string s)
        {
            mstring ms = MAppend(s);
            s = null;
            return ms;
        }

        public mstring Consume(ref char c)
        {
            mstring ms = MAppend(c);
            c = '\0';
            return ms;
        } 

        public void Clear()
        {
            StartIndex = 0;
            ByteCount = 0;
            CurrentDelPos = DEFAULT_DEL_POS;
        }

        public static bool operator ==(mstring x, mstring y)
        {
            if (x.ByteCount != y.ByteCount)
            {
                return false;
            }

            int i = x.StartIndex;
            int j = y.StartIndex;

            for (int k = 0; k < x.ByteCount; k++)
            {
                if (Stack.Buffer[i + k] != Stack.Buffer[j + k])
                {
                    return false;
                }
            }
            return true;
        }

        public static bool operator !=(mstring x, mstring y)
        {
            return !(x == y);
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            mstring ms;

            if(obj is string)
            {
                ms = mstring.Prepare((string)obj);
            }
            else
            {
                if (obj is mstring)
                {
                    ms = (mstring)obj;
                }
                else
                {
                    return false;
                }
            }

            return this == ms;
        }

        public mstring NextItemToString(char delimiter)
        {
            if (ByteCount == 0)
            {
                return mstring.Prepare();
            }

            int byteIndex = 0;
            int byteCount = 0;
            NextItem(delimiter, ref byteIndex, ref byteCount);

            if (byteCount == 0)
            {
                return mstring.Prepare();
            }

            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            Stack.PutBytes(Stack.Buffer, byteIndex, byteCount);
            ms.ByteCount = byteCount;
            return ms;
        }

        public mstring CsvNextItemToString()
        {
            return NextItemToString(',');
        }

        public Int32 NextItemToInt32(char delimiter)
        {
            if (ByteCount == 0)
            {
                return 0;
            }

            int byteIndex = 0;
            int byteCount = 0;
            NextItem(delimiter, ref byteIndex, ref byteCount);

            return GetNextInt32UTF16(Stack.Buffer, ref byteIndex, byteCount + byteIndex - 1);
        }

        public UInt32 NextItemToUInt32(char delimiter)
        {
            if (ByteCount == 0)
            {
                return 0;
            }

            int byteIndex = 0;
            int byteCount = 0;
            NextItem(delimiter, ref byteIndex, ref byteCount);

            return GetNextUInt32UTF16(Stack.Buffer, ref byteIndex, byteCount + byteIndex - 1);
        }

        public UInt32 NextItemToUInt(char delimiter)
        {
            return NextItemToUInt32(delimiter);
        }

        public Int32 NextItemToInt(char delimiter)
        {
            return NextItemToInt32(delimiter);
        }

        public Int32 CsvNextItemToInt32()
        {
            return NextItemToInt32(',');
        }

        public Int32 CsvNextItemToInt()
        {
            return CsvNextItemToInt32();
        }

        public Int16 NextItemToInt16(char delimiter)
        {
            if (ByteCount == 0)
            {
                return 0;
            }

            int byteIndex = 0;
            int byteCount = 0;
            NextItem(delimiter, ref byteIndex, ref byteCount);

            return GetNextInt16UTF16(Stack.Buffer, ref byteIndex, byteCount + byteIndex - 1);
        }

        public UInt16 NextItemToUInt16(char delimiter)
        {
            if (ByteCount == 0)
            {
                return 0;
            }

            int byteIndex = 0;
            int byteCount = 0;
            NextItem(delimiter, ref byteIndex, ref byteCount);

            return GetNextUInt16UTF16(Stack.Buffer, ref byteIndex, byteCount + byteIndex - 1);
        }

        public Int16 NextItemToShort(char delimiter)
        {
            return NextItemToInt16(delimiter);
        }

        public UInt16 NextItemToUShort(char delimiter)
        {
            return NextItemToUInt16(delimiter);
        }

        public Int64 NextItemToInt64(char delimiter)
        {
            if (ByteCount == 0)
            {
                return 0;
            }

            int byteIndex = 0;
            int byteCount = 0;
            NextItem(delimiter, ref byteIndex, ref byteCount);

            return GetNextInt64UTF16(Stack.Buffer, ref byteIndex, byteCount + byteIndex - 1);
        }

        public UInt64 NextItemToUInt64(char delimiter)
        {
            if (ByteCount == 0)
            {
                return 0;
            }

            int byteIndex = 0;
            int byteCount = 0;
            NextItem(delimiter, ref byteIndex, ref byteCount);

            return GetNextUInt64UTF16(Stack.Buffer, ref byteIndex, byteCount + byteIndex - 1);
        }

        public UInt64 NextItemToULong(char delimiter)
        {
            return NextItemToUInt64(delimiter);
        }

        public Int64 NextItemToLong(char delimiter)
        {
            return NextItemToInt64(delimiter);
        }

        public Int64 CsvNextItemToInt64()
        {
            return NextItemToInt64(',');
        }

        public Int64 CsvNextItemToLong()
        {
            return CsvNextItemToInt64();
        }

        public double NextItemToDouble(char delimiter)
        {            
            if (ByteCount == 0)
            {
                return 0;
            }

            int byteIndex = 0;
            int byteCount = 0;
            NextItem(delimiter, ref byteIndex, ref byteCount);

            return GetNextDoubleUTF16(Stack.Buffer, ref byteIndex, byteCount + byteIndex - 1);
        }

        public double CsvNextItemToDouble()
        {
            return NextItemToDouble(',');
        }

        private mstring ToCase(bool toUpper)
        {
            if (ByteCount == 0)
            {
                return mstring.Prepare();
            }

            int byteCount = 0;
            mstring ms;
            ms.StartIndex = Stack.CurrentPos;
            ms.CurrentDelPos = DEFAULT_DEL_POS;

            for (int i = 0; i < ByteCount; i += 2)
            {
                if (i + 1 >= ByteCount)
                {
                    throw new Exception("Missing bytes in UTF-16 encoded string.");
                }

                byte b0 = Stack.Buffer[i + StartIndex];
                byte b1 = Stack.Buffer[i + StartIndex + 1];

                if (UTFConverter.IsHighSurrogate(b0, b1) || UTFConverter.IsLowSurrogate(b0, b1))
                {
                    Stack.PutByte(b0);
                    Stack.PutByte(b1);
                    byteCount += 2;
                }
                else
                {
                    char ch = (char)UTFConverter.GetCodeValueUTF16(b0, b1);
                    char ch2;

                    if (toUpper)
                    {
                        ch2 = Char.ToUpper(ch);
                    }
                    else
                    {
                        ch2 = Char.ToLower(ch);
                    }

                    byteCount += Stack.PutChar(ch2);
                }
            }

            ms.ByteCount = byteCount;
            return ms;
        }

        public mstring MToUpper()
        {
            return ToCase(true);
        }
        public mstring MToLower()
        {
            return ToCase(false);
        }

        public mstring ToUpperM()
        {
            return MToUpper();
        }

        public mstring ToLowerM()
        {
            return MToLower();
        }

        private static int IndexOf(byte charByte0, byte charByte1, byte[] bytes, int byteIndex, int byteCount)
        {
            int endIndex = byteIndex + byteCount - 1;

            for (int i = byteIndex; i < endIndex; i += 2)
            {
                if (i + 1 > endIndex)
                {
                    return -1;
                }

                if (Stack.Buffer[i] == charByte0 && Stack.Buffer[i + 1] == charByte1)
                {
                    return i;
                }
            }

            return -1;
        }

        private static int IndexOf(char c, byte[] bytes, int byteIndex, int byteCount)
        {
            byte b0 = 0x0;
            byte b1 = 0x0;  
            UTFConverter.ConvertCodeValueToBytesUTF16((int)c, ref b0, ref b1);
            return IndexOf(b0, b1, bytes, byteIndex, byteCount);           
        }

        public int IndexOf(char c)
        {
            int i = IndexOf(c, Stack.Buffer, StartIndex, ByteCount);

            if (i == -1)
            {
                return -1;
            }

            return (i - StartIndex) / 2;
        }

        private static int IndexOf(mstring s, byte[] bytes, int byteIndex, int byteCount)
        {
            if (s.ByteCount == 0)
            {
                return 0;
            }

            if (s.ByteCount > byteCount)
            {
                return -1;
            }

            if ((s.StartIndex >= byteIndex) && (s.StartIndex + s.ByteCount <= byteIndex + byteCount))
            {
                return (s.StartIndex - byteIndex) / 2;
            }

            byte a0 = Stack.Buffer[s.StartIndex];
            byte a1 = Stack.Buffer[s.StartIndex + 1];
            byte b0 = Stack.Buffer[s.StartIndex + s.ByteCount - 2];
            byte b1 = Stack.Buffer[s.StartIndex + s.ByteCount - 1];
            int firstCharIndex = byteIndex;
            int bytesLeft = byteCount;

            for (; ; )
            {
                if (bytesLeft < s.ByteCount)
                {
                    return -1;
                }

                firstCharIndex = IndexOf(a0, a1, Stack.Buffer, firstCharIndex, bytesLeft);

                if (firstCharIndex == -1)
                {
                    return -1;
                }

                bytesLeft = byteCount - firstCharIndex + byteIndex;

                if (bytesLeft < s.ByteCount)
                {
                    return -1;
                }

                if (Stack.Buffer[firstCharIndex + s.ByteCount - 2] == b0 && Stack.Buffer[firstCharIndex + s.ByteCount - 1] == b1)
                {
                    bool bad = false;

                    for (int j = 0; j < s.ByteCount - 4; j++)
                    {
                        if (Stack.Buffer[firstCharIndex + 2 + j] != Stack.Buffer[s.StartIndex + 2 + j])
                        {
                            bad = true;
                            break;
                        }
                    }

                    if (!bad)
                    {
                        return firstCharIndex;
                    }
                }

                firstCharIndex += 2;
                bytesLeft -= 2;
            }            
        }

        public int IndexOf(mstring s)
        {
            int i = IndexOf(s, Stack.Buffer, StartIndex, ByteCount);

            if (i == -1)
            {
                return -1;
            }

            return (i - StartIndex) / 2;
        }

        public int IndexOf(string s)
        {
            mstring ms = mstring.Prepare(s);
            return IndexOf(ms);
        }

        public mstringarray MSplit(char delimiter)
        {           
            if (ByteCount == 0)
            {
                return mstringarray.Prepare(1);
            }

            int curPos = CurrentDelPos;
            ResetGetPosition();      

            int max = StartIndex + ByteCount - 1;
            int cnt = 0;
            recordset rs = recordset.Prepare();

            while (CurrentDelPos <= max)
            {
                mstring s = NextItemToString(delimiter);
                rs.PutInt32(s.StartIndex);
                rs.PutInt32(s.ByteCount);
                ++cnt;
            }

            CurrentDelPos = curPos;

            mstringarray arr = mstringarray.Prepare(cnt);

            for (int i = 0; i < cnt; i++)
            {
                arr.PutString(i, rs.GetInt32(), rs.GetInt32());
            }

            return arr;
        }

        public mstringarray SplitM(char delimiter)
        {
            return MSplit(delimiter);
        }

        public bool HasNextItem(char delimiter)
        {
            //Peek forward.
            int byteIndex = 0;
            int byteCount = 0;
            NextItemPeek(delimiter, ref byteIndex, ref byteCount);
            if (byteCount > 0)
            {
                return true;
            }            
            return false;
        }

        public bool HasNextItem()
        {
            return HasNextItem(',');
        }

        public void SkipToNextItem(char delimiter)
        {
            int byteIndex = 0;
            int byteCount = 0;
            NextItem(delimiter, ref byteIndex, ref byteCount);
        }

        private void NextItem(ref int curDelPos, char delimiter, ref int byteIndex, ref int byteCount)
        {
            byteIndex = 0;
            byteCount = 0;
            int max = StartIndex + ByteCount - 1;

            if (ByteCount == 0 || curDelPos > max)
            {
                return;
            }

            byte b0 = 0x0;
            byte b1 = 0x0;

            if (delimiter == ',')
            {
                b0 = 0x2C;
                b1 = 0x0;
            }
            else
            {
                UTFConverter.ConvertCodeValueToBytesUTF16((int)delimiter, ref b0, ref b1);
            }

            if (curDelPos == DEFAULT_DEL_POS)
            {
                curDelPos = StartIndex;
            }
            else
            {
                curDelPos += 2;
            }

            if (curDelPos > max)
            {
                return;
            }

            int itemStartIndex = curDelPos;
            int itemByteCount = 0;

            for (; curDelPos <= max; curDelPos += 2)
            {
                if (curDelPos + 1 > max)
                {
                    break;
                }

                if (Stack.Buffer[curDelPos] == b0 && Stack.Buffer[curDelPos + 1] == b1)
                {
                    break;
                }

                itemByteCount += 2;
            }

            byteIndex = itemStartIndex;
            byteCount = itemByteCount; 
        }

        private void NextItem(char delimiter, ref int byteIndex, ref int byteCount)
        {
            NextItem(ref CurrentDelPos, delimiter, ref byteIndex, ref byteCount);
        }

        private void NextItemPeek(char delimiter, ref int byteIndex, ref int byteCount)
        {
            int curdelpos = CurrentDelPos;
            NextItem(ref curdelpos, delimiter, ref byteIndex, ref byteCount);
        }

        private static int GetNextInt32UTF16(byte[] linebuf, ref int offset, int endIndex)
        {
            if (offset >= endIndex)
            {
                return 0;
            }

            unchecked
            {
                char ch = '\0';
                // Skip leading non-digits.
                for (; ; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (offset >= endIndex)
                    {
                        return 0;
                    }
                    if ('-' == ch
                        || (ch >= '0' && ch <= '9'))
                    {
                        break;
                    }
                }

                if (offset >= endIndex)
                {
                    return 0;
                }
                bool neg = false;
                if ('-' == ch)
                {
                    neg = true;
                    offset += 2;
                }

                int result = 0;
                for (; offset < endIndex; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (ch >= '0' && ch <= '9')
                    {
                        result *= 10;
                        result += (byte)ch - '0';
                    }
                    else
                    {
                        offset += 2; // ...
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

        private static UInt32 GetNextUInt32UTF16(byte[] linebuf, ref int offset, int endIndex)
        {
            if (offset >= endIndex)
            {
                return 0;
            }

            unchecked
            {
                char ch = '\0';
                // Skip leading non-digits.
                for (; ; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (offset >= endIndex)
                    {
                        return 0;
                    }
                    if (ch >= '0' && ch <= '9')
                    {
                        break;
                    }
                }

                if (offset >= endIndex)
                {
                    return 0;
                }
                //bool neg = false;
                /*if ('-' == ch)
                {
                    neg = true;
                    offset += 2;
                }*/

                uint result = 0;
                for (; offset < endIndex; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (ch >= '0' && ch <= '9')
                    {
                        result *= 10;
                        result += (uint)((byte)ch - '0');
                    }
                    else
                    {
                        offset += 2; // ...
                        break;
                    }
                }
                /*if (neg)
                {
                    result = -result;
                }*/
                return result;
            }
        }

        private static Int64 GetNextInt64UTF16(byte[] linebuf, ref int offset, int endIndex)
        {
            if (offset >= endIndex)
            {
                return 0;
            }

            unchecked
            {
                char ch = '\0';
                // Skip leading non-digits.
                for (; ; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (offset >= endIndex)
                    {
                        return 0;
                    }
                    if ('-' == ch
                        || (ch >= '0' && ch <= '9'))
                    {
                        break;
                    }
                }

                if (offset >= endIndex)
                {
                    return 0;
                }
                bool neg = false;
                if ('-' == ch)
                {
                    neg = true;
                    offset += 2;
                }

                Int64 result = 0;
                for (; offset < endIndex; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (ch >= '0' && ch <= '9')
                    {
                        result *= 10;
                        result += (byte)ch - '0';
                    }
                    else
                    {
                        offset += 2; // ...
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

        private static UInt64 GetNextUInt64UTF16(byte[] linebuf, ref int offset, int endIndex)
        {
            if (offset >= endIndex)
            {
                return 0;
            }

            unchecked
            {
                char ch = '\0';
                // Skip leading non-digits.
                for (; ; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (offset >= endIndex)
                    {
                        return 0;
                    }
                    if (ch >= '0' && ch <= '9')
                    {
                        break;
                    }
                }

                if (offset >= endIndex)
                {
                    return 0;
                }
                /*
                bool neg = false;
                if ('-' == ch)
                {
                    neg = true;
                    offset += 2;
                }*/

                UInt64 result = 0;
                for (; offset < endIndex; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (ch >= '0' && ch <= '9')
                    {
                        result *= 10;
                        result += (UInt64)((byte)ch - '0');
                    }
                    else
                    {
                        offset += 2; // ...
                        break;
                    }
                }
                /*if (neg)
                {
                    result = -result;
                }*/
                return result;
            }
        }

        private static Int16 GetNextInt16UTF16(byte[] linebuf, ref int offset, int endIndex)
        {
            if (offset >= endIndex)
            {
                return 0;
            }

            unchecked
            {
                char ch = '\0';
                // Skip leading non-digits.
                for (; ; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (offset >= endIndex)
                    {
                        return 0;
                    }
                    if ('-' == ch
                        || (ch >= '0' && ch <= '9'))
                    {
                        break;
                    }
                }

                if (offset >= endIndex)
                {
                    return 0;
                }
                bool neg = false;
                if ('-' == ch)
                {
                    neg = true;
                    offset += 2;
                }

                Int16 result = 0;
                for (; offset < endIndex; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (ch >= '0' && ch <= '9')
                    {
                        result *= 10;
                        result += (Int16)((byte)ch - '0');
                    }
                    else
                    {
                        offset += 2; // ...
                        break;
                    }
                }
                if (neg)
                {
                    result = (Int16)(-result);
                }
                return result;
            }
        }

        private static UInt16 GetNextUInt16UTF16(byte[] linebuf, ref int offset, int endIndex)
        {
            if (offset >= endIndex)
            {
                return 0;
            }

            unchecked
            {
                char ch = '\0';
                // Skip leading non-digits.
                for (; ; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (offset >= endIndex)
                    {
                        return 0;
                    }
                    if (ch >= '0' && ch <= '9')
                    {
                        break;
                    }
                }

                if (offset >= endIndex)
                {
                    return 0;
                }                

                UInt16 result = 0;
                for (; offset < endIndex; offset += 2)
                {
                    int xch = 0;
                    xch = linebuf[offset];
                    xch |= (int)linebuf[offset + 1] << 8;
                    ch = (char)xch;

                    if (ch >= '0' && ch <= '9')
                    {
                        result *= 10;
                        result += (UInt16)((byte)ch - '0');
                    }
                    else
                    {
                        offset += 2; // ...
                        break;
                    }
                }                
                return result;
            }
        }

        private static double GetNextDoubleUTF16(byte[] buffer, ref int offset, int endIndex)
        {
            if (offset >= endIndex)
            {
                return 0;
            }

            unchecked
            {
                char ch = '\0';
                // Skip leading non-digits.
                for (; ; offset += 2)
                {
                    if (offset >= endIndex)
                    {
                        return 0;
                    }

                    int xch = UTFConverter.GetCodeValueUTF16(buffer[offset], buffer[offset + 1]);                   
                    ch = (char)xch;

                    if ('-' == ch || '.' == ch || (ch >= '0' && ch <= '9'))
                    {
                        break;
                    }
                }

                bool neg = false;
                if ('-' == ch)
                {
                    neg = true;
                    offset += 2;
                }

                int ep = IndexOf('E', buffer, offset, endIndex - offset + 1);
                int dp = IndexOf('.', buffer, offset, endIndex - offset + 1);
                int wordStart = offset;
                int wordEnd = (ep == -1 ? endIndex : ep - 1);

                if (dp == -1)
                {
                    dp = wordEnd + 1;
                }

                int exp = 0;
                if (ep > -1)
                {
                    int o = ep + 2;
                    exp = GetNextInt32UTF16(buffer, ref o, endIndex);
                }

                int cut = dp + exp * 2;

                if (exp > 0)
                {
                    cut += 2;
                }

                double intg = 0;
                double dec = 0;
                int decCount = 0;

                for (int i = wordStart; i < cut; i += 2)
                {
                    if (i == dp)
                    {
                        continue;
                    }

                    if (i > wordEnd)
                    {
                        ch = '0';
                    }
                    else
                    {
                        int xch = UTFConverter.GetCodeValueUTF16(buffer[i], buffer[i + 1]);
                        ch = (char)xch;
                    }

                    if (ch >= '0' && ch <= '9')
                    {
                        intg *= 10;
                        intg += (byte)ch - '0';
                    }
                }

                for (int i = cut; i < wordEnd; i += 2)
                {
                    if (i == dp)
                    {
                        continue;
                    }

                    if (i < wordStart)
                    {
                        ch = '0';
                    }
                    else
                    {
                        int xch = UTFConverter.GetCodeValueUTF16(buffer[i], buffer[i + 1]);
                        ch = (char)xch;
                    }

                    if (ch >= '0' && ch <= '9')
                    {
                        dec *= 10;
                        dec += (byte)ch - '0';
                    }

                    ++decCount;
                }

                dec = dec / Math.Pow(10, decCount);

                double result = intg + dec;

                if (neg)
                {
                    result = -result;
                }

                return result;
            }
        }
        
        public static mstring Prepare(double x)
        {
            if (x == 0)
            {
                return mstring.Prepare('0');
            }

            if (double.IsInfinity(x))
            {
                return mstring.Prepare("Infinity");
            }

            if (double.IsNegativeInfinity(x))
            {
                return mstring.Prepare("-Infinity");
            }

            if (double.IsNaN(x))
            {
                return mstring.Prepare("-NaN");
            }

            if (Entry.isNegativeZero(x))
            {
                return mstring.Prepare("-0");
            }

            int startIndex = Stack.CurrentPos;          
            bool neg = false;

            if (x < 0)
            {
                neg = true;
                Stack.PutChar('-');               
                x = Math.Abs(x);
            }

            //For rounding.
            int wordStart = Stack.CurrentPos;
            Stack.PutChar('0');    

            int maxSf = 16;
            int maxLeading0s = 4;           
            double intg = Math.Truncate(x);
            double frac = x - intg;
            int sfLeft = maxSf;
            bool scNotation = false;
            int scExp = 0;
            int dotPos = -1;
            char ch;

            //Integral
            if (intg > 0)
            {
                bool addDot = false;
                int places = 1;
                double d = intg / (double)10;

                while (d >= 1)
                {
                    d = d / (double)10;
                    ++places;
                }

                if (places >= maxSf) // && frac == 0)
                {
                    addDot = true;
                    scNotation = true;
                    scExp = places - 1;
                }

                for (int i = places; i > 0; i--)
                {
                    double div = Math.Pow(10, i - 1);
                    double quo = Math.Truncate(x / div);
                    double rem = x - quo * div;
                    x = rem;
                                        
                    Stack.PutChar(GetIntChar((int)quo));

                    if (i == places && addDot)
                    {
                        dotPos = Stack.CurrentPos;
                        Stack.PutChar('.');
                    }

                    if (--sfLeft == 0)
                    {
                        break;
                    }
                }
            }

            //Fraction    
            if (sfLeft > 0 && frac > 0)
            {
                double _frac = frac;
                double d;

                if (intg == 0)
                {
                    int leading0s = 0;

                    for (; ; )
                    {
                        double tmp = _frac * 10;
                        d = Math.Truncate(tmp);
                        _frac = tmp - d;

                        if (d == 0)
                        {
                            ++leading0s;
                        }
                        else
                        {
                            break;
                        }
                    }

                    if (leading0s < maxLeading0s)
                    {
                        Stack.PutChar('0');
                        dotPos = Stack.CurrentPos;
                        Stack.PutChar('.');                      

                        for (int i = 0; i < leading0s; i++)
                        {
                            Stack.PutChar('0');
                        }

                        Stack.PutChar(GetIntChar((int)d));
                    }
                    else
                    {
                        Stack.PutChar(GetIntChar((int)d));
                        dotPos = Stack.CurrentPos;
                        Stack.PutChar('.');
                        scNotation = true;
                        scExp = -(leading0s + 1);
                    }

                    --sfLeft;
                }
                else
                {
                    dotPos = Stack.CurrentPos;
                    Stack.PutChar('.');
                }

                while (sfLeft > 0 && _frac > 0)
                {
                    double tmp = _frac * 10;
                    d = Math.Truncate(tmp);

                    Stack.PutChar(GetIntChar((int)d));
                    --sfLeft;

                    _frac = tmp - d;
                }
            }

            //Rounding
            if (sfLeft == 0)
            {
                char lastCh = (char)UTFConverter.GetCodeValueUTF16(Stack.Buffer[Stack.CurrentPos - 2], Stack.Buffer[Stack.CurrentPos - 1]);
                 
                //Dissolve it.
                Stack.ReplaceChar(Stack.CurrentPos - 2, '0');

                if (lastCh > '4')
                {
                    for (int i = Stack.CurrentPos - 4; i >= wordStart; i-=2)
                    {
                        if(i == dotPos)
                        {
                            continue;
                        }

                        ch = (char)UTFConverter.GetCodeValueUTF16(Stack.Buffer[i], Stack.Buffer[i + 1]);
                      
                        ++ch;

                        if (ch > '9')
                        {                            
                            Stack.ReplaceChar(i, '0');
                        }
                        else
                        {
                            Stack.ReplaceChar(i, ch);
                            break;
                        }
                    }
                }
            }

            //Strip trailing zeros after dot
            int wordEnd = Stack.CurrentPos - 1;

            if (dotPos > -1)
            {              
                for (int t = wordEnd - 1; t >= dotPos; t-=2)
                {
                    ch = (char)UTFConverter.GetCodeValueUTF16(Stack.Buffer[t], Stack.Buffer[t + 1]);
                    if (ch != '0')
                    {
                        wordEnd = t + 1;
                        break;
                    }
                }               
            }           

            //Strip '.' if it is at the end.
            if (wordEnd == dotPos + 1)
            {
                wordEnd -= 2;
            }

            if (scNotation)
            {
                wordEnd += Stack.SetChar(wordEnd + 1, 'E');

                if (scExp < 0)
                {
                    wordEnd += Stack.SetChar(wordEnd + 1, '-');
                    scExp = Math.Abs(scExp);
                }
                else
                {
                    wordEnd += Stack.SetChar(wordEnd + 1, '+');
                }

                int places = 1;
                double d = scExp / (double)10;

                while (d >= 1)
                {
                    d = d / (double)10;
                    ++places;
                }

                for (int i = places; i > 0; i--)
                {
                    double div = Math.Pow(10, i - 1);
                    double quo = Math.Truncate(scExp / div);
                    double rem = scExp - quo * div;
                    scExp = (int)rem;

                    wordEnd += Stack.SetChar(wordEnd + 1, GetIntChar((int)quo));                  
                }
            }

            //Take care of the first place holding byte.
            ch = (char)UTFConverter.GetCodeValueUTF16(Stack.Buffer[wordStart], Stack.Buffer[wordStart + 1]);

            if (ch == '0')
            {
                if (neg)
                {
                    Stack.ReplaceChar(wordStart, '-');                    
                }
                else
                {
                    wordStart += 2;
                }
            }
            else
            {
                if (neg)
                {
                    wordStart = startIndex;
                }
            }

            mstring ms;
            ms.StartIndex = wordStart;
            ms.CurrentDelPos = DEFAULT_DEL_POS;
            ms.ByteCount = wordEnd - wordStart + 1;
            return ms;
        }

        private static char GetIntChar(int x)
        {
            return (char)('0' + x);
        }

        private static byte[,] TrimCharsBytes = new byte[1,2]{{0x0,0x0}};
        public mstring MTrim(char trimChar)
        {
            byte b0 = 0x0;
            byte b1 = 0x0;
            UTFConverter.ConvertCodeValueToBytesUTF16((int)trimChar, ref b0, ref b1);
            TrimCharsBytes[0, 0] = b0;
            TrimCharsBytes[0, 1] = b1;
            return MTrim(TrimCharsBytes);
        }

        private mstring MTrim(byte[,] trimBytes)
        {
            int trimBytesLen = trimBytes.GetLength(0);
            if (trimBytesLen == 0)
            {
                return this;
            }
            if (ByteCount == 0)
            {
                Clear();
                return this;
            }
            if (trimBytes.GetLength(1) != 2)
            {
                throw new Exception("trimBytes width is invalid.");
            }

            int start = StartIndex;
            int endIndex = StartIndex + ByteCount - 1;
            int i;

            for (i = StartIndex; i < endIndex; i += 2)
            {
                bool found = false;
                for (int j = 0; j < trimBytesLen; j++)
                {
                    if (Stack.Buffer[i] == trimBytes[j, 0] && Stack.Buffer[i + 1] == trimBytes[j, 1])
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    start = i;
                    break;
                }
            }

            if (i < endIndex)
            {
                int end = start + 1;

                for (i = endIndex - 1; i > start; i -= 2)
                {
                    bool found = false;
                    for (int j = 0; j < trimBytesLen; j++)
                    {
                        if (Stack.Buffer[i] == trimBytes[j, 0] && Stack.Buffer[i + 1] == trimBytes[j, 1])
                        {
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        end = i + 1;
                        break;
                    }
                }

                StartIndex = start;
                ByteCount = end - start + 1;
                CurrentDelPos = DEFAULT_DEL_POS;
                return this;
            }
            else
            {
                Clear();
                return this;
            }
        }
     
        private static byte[,] WhiteSpaceCharsBytes = null;
        public mstring MTrim()
        {
            if (WhiteSpaceCharsBytes == null)
            {               
                char[] whiteSpaceChars = new char[] { '\u0009', '\u000A', '\u000B', '\u000C', '\u000D', '\u0020', '\u0085', '\u00A0', 
                '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200A', '\u200B', 
                '\u2028', '\u2029', '\u3000', '\uFEFF'};

                WhiteSpaceCharsBytes = new byte[whiteSpaceChars.Length, 2];

                for (int i = 0; i < whiteSpaceChars.Length; i++)
                {
                    byte b0 = 0x0;
                    byte b1 = 0x0;
                    UTFConverter.ConvertCodeValueToBytesUTF16((int)whiteSpaceChars[i], ref b0, ref b1);
                    WhiteSpaceCharsBytes[i, 0] = b0;
                    WhiteSpaceCharsBytes[i, 1] = b1;
                }                
            }
            return MTrim(WhiteSpaceCharsBytes);
        }

        public mstring TrimM(char trimChar)
        {
            return MTrim(trimChar);
        }

        public mstring TrimM()
        {
            return MTrim();
        }

        public bool StartsWith(mstring s)
        {            
            if (s.ByteCount == 0)
            {
                return true;
            }

            if (ByteCount < s.ByteCount)
            {
                return false;
            }

            for (int i = 0; i < s.ByteCount; i++)
            {
                if (Stack.Buffer[StartIndex + i] != Stack.Buffer[s.StartIndex + i])
                {
                    return false;
                }
            }

            return true;
        }

        public bool StartsWith(string s)
        {            
            return StartsWith(mstring.Prepare(s));
        }

        public bool EndsWith(mstring s)
        {
            if (s.ByteCount == 0)
            {
                return true;
            }

            if (ByteCount < s.ByteCount)
            {
                return false;
            }

            int startIndex = StartIndex + ByteCount - s.ByteCount;

            for (int i = 0; i < s.ByteCount; i++)
            {
                if (Stack.Buffer[startIndex + i] != Stack.Buffer[s.StartIndex + i])
                {
                    return false;
                }
            }

            return true;
        }

        public bool EndsWith(string s)
        {          
            return EndsWith(mstring.Prepare(s));
        }

        public bool Contains(char c)
        {
            return (IndexOf(c) != -1);
        }

        public bool Contains(mstring s)
        {
            return (IndexOf(s) != -1);
        }

        public bool Contains(string s)
        {           
            return (IndexOf(s) != -1);
        }
    }
}
