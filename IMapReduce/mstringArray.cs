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
    public struct mstringarray
    {
        private int m_startIndex;
        private int m_length;        

        public static mstringarray Prepare()
        {
            mstringarray arr;
            arr.m_length = 0;
            arr.m_startIndex = 0;
            return arr;
        }

        public static mstringarray Prepare(int length)
        {
            if (length == 0)
            {
                return mstringarray.Prepare();
            }

            mstringarray arr;
            arr.m_startIndex = Stack.CurrentPos;
            arr.m_length = length;
            Stack.PutInt32(length);

            for (int i = 0; i < length; i++)
            {
                Stack.PutInt32(0);
                Stack.PutInt32(0);
            }

            return arr;
        }

        public int Length
        {
            get
            {
                return m_length;
            }
        }
        
        public mstring this[int index]
        {
            get
            {
                return GetString(index);
            }
            set
            {
                PutString(index, value);
            }
        }

        private int GetStringStartIndexOffset(int index)
        {
            if (index < 0 || index >= m_length)
            {
                throw new ArgumentOutOfRangeException();
            }

            return m_startIndex + 4 + index * 8;
        }

        private int GetStringByteCountOffset(int index)
        {
            if (index < 0 || index >= m_length)
            {
                throw new ArgumentOutOfRangeException();
            }

            return m_startIndex + 8 + index * 8;
        }

        private int GetStringStartIndex(int index)
        {
            int offset = GetStringStartIndexOffset(index);
            return Entry.BytesToInt(Stack.Buffer, offset);
        }

        private int GetStringByteCount(int index)
        {
            int offset = GetStringByteCountOffset(index);
            return Entry.BytesToInt(Stack.Buffer, offset);
        }

        internal void PutString(int index, int mstringStartIndex, int mstringByteCount)
        {
            int si = GetStringStartIndexOffset(index);
            int bc = GetStringByteCountOffset(index);

            Entry.ToBytes(mstringStartIndex, Stack.Buffer, si);
            Entry.ToBytes(mstringByteCount, Stack.Buffer, bc);
        }

        private void PutString(int index, mstring s)
        {
            PutString(index, s.StartIndex, s.ByteCount);        
        }        

        private mstring PutString(int index, string s)
        {
            mstring ms = mstring.Prepare(s);
            PutString(index, ms);
            return ms;
        }       
        
        private mstring GetString(int index)
        {
            int si = GetStringStartIndex(index);
            int bc = GetStringByteCount(index);

            return mstring.Prepare(si, bc);
        }
    }
}
