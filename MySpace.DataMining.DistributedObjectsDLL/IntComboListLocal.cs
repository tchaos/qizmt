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

namespace MySpace.DataMining.DistributedObjects5
{
    public class IntComboListLocal : IntComboList
    {
        List<IntComboList.B8> b8list;


        public int _getinternalcapacity()
        {
            return b8list.Capacity;
        }


        public IntComboListLocal(string objectname)
            : base(objectname)
        {
        }


        public override void AddBlock(string capacity, string sUserBlockInfo)
        {
            if (null != b8list)
            {
                //throw new Exception("AddBlock can only be called once for IntComboList");
                return; // ...
            }

            b8list = new List<IntComboList.B8>(ParseCapacity(capacity));
        }


        public override void Open()
        {
        }


        public override void SortBlocks()
        {
            b8list.Sort();
        }


        public override void Flush()
        {
        }


        internal static int ParseCapacity(string capacity)
        {
            try
            {
                if (null == capacity || 0 == capacity.Length)
                {
                    throw new Exception("Invalid capacity: capacity not specified");
                }
                switch (capacity[capacity.Length - 1])
                {
                    case 'B':
                        if (1 == capacity.Length)
                        {
                            throw new Exception("Invalid capacity: " + capacity);
                        }
                        switch (capacity[capacity.Length - 2])
                        {
                            case 'K': // KB
                                return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024;

                            case 'M': // MB
                                return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024;

                            case 'G': // GB
                                return int.Parse(capacity.Substring(0, capacity.Length - 2)) * 1024 * 1024 * 1024;

                            default: // Just bytes with B suffix.
                                return int.Parse(capacity.Substring(0, capacity.Length - 1));
                        }
                    //break;

                    default: // Assume just bytes without a suffix.
                        return int.Parse(capacity);
                }
            }
            catch (FormatException e)
            {
                throw new FormatException("Invalid capacity: bad format: '" + capacity + "' problem: " + e.ToString());
            }
            catch (OverflowException e)
            {
                throw new OverflowException("Invalid capacity: overflow: '" + capacity + "' problem: " + e.ToString());
            }
        }


        public override int SlaveCount
        {
            get
            {
                return 1;
            }
        }


        public override void Close()
        {
            b8list = null;
        }


        public override void Add(int a, int b)
        {
            if (null == b8list)
            {
                throw new Exception("Call AddBlock before Add");
            }

            IntComboList.B8 x = new IntComboList.B8(a, b);
            b8list.Add(x);
        }


        public override IntComboListEnumerator[] GetEnumerators()
        {
            IntComboListEnumerator[] result = new IntComboListEnumerator[1];
            result[0] = new IntComboListLocalEnumerator(b8list.GetEnumerator());
            return result;
        }
    }


    public class IntComboListLocalEnumerator : IntComboListEnumerator
    {
        System.Collections.Generic.IEnumerator<IntComboList.B8> b8enum;


        internal IntComboListLocalEnumerator(System.Collections.Generic.IEnumerator<IntComboList.B8> b8enum)
        {
            this.b8enum = b8enum;
        }


        public override KeyRun Current
        {
            get
            {
                return KeyRun.Create(b8enum.Current, 1); // Note: doesn't compress keys (count always 1).
            }
        }


        public override bool MoveNext()
        {
            return b8enum.MoveNext();
        }


        public override void Reset()
        {
            b8enum.Reset();
        }
    }

}
