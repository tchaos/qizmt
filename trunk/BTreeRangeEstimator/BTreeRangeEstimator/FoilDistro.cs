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
using System.Collections;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects
{

    public class FoilDistro : RangeDistro
    {

        int majorcount, minorcount;
        int keymajor;

        IList<IList<byte>> table; // Sorted samples.
        UInt32[] majorminors; // Same Count as table.


        RangeNode RangeNodeFromTableIndex(int tableindex)
        {
            UInt32 mm = majorminors[tableindex];
            RangeNode rn;
            rn.MajorID = (UInt16)(mm >> 16);
            rn.MinorID = (UInt16)(mm);
            return rn;
        }


        // Note: samples should be all map input data.
        public FoilDistro(IList<IList<byte>> samples, int majorcount, int minorcount)
        {
            if (null == samples)
            {
                throw new Exception("FoilDistro: (null == samples)");
            }

#if DEBUG
            //System.Threading.Thread.Sleep(10000);
#endif

            this.majorcount = majorcount;
            this.minorcount = minorcount;

            if (samples.Count > 0)
            {
                keymajor = samples[0].Count;

                {
                    List<IList<byte>> tsamp1 = samples as List<IList<byte>>;
                    if (null != tsamp1)
                    {
                        tsamp1.Sort(FByteListCompare);
                    }
                    else
                    {
                        IList<byte>[] tsamp2 = samples as IList<byte>[];
                        if (null != tsamp2)
                        {
                            Array.Sort(tsamp2, FByteListCompare);
                        }
                        else
                        {
                            throw new Exception("DsDistro: cannot sort samples list (unknown type)");
                        }
                    }
                }
            }

            this.table = samples;
            this.majorminors = new uint[this.table.Count];
            PartitionMajorAndMinor();

        }


        private void PartitionMajorAndMinor()
        {
            int slotspermajor = table.Count / majorcount;
            int extramajorslots = table.Count % majorcount;
            if (0 != extramajorslots)
            {
                slotspermajor++;
            }
            int slotsperminor = slotspermajor / minorcount;
            int extraminorslots = slotspermajor % minorcount;
            if (0 != extraminorslots)
            {
                slotsperminor++;
            }
            ushort major = 0;
            int majorslot = 0;
            ushort minor = 0;
            int minorslot = 0;
            for (int i = 0; i < table.Count; i++)
            {
                if (major >= majorcount)
                {
                    throw new Exception("MajorID miscalculation for " + this.GetType().Name + ": out of range");
                }
                if (minor >= minorcount)
                {
                    throw new Exception("MinorID miscalculation for " + this.GetType().Name + ": out of range");
                }
                //ranges[i] = new RangeNode();
                UInt32 mm = ((UInt32)major << 16) | (UInt32)minor;
                majorminors[i] = mm;
                if (++majorslot >= slotspermajor)
                {
                    major++;
                    minor = 0;
                    majorslot = 0;
                    minorslot = 0;
                    if (major == extramajorslots)
                    {
                        slotspermajor--;
                    }
                    {
                        extraminorslots = slotspermajor % minorcount;
                        if (0 != extraminorslots)
                        {
                            slotsperminor++;
                        }
                    }
                }
                else if (++minorslot >= slotsperminor)
                {
                    minor++;
                    minorslot = 0;
                    if (minor == extraminorslots)
                    {
                        slotsperminor--;
                    }
                }
            }
        }


        int _BinarySearch(IList<byte> key, int keyoffset, int keylength, int tablestart, int tablelength)
        {
            if (tablelength < 1)
            {
                return -1;
            }
            for (; ; )
            {
#if DEBUG
                if (tablelength < 1)
                {
                    throw new Exception("(tablelength < 1)");
                }
#endif
                int midlen = tablelength / 2;
                int halfstart = tablestart + midlen;
                int x = FByteListCompare(key, keyoffset, keylength, table[halfstart]);
                if (x == 0)
                {
                    return halfstart;
                }
                if (x > 0)
                {
                    tablestart = halfstart + 1;
                    tablelength = midlen - 1;
                    if (tablelength <= 0)
                    {
                        return halfstart; // Nearest.
                    }
                    //return _BinarySearch(key, keyoffset, keylength, tablestart, tablelength);
                    // Loop...
                }
                else //if (x < 0)
                {
                    //tablestart = tablestart;
                    tablelength = midlen;
                    if (tablelength <= 1)
                    {
                        return tablestart; // Nearest.
                    }
                    //return _BinarySearch(key, keyoffset, keylength, tablestart, tablelength);
                    // Loop...
                }
            }
            //return -1;
        }


        public override RangeNode Distro(IList<byte> keybuf, int keyoffset, int keylength)
        {
            int result = _BinarySearch(keybuf, keyoffset, keylength, 0, table.Count);
            while (result < table.Count - 1 && FByteListCompare(keybuf, keyoffset, keylength, table[result]) > 0)
            {
                result++;
            }
            while (result > 0 && FByteListCompare(keybuf, keyoffset, keylength, table[result]) < 0)
            {
                result--;
            }
            return RangeNodeFromTableIndex(result);
        }


        public override int GetMemoryUsage()
        {
            checked
            {
                int mu = 4 + 4 + 4 + IntPtr.Size;
                if (null == table)
                {
                    throw new Exception("GetMemoryUsage: (null == table)");
                }
                mu += table.Count * IntPtr.Size;
                for (int i = 0; i < table.Count; i++)
                {
                    mu += IntPtr.Size; // GC
                    if (null == table[i])
                    {
                        throw new Exception("GetMemoryUsage: (null == table[i=" + i.ToString() + "])");
                    }
                    mu += table[i].Count;
                }
                return mu;
            }
        }



        internal int FByteListCompare(IList<byte> x, IList<byte> y)
        {
#if DEBUG
            if (x.Count != y.Count)
            {
                throw new Exception("FByteListCompare: List<byte>s being compared need to be the same length");
            }
#endif
            for (int i = 0; i < x.Count; i++)
            {
                int diff = x[i] - y[i];
                if (0 != diff)
                {
                    return diff;
                }
            }
            return 0;
        }

        internal int FByteListCompare(IList<byte> x, int xoff, int xlen, IList<byte> y)
        {
#if DEBUG
            if (xlen != y.Count)
            {
                throw new Exception("FByteListCompare: List<byte>s being compared need to be the same length");
            }
#endif
            for (int i = 0; i < xlen; i++)
            {
                int diff = x[xoff + i] - y[i];
                if (0 != diff)
                {
                    return diff;
                }
            }
            return 0;
        }


    }


}
