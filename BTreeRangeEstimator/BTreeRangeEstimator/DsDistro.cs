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

// Use data array, otherwise use unsafe pointers.
#define DSDISTRO_DATA_ARRAY


using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects
{

    public 
#if DSDISTRO_DATA_ARRAY
#else
        unsafe
#endif
        class DsDistro : RangeDistro
    {
        int majorcount, minorcount;
        int keymajor;

#if DSDISTRO_DATA_ARRAY
        Node[] data;

        int datalength { get { return data.Length; } }
#else
        uint* data;
        private int _datalen = 0;

        int datalength { get { return _datalen; } }
#endif

        const uint LEAF_BIT = 0x80000000;

        public override int GetMemoryUsage()
        {
            checked
            {
                int mu = 4 + 4 + 4 + IntPtr.Size + 4;
                if (null != data)
                {
                    mu += datalength * Node.SizeOf;
                }
                return mu;
            }
        }


        // leaf is from data and has LEAF_BIT set.
        private RangeNode RangeNodeFromLeaf(uint leaf)
        {
#if DEBUG//distro
            if ((LEAF_BIT & leaf) != LEAF_BIT)
            {
                throw new Exception("DsDistro: RangeNodeFromLeaf expects LEAF_BIT");
            }
#endif
            uint y = leaf & ~LEAF_BIT;
            RangeNode rn;
            rn.MajorID = (ushort)(y / (uint)minorcount);
            rn.MinorID = (ushort)(y % (uint)minorcount);
            return rn;
        }


        internal struct LeafInfo: IComparable<LeafInfo>
        {
            internal IList<byte> sample; // The actual sample key.
            internal uint leaf_noffset; // Offset into data of this int which should be set to (LEAF_BIT | majorminorcode)

            public int CompareTo(LeafInfo that)
            {
                for (int i = 0; i < this.sample.Count; i++)
                {
                    int diff = this.sample[i] - that.sample[i];
                    if (0 != diff)
                    {
                        return diff;
                    }
                }
                return 0;
            }

        }


        internal class DsIListByteComparer : IComparer<IList<byte>>
        {
            public int Compare(IList<byte> x, IList<byte> y)
            {
#if DEBUG
                if (x.Count != y.Count)
                {
                    throw new Exception("DsByteListComparer: List<byte>s being compared need to be the same length");
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
        }


        ~DsDistro()
        {
#if DSDISTRO_DATA_ARRAY
#else
            System.Runtime.InteropServices.Marshal.FreeHGlobal((IntPtr)data);
#endif
        }


        int CharSkip(IList<IList<byte>> samples, int isample, int iendsample, int ichar)
        {
            int isamplestart = isample;
            for (isample++; isample < iendsample
                && samples[isample][ichar] == samples[isamplestart][ichar];
                isample++)
            {
            }
            return isample - isamplestart;
        }


#if DSDISTRO_XCOUNT
        int XCount(IList<IList<byte>> samples, int isample, int iendsample, int ichar)
        {
            int thiscount = 0;
            int subcount = 0;
            while (isample < iendsample)
            {
                thiscount++;
                int xsame = CharSkip(samples, isample, iendsample, ichar);
#if DEBUG
                if (xsame < 1)
                {
                    throw new Exception("XCount: CharSkip returned " + xsame.ToString() + " at character position " + ichar.ToString() + " when expecting " + (iendsample - isample).ToString() + " more samples");
                }
#endif
                if (ichar + 1 < keymajor)
                {
                    subcount += XCount(samples, isample, isample + xsame, ichar + 1);
                }
                isample += xsame;
            }
            return thiscount + subcount;
        }

        // Returns total table count; countsmalltables is small tables; (return - countsmalltables) is large tables.
        // large table entries is (256 * return)
        int XCount(IList<IList<byte>> samples)
        {
            return XCount(samples, 0, samples.Count, 0);
        }
#endif


        // alloc_noffset is current allocation point in data.
        // Returns noffset in data.
        uint XPopulate(IList<IList<byte>> samples, int isample, int iendsample, int ichar, ref uint alloc_noffset, List<LeafInfo> leaves)
        {
            uint thiscount = 0;
            {
                int tisample = isample;
                while (tisample < iendsample)
                {
                    thiscount++;
                    int xsame = CharSkip(samples, tisample, iendsample, ichar);
#if DEBUG
                    if (xsame < 1)
                    {
                        throw new Exception("XPopulate: CharSkip returned " + xsame.ToString() + " at character position " + ichar.ToString() + " when expecting " + (iendsample - tisample).ToString() + " more samples");
                    }
#endif
                    tisample += xsame;
                }
            }
#if DEBUG
            // There will always be one child, unless it's a leaf and it doesn't denote children.
            if (0 == thiscount || thiscount > 256)
            {
                throw new Exception("DEBUG: DsDistro: XPopulate: (0 == thiscount || thiscount > 256)");
            }
#endif
            uint table_noffset = alloc_noffset; // readonly
            LeafInfo leafi;
            {
                alloc_noffset += thiscount; // !
                data[table_noffset].siblings_remain = (byte)(thiscount - 1); // # after this one (lets it be bound between 1-256 rather than 0-255)
                uint ientry = 0;
                while (isample < iendsample)
                {
                    int xsame = CharSkip(samples, isample, iendsample, ichar);
#if DEBUG
                    if (xsame < 1)
                    {
                        throw new Exception("XPopulate: CharSkip returned " + xsame.ToString() + " at character position " + ichar.ToString() + " when expecting " + (iendsample - isample).ToString() + " more samples");
                    }
#endif
                    if (ichar + 1 < keymajor)
                    {
                        data[table_noffset + ientry].glyph_value = samples[isample][ichar];
                        uint nextptr = XPopulate(samples, isample, isample + xsame, ichar + 1, ref alloc_noffset, leaves);
                        data[table_noffset + ientry].children_noffset = nextptr;
                    }
                    else
                    {
                        // Leaf...
                        data[table_noffset + ientry].glyph_value = samples[isample][ichar];
                        uint leaf_noffset = table_noffset + ientry; // Offset into data of this int which should be set to (LEAF_BIT | majorminorcode)
                        leafi.sample = samples[isample];
                        leafi.leaf_noffset = leaf_noffset;
                        leaves.Add(leafi);
                    }
                    isample += xsame;
                    ientry++;
                }
            }
            return table_noffset;
        }

        uint XPopulate(IList<IList<byte>> samples, List<LeafInfo> leaves)
        {
            uint alloc_noffset = 0;
            return XPopulate(samples, 0, samples.Count, 0, ref alloc_noffset, leaves);
        }


        public DsDistro(IList<IList<byte>> samples, int majorcount, int minorcount)
        {
#if DEBUG
            //System.Threading.Thread.Sleep(10000);
#endif

            this.majorcount = majorcount;
            this.minorcount = minorcount;

            if (samples.Count < 1)
            {
                return;
            }
            keymajor = samples[0].Count;

            {
                List<IList<byte>> tsamp1 = samples as List<IList<byte>>;
                if (null != tsamp1)
                {
                    tsamp1.Sort(new DsIListByteComparer());
                }
                else
                {
                    IList<byte>[] tsamp2 = samples as IList<byte>[];
                    if (null != tsamp2)
                    {
                        Array.Sort(tsamp2, new DsIListByteComparer());
                    }
                    else
                    {
                        throw new Exception("DsDistro: cannot sort samples list (unknown type)");
                    }
                }
            }

            checked
            {
                int ndataelements = keymajor * samples.Count; // Worst case.
                if (ndataelements >= 0x40000000)
                {
                    throw new Exception("Too much sample data (" + ndataelements.ToString() + " elements)");
                }
#if DSDISTRO_DATA_ARRAY
                data = new Node[ndataelements];
#else
                data = (uint*)System.Runtime.InteropServices.Marshal.AllocHGlobal(new IntPtr(ndataelements * Node.SizeOf));
                _datalen = (int)ndataelements;
#endif
                /* // Note: this is probably slow... Since NO_DATA is 0, skip this.
                for (uint id = 0; id < ndataelements; id++)
                {
                    data[id] = NO_DATA;
                }*/
#if DEBUG
                /*for (uint id = 0; id < ndataelements; id++)
                {
                    if (NO_DATA != data[id])
                    {
                        throw new Exception("AllocHGlobal did not initialize");
                    }
                }*/
#endif
            }

            List<LeafInfo> leaves = new List<LeafInfo>(samples.Count);
            XPopulate(samples, leaves); // Top-level populate returns 0.

            // Round up the leaves...
            //leaves.Sort(); // Already sorted the input.
            _PartitionMajorMinorLeaves(leaves);
#if DEBUG
            for (int ip = 0; ip < leaves.Count; ip++)
            {
                if ((data[leaves[ip].leaf_noffset].children_noffset & LEAF_BIT) != LEAF_BIT)
                {
                    throw new Exception("DsDistro: not all leaf nodes are assigned major/minor values");
                }
            }
#endif
            leaves = null; // Don't need them anymore.

        }


        // Expects the leaves to be sorted.
        void _PartitionMajorMinorLeaves(List<LeafInfo> leaves)
        {
            int chunkymax = 0;

            int iMajorPartitionSize = leaves.Count / (int)majorcount;
            iMajorPartitionSize++;
            int majorremain = leaves.Count % (int)majorcount;

            int iMinorPartitionSize = leaves.Count / (int)(majorcount * minorcount);
            iMinorPartitionSize++;
            int minorremain = leaves.Count % (int)(majorcount * minorcount);

            if (iMinorPartitionSize <= 0)
            {
                iMinorPartitionSize = 1;
            }
            if (iMajorPartitionSize <= 0)
            {
                iMajorPartitionSize = 1;
            }

            short cur_Major_id = 0;
            for (int i = 0; i < leaves.Count; )
            {
                int chunkcurr = 0;
                short cur_Minor_id = 0;
                int j;
                for (j = i; j < i + iMajorPartitionSize && j < leaves.Count; )
                {
                    int k;
                    for (k = j; k < j + iMinorPartitionSize && k < leaves.Count && k < i + iMajorPartitionSize; k++)
                    {
                        LeafInfo pRangeItem = leaves[k];
                        data[pRangeItem.leaf_noffset].children_noffset = LEAF_BIT | ((uint)cur_Major_id * (uint)minorcount + (uint)cur_Minor_id);
                        leaves[k] = pRangeItem;
                    }
                    j = k;
                    if (cur_Minor_id < minorcount - 1)
                    {
                        ++cur_Minor_id;
                    }
                    else
                    {
                        ++chunkcurr;
                    }
                }
                if (chunkcurr > chunkymax)
                {
                    chunkymax = chunkcurr;
                }
                i = j;
                if (cur_Major_id < majorcount - 1)
                {
                    ++cur_Major_id;
                }
            }
        }


        uint _SmallBinarySearch(uint startoffset, uint length, uint value)
        {
            if (length < 1)
            {
                return 0xFFFFFFFF;
            }
            for (; ; )
            {
                {
                    uint midlen = length / 2;
                    uint halfstart = startoffset + midlen;
                    if (data[halfstart].glyph_value == value)
                    {
                        return halfstart;
                    }
                    else if (value > data[halfstart].glyph_value)
                    {
                        if (midlen <= 1)
                        {
                            return halfstart; // Nearest.
                        }
                        //return SmallBinarySearch(halfstart + 1, midlen - 1, value);
                        startoffset = halfstart + 1;
                        length = midlen - 1;
                        //value = value;
                        // Loop...
                    }
                    else //if (value < data[halfstart])
                    {
                        if (midlen < 1)
                        {
                            return startoffset; // Nearest.
                        }
                        //return SmallBinarySearch(startoffset, midlen, value);
                        //startoffset = startoffset;
                        length = midlen;
                        //value = value;
                        // Loop...
                    }
                }
            }
            //return 0xFFFFFFFF;
        }


        // Returns offset of slot. if data[return] isn't idx, it is just nearest.
        uint SlotLookup(uint sampleentry_noffset, byte idx)
        {
            uint nentries = (uint)data[sampleentry_noffset].siblings_remain + 1;
            uint bslot = _SmallBinarySearch(sampleentry_noffset, nentries, idx); // idx is the value.
            return bslot;
        }


        public override RangeNode Distro(IList<byte> key, int keyoffset, int keylength)
        {
            if (keylength != this.keymajor)
            {
                throw new Exception("Key length not of expected size (expected " + this.keymajor.ToString() + ", got " + keylength.ToString() + ")");
            }
            if (keyoffset + keylength > key.Count)
            {
                throw new Exception("Key buffer of size " + key.Count.ToString() + " not big enough to hold key of length " + keylength.ToString() + " at offset " + keyoffset.ToString());
            }
            if (keyoffset < 0 || keylength < 0)
            {
                throw new Exception("Key buffer of size " + key.Count.ToString() + " not big enough to hold key of BAD length " + keylength.ToString() + " at BAD offset " + keyoffset.ToString());
            }

            {
                IList<byte> word = key;
                uint sampleentry_noffset = 0;
                uint x;
                int letterend = keyoffset + keymajor;
                for (int letterindex = keyoffset; letterindex < letterend; letterindex++)
                {
                    byte letter = word[letterindex];
                    uint islot = SlotLookup(sampleentry_noffset, letter);
                    {
                        if (data[islot].glyph_value == letter)
                        {
                            x = data[islot].children_noffset;
                        }
                        else if (data[islot].glyph_value > letter)
                        {
                            // Mine is higher, so always go lower from here.
                            for (int xkmax = 0; ; xkmax++)
                            {
                                if (xkmax > keymajor)
                                {
                                    throw new Exception("DsDistro: Expected leaf node, hit end of key (higher)");
                                }
                                x = data[islot].children_noffset;
                                if ((x & LEAF_BIT) == LEAF_BIT)
                                {
                                    return RangeNodeFromLeaf(x);
                                }
                                sampleentry_noffset = x;
                                islot = sampleentry_noffset;
                            }
                        }
                        else //if (data[islot] < letter)
                        {
                            // Mine is lower, so always go higher from here.
                            for (int xkmax = 0; ; xkmax++)
                            {
                                if (xkmax > keymajor)
                                {
                                    throw new Exception("DsDistro: Expected leaf node, hit end of key (higher)");
                                }
                                x = data[islot].children_noffset;
                                if ((x & LEAF_BIT) == LEAF_BIT)
                                {
                                    return RangeNodeFromLeaf(x);
                                }
                                sampleentry_noffset = x;
                                uint nentries = (uint)data[sampleentry_noffset].siblings_remain + 1;
                                islot = sampleentry_noffset + (nentries - 1); // Last slot.
                            }
                        }
                    }
                    if ((x & LEAF_BIT) == LEAF_BIT)
                    {
                        return RangeNodeFromLeaf(x);
                    }
                    sampleentry_noffset = x;
                }
            }
            throw new Exception("DsDistro: Expected leaf node, hit end of key (normal)");
        }


        [System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Explicit, Size = 6)]
        internal struct Node
        {
            public const int SizeOf = 6;

            [System.Runtime.InteropServices.FieldOffset(0)]
            internal byte glyph_value;

            [System.Runtime.InteropServices.FieldOffset(1)]
            internal byte siblings_remain; // Length info; first child has remaining children (length = remain + 1)

            [System.Runtime.InteropServices.FieldOffset(2)]
            internal uint children_noffset;

        }

    }


}
