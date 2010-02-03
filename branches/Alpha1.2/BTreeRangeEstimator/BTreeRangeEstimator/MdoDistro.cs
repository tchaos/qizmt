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

    public partial class MdoDistro : RangeDistro
    {
        public int m_iMajorCount = 1;
        public int m_iMinorCount = 1;
        int iSampleCount = -1;
        public GlyphNode tree = GlyphNode.Prepare((byte)'\0', 0, 0);
        public int m_iSampleKeyLength = -1;

        public MdoDistro(IList<IList<byte>> samples, int iMajorCount, int iMinorCount)
        {
            if (samples.Count > 0)
            {
                m_iSampleKeyLength = samples[0].Count;
            }
            m_iMajorCount = iMajorCount;
            m_iMinorCount = iMinorCount;
            iSampleCount = samples.Count;
            for (int i = 0; i < iSampleCount; i++)
            {
                if (samples[i].Count != m_iSampleKeyLength)
                {
                    // Fail case...
                    StringBuilder keysb = new StringBuilder(samples[i].Count);
                    for (int ki = 0; ki < samples[i].Count; ki++)
                    {
                        if (keysb.Length > 0)
                        {
                            keysb.Append(',');
                        }
                        keysb.Append((byte)samples[i][ki]);
                    }
                    throw new Exception("Not all samples have the same key length (expected " + m_iSampleKeyLength.ToString() + ", got " + samples[i].Count.ToString() + "). Consider recreate input or delete cache file if caching on. {key=" + keysb.ToString() + "}");
                }
                IList<byte> word = samples[i];
                tree.EatWord(word, 0);
            }
            GlyphNode.IndexNodes(tree);

            try
            {
                GlyphNode.leaf_nodes.Sort(new cmprGlyphNode());

                PartitionMajorAndMinor(GlyphNode.leaf_nodes, m_iMajorCount, m_iMinorCount);

                GlyphNode.leaf_nodes = null; // Not needed anymore.
            }
            catch
            {
            }

        }

        private void PartitionMajorAndMinor(List<GlyphNode> rConverted_Samples, int iMajorCount, int iMinorCount)
        {
            int slotspermajor = rConverted_Samples.Count / iMajorCount;
            int extramajorslots = rConverted_Samples.Count % iMajorCount;
            if (0 != extramajorslots)
            {
                slotspermajor++;
            }
            int slotsperminor = slotspermajor / iMinorCount;
            int extraminorslots = slotspermajor % iMinorCount;
            if (0 != extraminorslots)
            {
                slotsperminor++;
            }
            ushort major = 0;
            int majorslot = 0;
            ushort minor = 0;
            int minorslot = 0;
            for (int i = 0; i < rConverted_Samples.Count; i++)
            {
                if (major >= iMajorCount)
                {
                    throw new Exception("MajorID miscalculation for " + this.GetType().Name + ": out of range");
                }
                if (minor >= iMinorCount)
                {
                    throw new Exception("MinorID miscalculation for " + this.GetType().Name + ": out of range");
                }
                //ranges[i] = new RangeNode();
                GlyphNode gn = rConverted_Samples[i];
                gn.iMajorId = major;
                gn.iMinorId = minor;
                rConverted_Samples[i] = gn;
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
                        extraminorslots = slotspermajor % iMinorCount;
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

        public override RangeNode Distro(IList<byte> key, int keyoffset, int keylength)
        {
            RangeNode rn;
            if (tree.child_glyphs == null)
            {
                rn.MajorID = tree.iMajorId;
                rn.MinorID = tree.iMinorId;
                return rn;
            }

            if (keylength != m_iSampleKeyLength)
            {
                throw new Exception("Key length not of expected size (expected " + m_iSampleKeyLength.ToString() + ", got " + keylength.ToString() + ")");
            }
            if (keyoffset + keylength > key.Count)
            {
                throw new Exception("Key buffer of size " + key.Count.ToString() + " not big enough to hold key of length " + keylength.ToString() + " at offset " + keyoffset.ToString());
            }
            if (keyoffset < 0 || keylength < 0)
            {
                throw new Exception("Key buffer of size " + key.Count.ToString() + " not big enough to hold key of BAD length " + keylength.ToString() + " at BAD offset " + keyoffset.ToString());
            }
            GlyphNode gn = tree.FindWord(tree, key, keyoffset, keylength);
            rn.MajorID = gn.iMajorId;
            rn.MinorID = gn.iMinorId;
            return rn;
        }

        public override int GetMemoryUsage()
        {
            checked
            {
                int mu = 4 + 4 + 4 + IntPtr.Size + 4;
                if (null != tree)
                {
                    mu += tree._GetMemoryUsage();
                }
                return mu;
            }
        }

    }
}
