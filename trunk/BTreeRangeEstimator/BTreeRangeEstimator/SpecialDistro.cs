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

    public class Distro1 : RangeDistro
    {
        const int RANGE_LIMIT = 256;

        RangeNode[] ranges;


        public override int GetMemoryUsage()
        {
            checked
            {
                int mu = IntPtr.Size;
                if (null != ranges)
                {
                    mu += ranges.Length * RangeNode.SizeOf;
                }
                return mu;
            }
        }


        public Distro1(int iMajorCount, int iMinorCount)
        {
#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 10);
#endif
            ranges = new RangeNode[RANGE_LIMIT];

            int slotspermajor = RANGE_LIMIT / iMajorCount;
            int extramajorslots = RANGE_LIMIT % iMajorCount;
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
            for (int i = 0; i < RANGE_LIMIT; i++)
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
                ranges[i].MajorID = major;
                ranges[i].MinorID = minor;
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
            return ranges[key[keyoffset]];
        }

    }


    public class Distro2 : RangeDistro
    {
        const int RANGE_LIMIT = 65536;

        RangeNode[] ranges;


        public override int GetMemoryUsage()
        {
            checked
            {
                int mu = IntPtr.Size;
                if (null != ranges)
                {
                    mu += ranges.Length * RangeNode.SizeOf;
                }
                return mu;
            }
        }


        public Distro2(int iMajorCount, int iMinorCount)
        {
#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 10);
#endif
            ranges = new RangeNode[RANGE_LIMIT];

            int slotspermajor = RANGE_LIMIT / iMajorCount;
            int extramajorslots = RANGE_LIMIT % iMajorCount;
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
            for (int i = 0; i < RANGE_LIMIT; i++)
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
                ranges[i].MajorID = major;
                ranges[i].MinorID = minor;
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
            return ranges[(key[keyoffset + 0] << 8) + key[keyoffset + 1]];
        }

    }

}

