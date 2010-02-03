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
    public struct RangeNode
    {
        public ushort MajorID;
        public ushort MinorID;

        public const int SizeOf = 2 + 2;

    }

    public abstract class RangeDistro
    {
        public abstract RangeNode Distro(IList<byte> key, int keyoffset, int keylength);

        public virtual RangeNode Distro(string word)
        {
            byte[] key = Encoding.ASCII.GetBytes(word);
            return Distro(key, 0, key.Length);
        }

        public virtual RangeNode Distro(IList<byte> key)
        {
            return Distro(key, 0, key.Count);
        }

        public abstract int GetMemoryUsage();

    }

}

