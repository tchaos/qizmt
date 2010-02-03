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

namespace MySpace.DataMining.DistributedObjects
{
	public class Int64HTSlot
	{
		public Int64 key = 0;
		public int score = 0;
		public Int64[] values; // 0..score are valid, score..end are garbage.
	}

	public class Int64HT
	{
		public List<int> dirtyslots;
		public Int64HTSlot[] slots;
		public int maxvalues = 0;


		// Note: hashsize should be prime near (maxentries*10)
		public Int64HT(int maxentries, int hashsize, int maxvalues)
		{
			this.maxvalues = maxvalues;
			dirtyslots = new List<int>(maxentries);
			slots = new Int64HTSlot[hashsize];
			for (int hi = 0; hi != slots.Length; hi++)
			{
				slots[hi] = new Int64HTSlot();
				if (maxvalues > 0)
				{
					slots[hi].values = new Int64[maxvalues];
				}
			}
		}

		public Int64HT(int maxentries, int hashsize)
			: this(maxentries, hashsize, 0)
		{
		}

		public Int64HT(int maxvalues)
			: this(28 * 28, 7853, maxvalues)
		{
		}

		public Int64HT()
			: this(0)
		{
		}


		public void Add(Int64 id, Int64 value)
		{
			int i = FindSlot(id);
			slots[i].key = id;
			if (0 == slots[i].score)
			{
				dirtyslots.Add(i);
			}
			if (slots[i].score < maxvalues)
			{
				slots[i].values[slots[i].score] = value;
			}
			slots[i].score++;
		}

		public void Add(Int32 id)
		{
			Add(id, 0);
		}


		// Note: id==0 not reliable.
		public bool ContainsKey(Int64 id)
		{
			int i = FindSlot(id);
			return slots[i].key == id;
		}

		public bool Contains(Int64 id)
		{
			int i = FindSlot(id);
			return slots[i].key == id && 0 != slots[i].score;
		}


		// Doesn't un-dirty.
		public void UnsetKey(Int64 id)
		{
			UnsetSlot(FindSlot(id));
		}

		// Doesn't un-dirty.
		public void UnsetSlot(int slotindex)
		{
			slots[slotindex].key = 0;
			slots[slotindex].score = 0;
		}


		protected int FindSlot(Int64 id)
		{
			int collide = 0;
			int i = (int)(id % slots.Length);
			while (0 != slots[i].key && id != slots[i].key)
			{
				if (++collide == slots.Length)
				{
					throw new Exception("DHT infinite loop: hash table is full");
				}
				i++;
				if (i == slots.Length)
				{
					i = 0;
				}
			}
			return i;
		}


		// Assumes dirtied slots were cleaned.
		public void ZeroDirty()
		{
			dirtyslots.Clear();
		}

		// Cleans dirtied slots.
		public void DeepClean()
		{
			foreach (int i in dirtyslots)
			{
				slots[i].key = 0;
				slots[i].score = 0;
			}
			ZeroDirty();
		}
	}
}
