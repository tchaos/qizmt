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
    public class Hashtable : DistObject
    {
        int appendbuffersize = -1;
        byte[] buf;
        byte nextenumid = 0;


        public Hashtable(string objectname) :
            base(objectname)
        {
            this.buf = new byte[BUF_SIZE];
        }


        public override char GetDistObjectTypeChar()
        {
            return 'H';
        }


        public void EnableAppendBuffer(int initialbuffersize)
        {
            if (didopen)
            {
                throw new Exception("Attempted to EnableAppendBuffer after Open");
            }

            appendbuffersize = initialbuffersize;
        }


        public System.Collections.IDictionaryEnumerator[] GetEnumerators()
        {
            if (!didopen)
            {
                throw new Exception("Attempted to GetEnumerators before Open");
            }

            System.Collections.IDictionaryEnumerator[] results;
            lock (dslaves)
            {
                byte enumid = nextenumid;
                if (nextenumid >= 32)
                    nextenumid = 0;
                else
                    nextenumid++;

                int i;
                results = new System.Collections.IDictionaryEnumerator[dslaves.Count];

                i = 0;
                foreach (SlaveInfo slave in dslaves)
                {
                    results[i++] = new HashtablePartEnumerator(slave, enumid);
                }
            }
            return results;
        }


        public void Add(byte[] key, byte[] value)
        {
            if (!didopen)
            {
                throw new Exception("Must Open before writing to Hashtable");
            }

            int slaveID = DetermineSlave(key);

            if (slaveID < 0 || slaveID >= dslaves.Count)
            {
                throw new Exception("Sub process missing: subProcessID needed: " + slaveID.ToString());
            }
            SlaveInfo slave = dslaves[slaveID];

            lock (slave)
            {
                // 'P' for publish/put
                slave.nstm.WriteByte((byte)'P');
                XContent.SendXContent(slave.nstm, key);
                XContent.SendXContent(slave.nstm, value);
                // Note: no confirmation
            }
        }


        public void Append(byte[] key, byte[] value)
        {
            if (!didopen)
            {
                throw new Exception("Must Open before writing to Hashtable");
            }

            int slaveID = DetermineSlave(key);

            if (slaveID < 0 || slaveID >= dslaves.Count)
            {
                throw new Exception("Sub process missing: subProcessID needed: " + slaveID.ToString());
            }
            SlaveInfo slave = dslaves[slaveID];

            lock (slave)
            {
                // 'A' for append
                slave.nstm.WriteByte((byte)'A');
                XContent.SendXContent(slave.nstm, key);
                XContent.SendXContent(slave.nstm, value);
                // Note: no confirmation
            }
        }


        // key is which key to add to the value.
        public void MathAdd(byte[] key, int operand)
        {
            if (!didopen)
            {
                throw new Exception("Must Open before writing to Hashtable");
            }

            int slaveID = DetermineSlave(key);

            if (slaveID < 0 || slaveID >= dslaves.Count)
            {
                throw new Exception("Sub process missing: subProcessID needed: " + slaveID.ToString());
            }
            SlaveInfo slave = dslaves[slaveID];

            lock (slave)
            {
                // 'a' for math add
                slave.nstm.WriteByte((byte)'a');
                XContent.SendXContent(slave.nstm, key);
                XContent.SendXContent(slave.nstm, ToBytes(operand));
                // Note: no confirmation
            }
        }


        public void Length(byte[] key, int setlength)
        {
            if (!didopen)
            {
                throw new Exception("Must Open before writing to Hashtable");
            }

            int slaveID = DetermineSlave(key);

            if (slaveID < 0 || slaveID >= dslaves.Count)
            {
                throw new Exception("Sub process missing: subProcessID needed: " + slaveID.ToString());
            }
            SlaveInfo slave = dslaves[slaveID];

            lock (slave)
            {
                // 'L' for set length
                slave.nstm.WriteByte((byte)'L');
                XContent.SendXContent(slave.nstm, key);
                XContent.SendXContent(slave.nstm, DistObjectBase.ToBytes(setlength));
                // Note: no confirmation
            }
        }

        // Returns -1 if no such key.
        public int Length(byte[] key)
        {
            if (!didopen)
            {
                throw new Exception("Must Open before writing to Hashtable");
            }

            int slaveID = DetermineSlave(key);

            if (slaveID < 0 || slaveID >= dslaves.Count)
            {
                throw new Exception("Sub process missing: subProcessID needed: " + slaveID.ToString());
            }
            SlaveInfo slave = dslaves[slaveID];

            lock (slave)
            {
                // 'l' for get length
                slave.nstm.WriteByte((byte)'l');
                XContent.SendXContent(slave.nstm, key);

                // Replies with tag '+' and xcontent length; or tag '-' if not exists.
                byte[] result = null;
                int len;
                int x = slave.nstm.ReadByte();
                if ('+' == x)
                {
                    result = XContent.ReceiveXBytes(slave.nstm, out len, buf);
                    return DistObjectBase.BytesToInt(result); // Only reads first one.
                }
                else if ('-' == x)
                {
                    //result = null;
                    return -1;
                }
                else
                {
                    throw new Exception("Server returned invalid response for l (Length)");
                }
            }
        }


        // Gives null if no such key.
        public byte[] Get(byte[] key)
        {
            if (!didopen)
            {
                throw new Exception("Must Open before reading from Hashtable");
            }

            int slaveID = DetermineSlave(key);

            if (slaveID < 0 || slaveID >= dslaves.Count)
            {
                throw new Exception("Sub process missing: subProcessID needed: " + slaveID.ToString());
            }
            SlaveInfo slave = dslaves[slaveID];

            lock (slave)
            {
                // 'G' for get
                slave.nstm.WriteByte((byte)'G');
                XContent.SendXContent(slave.nstm, key);

                // Replies with tag '+' and xcontent value; or tag '-' if not exists.
                byte[] result = null;
                int len;
                int x = slave.nstm.ReadByte();
                if ('+' == x)
                {
                    result = XContent.ReceiveXBytes(slave.nstm, out len, null); // Null buffer for copy.
                }
                else if ('-' == x)
                {
                    //result = null;
                }
                else
                {
                    throw new Exception("Server returned invalid response for G (get)");
                }
                return result;
            }
        }


        // get: gives null if no such key.
        public byte[] this[byte[] key]
        {
            get
            {
                return Get(key);
            }

            set
            {
                Add(key, value);
            }
        }


        public bool ContainsKey(byte[] key)
        {
            return -1 != Length(key);
        }


        public override void Open()
        {
            base.Open();

            byte[] bappendbuffersize = null;
            if (appendbuffersize >= 0)
            {
                bappendbuffersize = ToBytes(appendbuffersize);

                foreach (SlaveInfo slave in dslaves)
                {
                    //if (appendbuffersize >= 0)
                    {
                        slave.nstm.WriteByte((byte)'b'); // EnableAppendBuffer
                        XContent.SendXContent(slave.nstm, bappendbuffersize);
                    }
                }
            }
        }

    }


    internal class HashtablePartEnumerator : System.Collections.IDictionaryEnumerator
    {
        internal SlaveInfo slave;
        internal byte enumid;
        internal bool first = true;


        internal HashtablePartEnumerator(SlaveInfo slave, byte enumid)
        {
            this.slave = slave;
            this.enumid = enumid;
        }


        public bool MoveNext()
        {
            return cangetnext();
        }


        public void Reset()
        {
            lock (slave)
            {
                cur = new System.Collections.DictionaryEntry();
                internalsendreset();
            }
        }


        public object Current
        {
            get
            {
                return Entry;
            }
        }


        public object Key
        {
            get
            {
                return Entry.Key;
            }
        }


        public object Value
        {
            get
            {
                return Entry.Value;
            }
        }


        public System.Collections.DictionaryEntry Entry
        {
            get
            {
                if (null == cur.Key)
                {
                    //getnext();
                }
                return cur;
            }
        }


        private System.Collections.DictionaryEntry cur;

        internal bool cangetnext()
        {
            lock (slave)
            {
                if (first)
                {
                    first = false;
                    internalsendreset();
                }

                // 'N' for next..
                slave.nstm.WriteByte((byte)'N');
                slave.nstm.WriteByte(enumid);

                // Replies with tag '+' and xcontent key and xcontent value; or tag '-' if not exists.
                int len;
                int x = slave.nstm.ReadByte();
                if ('+' == x)
                {
                    byte[] key = XContent.ReceiveXBytes(slave.nstm, out len, null); // Null buffer for copy.
                    byte[] value = XContent.ReceiveXBytes(slave.nstm, out len, null); // Null buffer for copy.
                    cur = new System.Collections.DictionaryEntry(key, value);
                    return true;
                }
                else if ('-' == x)
                {
                    return false;
                }
                else
                {
                    throw new Exception("Server returned invalid response for enumeration");
                }
            }
        }

        internal void getnext()
        {
            if (!cangetnext())
            {
                throw new InvalidOperationException("Past end of enumeration");
            }
        }


        internal void internalsendreset()
        {
            // 'n' for reset next..
            slave.nstm.WriteByte((byte)'n');
            slave.nstm.WriteByte(enumid);
        }
    }

}

