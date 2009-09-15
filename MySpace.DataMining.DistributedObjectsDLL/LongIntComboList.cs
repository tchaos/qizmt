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


namespace MySpace.DataMining.DistributedObjects5
{
    public class LongIntComboList : DistObject
    {
        byte[] buf;
        byte nextenumid = 0;
        int addbufinitsize = 0;
        int mindupes = 0;


        public LongIntComboList(string objectname)
            : base(objectname)
        {
            this.buf = new byte[BUF_SIZE];

            EnableAddBuffer(); // ...
        }


        public void SetMinimumDuplicateCount(int mindupes)
        {
            this.mindupes = mindupes;
        }


        // Call with 0 to disable (before any adds!)
        public void EnableAddBuffer(int buffersize)
        {
            addbufinitsize = buffersize;
        }

        public void EnableAddBuffer()
        {
            EnableAddBuffer(FILE_BUFFER_SIZE);
        }


        public override char GetDistObjectTypeChar()
        {
            return 'C'; // 12 hex
        }


        public override void Open()
        {
            base.Open();

            if (mindupes > 0)
            {
                foreach (SlaveInfo slave in dslaves)
                {
                    slave.nstm.WriteByte((byte)'M');
                    DistObject.ToBytes(mindupes, buf, 0);
                    XContent.SendXContent(slave.nstm, buf, 4);
                }
            }
        }


        public virtual void SortBlocks()
        {
            // Enter all the locks...
            foreach (SlaveInfo slave in dslaves)
            {
                System.Threading.Monitor.Enter(slave); // !
            }
            try
            {
                // Sort all concurrently...
                foreach (SlaveInfo _slave in dslaves)
                {
                    BufSlaveInfo slave = (BufSlaveInfo)_slave;

                    //if (addbufinitsize > 0)
                    {
                        slave.FlushAddBuf_unlocked(); // !
                    }

                    slave.nstm.WriteByte((byte)'s'); // Sort.
                }
                // Wait for acks... (join!)
                foreach (SlaveInfo slave in dslaves)
                {
                    if ((byte)'+' != slave.nstm.ReadByte())
                    {
                        throw new Exception("SortBlocks error: sub process " + slave.slaveID.ToString() + " did not return a valid response");
                    }
                }
            }
            finally
            {
                // Release all locks...
                foreach (SlaveInfo slave in dslaves)
                {
                    System.Threading.Monitor.Exit(slave); // !
                }
            }
        }


        public virtual void Flush()
        {
            if (addbufinitsize > 0)
            {
                foreach (SlaveInfo _slave in dslaves)
                {
                    BufSlaveInfo slave = (BufSlaveInfo)_slave;
                    lock (slave)
                    {
                        slave.FlushAddBuf_unlocked();
                    }
                }
            }
        }


        class BufSlaveInfo : SlaveInfo
        {
            byte[] addbuf = null;
            int addbuflen = 0; // Number of valid bytes in addbuf.


            internal void EnsureAddBuf_unlocked(int initsize)
            {
                if (null == addbuf)
                {
                    addbuf = new byte[initsize];
                }
            }


            internal void FlushAddBuf_unlocked()
            {
                if (addbuflen > 0)
                {
                    int xlen = addbuflen;
                    addbuflen = 0;
                    nstm.WriteByte((byte)'p'); // Put/publish batch.
                    XContent.SendXContent(nstm, addbuf, xlen);
                }
            }


            internal void AddBuf_unlocked(byte[] value, int valuelength)
            {
                if (addbuflen + valuelength > addbuf.Length)
                {
                    FlushAddBuf_unlocked();
                    addbuflen = 0;
                    if (valuelength > addbuf.Length)
                    {
                        addbuf = new byte[valuelength];
                    }
                }

                for (int i = 0; i != valuelength; i++)
                {
                    addbuf[addbuflen + i] = value[i];
                }
                addbuflen += valuelength;
            }
        }


        public override SlaveInfo createSlaveInfo()
        {
            return new BufSlaveInfo();
        }


        public virtual void Add(Int64 a, Int32 b)
        {
            if (!didopen)
            {
                throw new Exception("Must Open before writing to Hashtable");
            }

            B12 b12; // = new B12(a, b);
            b12.A = a;
            b12.B = b;

            int slaveID = (int)(b12.A % dslaves.Count);

            /*if (slaveID < 0 || slaveID >= dslaves.Count)
            {
                throw new Exception("Slave missing: slaveID needed: " + slaveID.ToString());
            }*/
            SlaveInfo slave = dslaves[slaveID];

            lock (slave)
            {
                if (addbufinitsize > 0)
                {
                    BufSlaveInfo bslave = (BufSlaveInfo)slave;
                    bslave.EnsureAddBuf_unlocked(addbufinitsize);
                    b12.CopyToArray(buf);
                    bslave.AddBuf_unlocked(buf, 12);
                }
            }
        }


        public virtual LongIntComboListEnumerator[] GetEnumerators()
        {
            if (!didopen)
            {
                throw new Exception("Attempted to GetEnumerators before Open");
            }

            LongIntComboListEnumerator[] results;
            lock (dslaves)
            {
                byte enumid = nextenumid;
                if (nextenumid >= 32)
                    nextenumid = 0;
                else
                    nextenumid++;

                int i;
                results = new LongIntComboListEnumerator[dslaves.Count];

                i = 0;
                foreach (SlaveInfo _slave in dslaves)
                {
                    BufSlaveInfo slave = (BufSlaveInfo)_slave;
                    if (addbufinitsize > 0)
                    {
                        lock (slave)
                        {
                            slave.FlushAddBuf_unlocked();
                        }
                    }
                    results[i] = new LongIntComboListEnumerator(slave, enumid);
                    results[i].buf = this.buf; // Not needed, but fine.
                    i++;
                }
            }
            return results;
        }


        public struct B12 : IComparable
        {
            public Int64 A;
            public Int32 B;


            internal B12(Int64 a, Int32 b)
            {
                this.A = a;
                this.B = b;
            }


            public int CompareTo(object obj)
            {
                B12 that = (B12)obj;
                {
                    long diff = this.A - that.A;
                    if (diff > 0) return 1; else if (diff < 0) return -1;
                }
                {
                    int diff = this.B - that.B; if (0 != diff) return diff;
                }
                return 0;
            }


            public void CopyToArray(byte[] buf)
            {
                DistObjectBase.LongToBytes(A, buf, 0);
                DistObjectBase.ToBytes(B, buf, 8);
            }

            public void SetFromArray(byte[] buf)
            {
                A = DistObjectBase.BytesToLong(buf, 0);
                B = DistObjectBase.BytesToInt(buf, 8);
            }
        }
    }


    public class LongIntComboListEnumerator
    {
        public struct KeyRun
        {
            public Int64 A;
            public Int32 B;
            public int Count;


            public static KeyRun Create(LongIntComboList.B12 b12, int count)
            {
                KeyRun result;
                result.A = b12.A;
                result.B = b12.B;
                result.Count = count;
                return result;
            }


            public LongIntComboList.B12 ToB12()
            {
                LongIntComboList.B12 b12;
                b12.A = A;
                b12.B = B;
                return b12;
            }
        }


        internal SlaveInfo slave;
        internal byte enumid;
        internal bool first = true;


        internal LongIntComboListEnumerator(SlaveInfo slave, byte enumid)
        {
            this.slave = slave;
            this.enumid = enumid;
        }

        protected internal LongIntComboListEnumerator()
        {
        }


        public virtual KeyRun Current
        {
            get
            {
                return KeyRun.Create(cur, currun);
            }
        }


        public virtual bool MoveNext()
        {
            return cangetnext();
        }


        public virtual void Reset()
        {
            lock (slave)
            {
                cur = new LongIntComboList.B12(); // ...
                internalsendreset();
            }
        }


        private LongIntComboList.B12 cur;
        int currun = 0;
        public byte[] buf;

        public byte[] nextbuf; // Buffering the 'next' elements; null at first!
        public int nextbufpos = 0; // Where in nextbuf the valid elements start.
        public int nextbufend = 0; // Where in nextbuf the valid elements end.

        internal bool cangetnext()
        {
            lock (slave)
            {
                if (first)
                {
                    first = false;
                    internalsendreset();
                }

                if (nextbufpos < nextbufend)
                {
                    fillcurfromnextbuf();
                    return true;
                }
                else
                {
                    // 'e for batch next..
                    slave.nstm.WriteByte((byte)'e');
                    slave.nstm.WriteByte(enumid);

                    // Replies with tag '+' and xcontent values; or tag '-' if not exists.
                    //int len;
                    int x = slave.nstm.ReadByte();
                    if ('+' == x)
                    {
                        nextbufpos = 0;
                        nextbufend = 0;
                        nextbuf = XContent.ReceiveXBytes(slave.nstm, out nextbufend, nextbuf);
                        fillcurfromnextbuf();
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
        }

        internal void getnext()
        {
            if (!cangetnext())
            {
                throw new InvalidOperationException("Past end of enumeration");
            }
        }


        internal void fillcurfromnextbuf()
        {
            if (nextbufpos + 12 > nextbufend)
            {
                throw new Exception("Unaligned values (enumeration buffer)");
            }

            currun = 1;

            cur.A = DistObjectBase.BytesToLong(nextbuf, nextbufpos);
            cur.B = DistObjectBase.BytesToInt(nextbuf, nextbufpos + 8);

            nextbufpos += 12;
        }


        internal void internalsendreset()
        {
            // 'n' for reset next..
            slave.nstm.WriteByte((byte)'n');
            slave.nstm.WriteByte(enumid);
        }
    }
}
