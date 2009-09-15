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
using System.Net;
using System.Net.Sockets;


namespace MySpace.DataMining.DistributedObjects5
{
    public class HashtableObjectPart : DistObjectBase
    {
        public static bool UseStringKeys = false;

        static HOPComparer hopcmp = new HOPComparer();
        System.Collections.IDictionaryEnumerator[] enums;
        int appendbuffersize = -1;

        System.Collections.Hashtable table;


        /*
        public System.Collections.IEnumerator GetEnumerator()
        {
            return table.GetEnumerator();
        }
         * */


        public bool AppendBufferingEnabled
        {
            get
            {
                return appendbuffersize >= 0;
            }
        }


        // Warning: return's Length is not valid data length, but preallocation buffer.
        public override byte[] GetValue(byte[] key, out int valuelength)
        {
            object ovalue;
            if (UseStringKeys)
            {
                ovalue = table[Encoding.ASCII.GetString(key)];
            }
            else
            {
                ovalue = table[key];
            }
            if (null == ovalue)
            {
                valuelength = -1;
                return null;
            }

            if (AppendBufferingEnabled)
            {
                AppendBuffer abvalue = (AppendBuffer)ovalue;
                return abvalue.Get(out valuelength);
            }
            else
            {
                byte[] value = (byte[])ovalue;
                valuelength = value.Length;
                return value;
            }
        }


        // Warning: key needs to be immutable!
        public override void CopyAndSetValue(byte[] key, byte[] value, int valuelength)
        {
            if (AppendBufferingEnabled)
            {
                object ovalue;
                if (UseStringKeys)
                {
                    ovalue = table[Encoding.ASCII.GetString(key)];
                }
                else
                {
                    ovalue = table[key];
                }
                if (null != ovalue)
                {
                    AppendBuffer abvalue = (AppendBuffer)ovalue;
                    abvalue.Set(value, valuelength);
                    //if(UseStringKeys) x else table[key] = abvalue;
                }
                else
                {
                    int valinitsize = valuelength * 2;
                    if (valinitsize < appendbuffersize)
                    {
                        valinitsize = appendbuffersize;
                    }
                    AppendBuffer abvalue = new AppendBuffer(valinitsize);
                    abvalue.Set(value, valuelength);
                    if (UseStringKeys)
                    {
                        table[Encoding.ASCII.GetString(key)] = abvalue;
                    }
                    else
                    {
                        table[key] = abvalue;
                    }
                }
            }
            else
            {
                if (UseStringKeys)
                {
                    table[Encoding.ASCII.GetString(key)] = GetSliceCopy(value, valuelength);
                }
                else
                {
                    table[key] = GetSliceCopy(value, valuelength);
                }
            }
        }


        // Warning: key needs to be immutable (for when append is actually just a set).
        // avalue can be mutable.
        public override void AppendValue(byte[] key, byte[] avalue, int avaluelength)
        {
            object ovalue = table[key];
            if (null == ovalue)
            {
                CopyAndSetValue(key, avalue, avaluelength);
            }
            else
            {
                if (AppendBufferingEnabled)
                {
                    AppendBuffer abvalue = (AppendBuffer)ovalue;
                    abvalue.Append(avalue, avaluelength);
                    //table[key] = abvalue;
                }
                else
                {
                    byte[] oldvalue = (byte[])ovalue;
                    table[key] = AppendSliceCopy(oldvalue, avalue, avaluelength);
                }
            }
        }


        public static HashtableObjectPart Create(int capacity)
        {
            return new HashtableObjectPart(capacity);
        }


        private HashtableObjectPart(int capacity)
        {
            if (UseStringKeys)
            {
                this.table = new System.Collections.Hashtable(capacity);
            }
            else
            {
                this.table = new System.Collections.Hashtable(capacity, (System.Collections.IEqualityComparer)hopcmp);
            }
            this.enums = new System.Collections.IDictionaryEnumerator[32];
        }


        protected override void ProcessCommand(NetworkStream nstm, char tag)
        {
            //string s;
            int len;

            switch (tag)
            {
                case 'N': // Next in enumeration..
                    {
                        int ienumid = nstm.ReadByte();
                        if (ienumid >= 0)
                        {
                            byte enumid = (byte)ienumid;
                            if (enumid >= this.enums.Length)
                            {
                                nstm.WriteByte((byte)'-');
                            }
                            else
                            {
                                if (null == this.enums[enumid])
                                {
                                    this.enums[enumid] = table.GetEnumerator();
                                }
                                if (this.enums[enumid].MoveNext())
                                {
                                    nstm.WriteByte((byte)'+');
                                    System.Collections.DictionaryEntry de = this.enums[enumid].Entry;
                                    XContent.SendXContent(nstm, (byte[])de.Key);
                                    if (AppendBufferingEnabled)
                                    {
                                        AppendBuffer abvalue = (AppendBuffer)de.Value;
                                        byte[] result = abvalue.Get(out len);
                                        XContent.SendXContent(nstm, result, len);
                                    }
                                    else
                                    {
                                        XContent.SendXContent(nstm, (byte[])de.Value);
                                    }
                                }
                                else
                                {
                                    nstm.WriteByte((byte)'-');
                                }
                            }
                        }
                    }
                    break;

                case 'n': // Reset next in enumeration..
                    {
                        int ienumid = nstm.ReadByte();
                        if (ienumid >= 0)
                        {
                            byte enumid = (byte)ienumid;
                            if (XLog.logging)
                            {
                                XLog.log("Starting enumeration (enumid:" + enumid.ToString() + ")");
                            }
                            if (enumid < this.enums.Length
                                && null != this.enums[enumid])
                            {
                                //this.enums[enumid].Reset();
                                this.enums[enumid] = null;
                            }
                        }
                    }
                    break;

                case 'b': // Enable AppendBuffer
                    XContent.ReceiveXBytes(nstm, out len, buf);
                    appendbuffersize = BytesToInt(buf);
                    if (appendbuffersize < 0 || appendbuffersize >= 16777216)
                    {
                        throw new Exception("Invalid AppendBuffer preallocation size (appendbuffersize): " + appendbuffersize.ToString());
                    }
                    if (XLog.logging)
                    {
                        XLog.log("Enabled AppendBuffer with initial value sizes of " + appendbuffersize.ToString());
                    }
                    break;

                case 'L': // Set length..
                    {
                        // Key..
                        byte[] putkey = XContent.ReceiveXBytes(nstm, out len, buf);
                        putkey = GetSliceCopy(putkey, len); // !
                        // New length..
                        byte[] putlength = XContent.ReceiveXBytes(nstm, out len, buf);
                        int newlength = BytesToInt(putlength);

                        if (AppendBufferingEnabled)
                        {
                            object ovalue;
                            if (UseStringKeys)
                            {
                                ovalue = table[Encoding.ASCII.GetString(putkey)];
                            }
                            else
                            {
                                ovalue = table[putkey];
                            }
                            if (null == ovalue)
                            {
                                int setbufsize = newlength;
                                if (setbufsize < appendbuffersize)
                                {
                                    setbufsize = appendbuffersize; // ?
                                }
                                if (UseStringKeys)
                                {
                                    table[Encoding.ASCII.GetString(putkey)] = new AppendBuffer(setbufsize);
                                }
                                else
                                {
                                    table[putkey] = new AppendBuffer(setbufsize);
                                }
                            }
                            else
                            {
                                AppendBuffer abvalue = (AppendBuffer)ovalue;
                                abvalue.SetLength(newlength);
                            }
                        }
                        else
                        {
                            byte[] oldvalue = GetValue(putkey, out len);
                            if (newlength < len)
                            {
                                CopyAndSetValue(putkey, oldvalue, newlength);
                            }
                        }
                    }
                    break;

                case 'G': // Get..
                    {
                        //Thread.Sleep(15000);

                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        byte[] getkey = GetSliceCopy(buf, len);

                        byte[] result = GetValue(getkey, out len);
                        if (null == result)
                        {
                            nstm.WriteByte((byte)'-'); // Key doesn't exist!
                        }
                        else
                        {
                            nstm.WriteByte((byte)'+'); // It exists!
                            XContent.SendXContent(nstm, result, len);
                        }
                    }
                    break;

                case 'l': // Get length..
                    {
                        byte[] getkey = XContent.ReceiveXBytes(nstm, out len, buf);
                        getkey = GetSliceCopy(getkey, len);

                        byte[] value = GetValue(getkey, out len);
                        if (null == value)
                        {
                            nstm.WriteByte((byte)'-'); // Key doesn't exist!
                        }
                        else
                        {
                            nstm.WriteByte((byte)'+'); // It exists!
                            XContent.SendXContent(nstm, ToBytes(len));
                        }
                    }
                    break;

                case 'P': // Put..
                    {
                        // Key..
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        byte[] putkey = GetSliceCopy(buf, len);
                        // Value..
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        CopyAndSetValue(putkey, buf, len);
                    }
                    break;

                case 'a': // Math add..
                    {
                        // Key..
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        byte[] putkey = GetSliceCopy(buf, len); // !
                        // Operand..
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        if (buf.Length >= 4)
                        {
                            int operand = BytesToInt(buf);
                            // Do add...
                            byte[] val = GetValue(putkey, out len);
                            if (null == val || val.Length < 4) // Key doesn't exist, or too small, so assume it was 0.
                            {
                                ToBytes(operand, buf, 0);
                                CopyAndSetValue(putkey, buf, 4);
                            }
                            else
                            {
                                int existingint = BytesToInt(val);
                                int result = existingint + operand;
                                //XLog.log("blarg", "adding " + existingint.ToString() + " and " + operand.ToString() + ", got result " + result.ToString());
                                ToBytes(result, buf, 0);
                                CopyAndSetValue(putkey, buf, 4);
                            }
                        }
                    }
                    break;

                case 'A': // Append..
                    {
                        // Key..
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        byte[] putkey = GetSliceCopy(buf, len);
                        // Value..
                        buf = XContent.ReceiveXBytes(nstm, out len, buf);
                        AppendValue(putkey, buf, len);
                    }
                    break;

                default:
                    base.ProcessCommand(nstm, tag);
                    break;
            }
        }
        
    }


    internal class HOPComparer : System.Collections.IEqualityComparer, System.Collections.IComparer
    {
        int gethashcode(byte[] x, int xlen)
        {
            unchecked
            {
                // Note: different from DLL's DetermineSlave so things distribute better here.
                int hc = 0;
                for (int i = 0; i != xlen; i++)
                {
                    hc = (int)x[i] + (hc << 6) + (hc << 16) - hc;
                }
                return hc & 0x7FFFFFFF;
            }
        }


        public int GetHashCode(Object obj)
        {
            byte[] x = obj as byte[];
            if (null != x)
            {
                return gethashcode(x, x.Length);
            }
            else
            {
                AppendBuffer abx = obj as AppendBuffer;
                if (null != abx)
                {
                    return gethashcode(abx.buf, abx.GetLength());
                }
            }
            return obj.GetHashCode();
        }


        int cmp(byte[] x, int xlen, byte[] y, int ylen)
        {
            if (xlen != ylen)
            {
                return xlen - ylen;
            }
            for (int i = 0; i != xlen; i++)
            {
                int diff = x[i] - y[i];
                if (0 != diff)
                    return diff;
            }
            return 0;
        }


        public new bool Equals(object ox, object oy)
        {
            byte[] x = ox as byte[];
            if (null != x)
            {
                byte[] y = (byte[])oy;
                return 0 == cmp(x, x.Length, y, y.Length);
            }
            else
            {
                AppendBuffer abx = ox as AppendBuffer;
                if (null != x)
                {
                    AppendBuffer aby = oy as AppendBuffer;
                    return 0 == cmp(abx.buf, abx.GetLength(), aby.buf, aby.GetLength());
                }
            }

            return ox.Equals(oy);
        }


        public int Compare(object ox, object oy)
        {
            byte[] x = ox as byte[];
            if (null != x)
            {
                byte[] y = (byte[])oy;
                return cmp(x, x.Length, y, y.Length);
            }
            else
            {
                AppendBuffer abx = ox as AppendBuffer;
                if (null != x)
                {
                    AppendBuffer aby = oy as AppendBuffer;
                    return cmp(abx.buf, abx.GetLength(), aby.buf, aby.GetLength());
                }
            }

            return -999; // ?
        }
    }

}
