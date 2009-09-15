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
    public struct DGlobals
    {
        internal static Dictionary<string, string> vars = null;
        private static string[] allkeys = null;      

        static DGlobals()
        {
            vars = new Dictionary<string, string>();
        }

        public static void Add(string key, string value)
        {
            if (StaticGlobals.ExecutionContext != ExecutionContextType.LOCAL)
            {
                throw new Exception("DGlobals.Add can only be executed within a Local job");
            }
            _add(key, value);
        }

        internal static void _add(string key, string value)
        {
            if (key == null || key.Length == 0)
            {
                throw new Exception("DGlobals.Add Error:  Key cannot be empty or null.");
            }

            if (!vars.ContainsKey(key))
            {
                if (vars.Count >= StaticGlobals.DSpace_MaxDGlobals)
                {
                    return;
                }
            }

            vars[key] = value;
            
            if (StaticGlobals.ExecutionMode == ExecutionMode.DEBUG)
            {
                byte[] buf = KeyValueToBytes(key, value);
                Console.WriteLine("{0}{1}", "{B5F03A4C-F06F-49a7-A4C4-FBD9292FFB93}", Convert.ToBase64String(buf));
            }
        }

        public static string Get(string key)
        {
            return vars[key];
        }

        public static int GetCount()
        {
            return vars.Count;
        }

        public static string[] GetAllKeys()
        {
            if (allkeys == null || allkeys.Length != vars.Count)
            {
                allkeys = new string[vars.Count];
            }

            int i = 0;
            foreach (string key in vars.Keys)
            {
                allkeys[i++] = key;
            }
            return allkeys;
        }

        internal static int KeyValueToBytes(string key, string value, byte[] buf, int offset)
        {
            int keyLen = key.Length * 2;
            int valueLen = (value == null) ? -1 : value.Length * 2;
            Entry.ToBytes(keyLen, buf, offset);
            Entry.ToBytes(valueLen, buf, offset + 4);
            System.Text.Encoding.Unicode.GetBytes(key, 0, key.Length, buf, offset + 8);
            if (valueLen > 0)
            {
                System.Text.Encoding.Unicode.GetBytes(value, 0, value.Length, buf, offset + 8 + keyLen);
            }
            return 4 + 4 + keyLen + (valueLen > 0 ? valueLen : 0);
        }

        internal static byte[] KeyValueToBytes(string key, string value)
        {
            byte[] buf = new byte[4 + 4 + key.Length * 2 + (value == null ? 0 : (value.Length * 2))];
            KeyValueToBytes(key, value, buf, 0);
            return buf;
        }

        internal static int BytesToKeyValue(byte[] buf, int offset, ref string key, ref string value)
        {
            int keyLen = Entry.BytesToInt(buf, offset);
            int valueLen = Entry.BytesToInt(buf, offset + 4);
            key = System.Text.Encoding.Unicode.GetString(buf, offset + 8, keyLen);
            value = null;
            if (valueLen > 0)
            {
                value = System.Text.Encoding.Unicode.GetString(buf, offset + 8 + keyLen, valueLen);
            }
            else if (valueLen == 0)
            {
                value = "";
            }
            return 4 + 4 + keyLen + (valueLen > 0 ? valueLen : 0);
        }
    }

    public struct DGlobalsM
    {
        private static string sbuf = null;

        public static string ToCode()
        {
            if (sbuf != null && sbuf.Length > 0)
            {
                return "DGlobalsM.FromBase64String(\"" + sbuf + "\");";
            }
            return "";
        }

        public static int ToBytes(ref byte[] buf)
        {            
            int slen = 0;
            foreach(KeyValuePair<string, string> pair in DGlobals.vars)
            {
                string key = pair.Key;
                string value = pair.Value;
                slen += key.Length;
                if (value != null)
                {
                    slen += value.Length;
                }
            }
            if (buf == null || buf.Length < 4 + 8 * DGlobals.vars.Count + slen * 2)
            {
                buf = new byte[4 + 8 * DGlobals.vars.Count + slen * 2];
            }
            Entry.ToBytes(DGlobals.vars.Count, buf, 0);
            int offset = 4;
            foreach (KeyValuePair<string, string> pair in DGlobals.vars)
            {
                string key = pair.Key;
                string value = pair.Value;
                offset = offset + DGlobals.KeyValueToBytes(key, value, buf, offset);                
            }
            return offset;
        }

        public static void FromBase64String(string s)
        {
            sbuf = s;
            byte[] buf = Convert.FromBase64String(s);
            FromBytes(buf, buf.Length, false);
        }

        public static void FromBytes(byte[] buf, int byteCount, bool replaceCache)
        {
            if (byteCount > 4)
            {
                int varsCount = Entry.BytesToInt(buf, 0);
                if (varsCount > 0)
                {
                    int offset = 4;
                    for (; ; )
                    {
                        string key = null;
                        string value = null;
                        offset += DGlobals.BytesToKeyValue(buf, offset, ref key, ref value);
                        DGlobals._add(key, value);
                        if (offset >= byteCount)
                        {
                            break;
                        }
                    }
                    if (replaceCache)
                    {
                        sbuf = Convert.ToBase64String(buf, 0, byteCount);
                    }
                    return;
                }
            }
            if (replaceCache)
            {
                sbuf = null;
            }            
        }

        public static void ResetCache()
        {
            if (DGlobals.vars.Count == 0)
            {
                sbuf = null;
            }
            else
            {
                byte[] buf = null;
                ToBytes(ref buf);
                sbuf = Convert.ToBase64String(buf);
            }            
        }

        public static void FromBase64StringInDebugMode(string s)
        {
            if (StaticGlobals.ExecutionMode != ExecutionMode.DEBUG)
            {
                throw new Exception("DGlobalsM.LoadBased64String can only be executed in DEBUG mode.");
            }

            byte[] buf = Convert.FromBase64String(s);
            string key = null;
            string value = null;
            DGlobals.BytesToKeyValue(buf, 0, ref key, ref value);           
            //DGlobals._add(key, value); //Don't want to call DGlobals._add(key, value), this will write to the console.
            DGlobals.vars[key] = value;
        }
    }
}