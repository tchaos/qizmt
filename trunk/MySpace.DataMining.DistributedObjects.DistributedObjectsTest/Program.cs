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
using System.Threading;
using System.Runtime.InteropServices;

using DistTools = MySpace.DataMining.DistributedObjects5.DistObjectBase;


namespace MySpace.DataMining.DistributedObjects5
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Press Enter for normal test, or:");
            Console.WriteLine("    'i' for interactive mode (press key at points)");
            Console.WriteLine("    'f' for full enumeration mode (slow)");
            Console.WriteLine("    'b' for buffer stress mode (allocates 0.5 GB memory)");
            Console.Write("> ");
            char kc = Console.ReadKey().KeyChar;
            bool interactive = ((byte)'i' == kc);
            if ('l' == kc)
                goto buffer_limits;
            Console.WriteLine();

            Tests5.RunTests(interactive, kc);

            Tests4.RunTests(interactive, kc);

            return;

            {
                System.Diagnostics.Debug.Assert(2 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(2), "2");
                System.Diagnostics.Debug.Assert(4 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(3), "3");
                System.Diagnostics.Debug.Assert(4 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(4), "4");
                System.Diagnostics.Debug.Assert(8 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(5), "5");
                System.Diagnostics.Debug.Assert(8 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(6), "6");
                System.Diagnostics.Debug.Assert(8 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(7), "7");
                System.Diagnostics.Debug.Assert(8 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(8), "8");
                System.Diagnostics.Debug.Assert(16 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(9), "9");
                System.Diagnostics.Debug.Assert(16 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(15), "15");
                System.Diagnostics.Debug.Assert(16 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(16), "16");
                System.Diagnostics.Debug.Assert(1024 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(1000), "1000");
                System.Diagnostics.Debug.Assert(1024 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(1024), "1024");
                System.Diagnostics.Debug.Assert(2048 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(1025), "1025");
                System.Diagnostics.Debug.Assert(2048 == MySpace.DataMining.DistributedObjects.Entry.Round2Power(2048), "2048");
            }


            {
                Console.WriteLine("Testing ArrayComboList with int key and 0-length value.");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("fooobj", 4);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");
                //acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs1.txt|slaveid=1");

                acl.Open();

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                int itercount = 200;
                int maxtimes = 5;
                for (int times = 0; times < maxtimes; times++)
                {
                    for (int i = 0; i < itercount; i++)
                    {
                        keybuf.Clear();
                        MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(i, keybuf);
                        acl.Add(keybuf, valuebuf, 0); // 0-length value.
                    }
                }

                acl.SortBlocks();

                int n = 0;
                ArrayComboListEnumerator[] enums = acl.GetEnumerators();
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        n++;
                    }
                }
                if (n != itercount * maxtimes)
                {
                    throw new Exception("Whoops");
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                acl.Close();

                Console.WriteLine("Done");
            }


            {
                Console.WriteLine("Testing ArrayComboList with int key and 0-length value in reducer.");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("fooobj", 4);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");
                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs1.txt|slaveid=1");

                acl.Open();

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                int itercount = 200;
                int maxtimes = 5;
                for (int times = 0; times < maxtimes; times++)
                {
                    for (int i = 0; i < itercount; i++)
                    {
                        keybuf.Clear();
                        MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(i, keybuf);
                        acl.Add(keybuf, valuebuf, 0); // 0-length value.
                    }
                }

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(@"
                       List<byte> lbuf = new List<byte>();

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            lbuf.Clear();
                            MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(values.Length, lbuf);
                            output.Add(key, ByteSlice.Create(lbuf));
                            if(" + maxtimes.ToString() + @" != values.Length)
                            {
                                throw new OverflowException();
                            }
                        }
", null);
                int n = 0;
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        n++;
                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        int ikey = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                        if (ikey < 0 || ikey > itercount)
                        {
                            throw new Exception("Bad key!");
                        }
                        valuebuf.Clear();
                        en.Current.CopyValueAppend(valuebuf);
                        int x = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf);
                        if (x != maxtimes)
                        {
                            throw new OverflowException();
                        }
                    }
                }
                if (n != itercount) // only itercount, not (itercount * maxtimes), because it was reduced from 5 to 1.
                {
                    throw new Exception("Whoops");
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                acl.Close();

                Console.WriteLine("Done");
            }


            {
                Console.WriteLine("Testing ArrayComboList with complex reducing and key/value manipulation!");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("aclxobj", 8);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");
                //acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs1.txt|slaveid=1");

                acl.Open();

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                keybuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(99, keybuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(0, keybuf); // Padding.
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(7828, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(345, valuebuf);
                acl.Add(keybuf, valuebuf, valuebuf.Count);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(334, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(235, valuebuf);
                acl.Add(keybuf, valuebuf, valuebuf.Count);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(55, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(34345, valuebuf);
                acl.Add(keybuf, valuebuf, valuebuf.Count);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(111, valuebuf); // !
                acl.Add(keybuf, valuebuf, valuebuf.Count);

                keybuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(35, keybuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(0, keybuf); // Padding.
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(78, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(33, valuebuf);
                acl.Add(keybuf, valuebuf, valuebuf.Count);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(11111, valuebuf); // !
                acl.Add(keybuf, valuebuf, valuebuf.Count);

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(@"
                        List<byte> keybuf = new List<byte>();
                        List<byte> valuebuf = new List<byte>();

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            ByteSlice lastsingle = ByteSlice.Create();
                            for(int i = 0; i != values.Length; i++)
                            {
                                if(values[i].Value.Length == 4)
                                {
                                    lastsingle = values[i].Value;
                                    break;
                                }
                            }

                            for(int i = 0; i != values.Length; i++)
                            {
                                if(values[i].Value.Length == 8)
                                {
                                    keybuf.Clear();
                                    for(int boat = 0; boat < 4; boat++)
                                    {
                                        keybuf.Add(key[boat]);
                                    }
                                    for(int boat = 0; boat < 4; boat++)
                                    {
                                        keybuf.Add(values[i].Value[boat]);
                                    }

                                    valuebuf.Clear();
                                    for(int boat = 4; boat < 8; boat++)
                                    {
                                        valuebuf.Add(values[i].Value[boat]);
                                    }
                                    for(int boat = 0; boat != lastsingle.Length; boat++)
                                    {
                                        valuebuf.Add(lastsingle[boat]);
                                    }
                                    output.Add(ByteSlice.Create(keybuf), ByteSlice.Create(valuebuf));
                                }
                            }
                        }
", null);
                int npass = 0;
                int nk99 = 0;
                int nk35 = 0;
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        int keyfirst = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                        int keysecond = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf, 4);
                        npass += keyfirst;
                        switch (keyfirst)
                        {
                            case 99:
                                {
                                    nk99 += keysecond;
                                    valuebuf.Clear();
                                    en.Current.CopyValueAppend(valuebuf);
                                    int vfirst = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf);
                                    int vsecond = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf, 4);
                                    if (111 != vsecond)
                                    {
                                        throw new Exception("vsecond");
                                    }
                                    switch (vfirst)
                                    {
                                        case 345:
                                            break;
                                        case 235:
                                            break;
                                        case 34345:
                                            break;
                                        default:
                                            throw new Exception("vfirst");
                                    }
                                }
                                break;
                            case 35:
                                {
                                    nk35 += keysecond;
                                    valuebuf.Clear();
                                    en.Current.CopyValueAppend(valuebuf);
                                    int vfirst = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf);
                                    int vsecond = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf, 4);
                                    if (11111 != vsecond)
                                    {
                                        throw new Exception("vsecond");
                                    }
                                    switch (vfirst)
                                    {
                                        case 33:
                                            break;
                                        default:
                                            throw new Exception("vfirst");
                                    }
                                }
                                break;
                            default:
                                throw new Exception("Bad.");
                        }
                    }
                }
                if (99 * 3 + 35 * 1 != npass
                    || 7828 + 334 + 55 != nk99
                    || 78 != nk35)
                {
                    throw new Exception("overboard");
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                acl.Close();

                Console.WriteLine("Done");
            }


            {
                Console.WriteLine("Testing ArrayComboList with complex reducing and key/value manipulation, take 2");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("aclxobj2", 8);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");
                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs1.txt|slaveid=1");

                acl.Open();

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                keybuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(0, keybuf); // Padding.
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(99, keybuf);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(7828, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(345, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(3434, valuebuf);
                acl.Add(keybuf, valuebuf, valuebuf.Count);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(334, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(235, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(2222, valuebuf);
                acl.Add(keybuf, valuebuf, valuebuf.Count);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(55, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(34345, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(342, valuebuf);
                acl.Add(keybuf, valuebuf, valuebuf.Count);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(111, valuebuf); // !
                acl.Add(keybuf, valuebuf, valuebuf.Count);

                keybuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(0, keybuf); // Padding.
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(35, keybuf);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(78, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(33, valuebuf);
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(23454, valuebuf);
                acl.Add(keybuf, valuebuf, valuebuf.Count);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(11111, valuebuf); // !
                acl.Add(keybuf, valuebuf, valuebuf.Count);

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(@"
                        List<byte> keybuf = new List<byte>();
                        List<byte> valuebuf = new List<byte>();

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            ByteSlice lastsingle = ByteSlice.Create();
                            for(int i = 0; i != values.Length; i++)
                            {
                                if(values[i].Value.Length == 4)
                                {
                                    lastsingle = values[i].Value;
                                    break;
                                }
                            }

                            for(int i = 0; i != values.Length; i++)
                            {
                                if(values[i].Value.Length == 12)
                                {
                                    keybuf.Clear();
                                    for(int boat = 0; boat < 4; boat++)
                                    {
                                        keybuf.Add(values[i].Value[boat]);
                                    }
                                    for(int boat = 4; boat < 8; boat++)
                                    {
                                        keybuf.Add(key[boat]);
                                    }

                                    valuebuf.Clear();
                                    for(int boat = 4; boat < 12; boat++)
                                    {
                                        valuebuf.Add(values[i].Value[boat]);
                                    }
                                    for(int boat = 0; boat != lastsingle.Length; boat++)
                                    {
                                        valuebuf.Add(lastsingle[boat]);
                                    }
                                    int x = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf, 0)
                                        + MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf, 4)
                                        + MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf, 8);
                                    valuebuf.Clear();
                                    MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(x, valuebuf);
                                    output.Add(ByteSlice.Create(keybuf), ByteSlice.Create(valuebuf));
                                }
                            }
                        }
", null);
                int npass = 0;
                int nk99 = 0;
                int nk35 = 0;
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        int keyfirst = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                        int keysecond = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf, 4);
                        npass += keysecond;
                        switch (keysecond)
                        {
                            case 99:
                                {
                                    nk99 += keyfirst;
                                    valuebuf.Clear();
                                    en.Current.CopyValueAppend(valuebuf);
                                    int v = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf);
                                    switch (v)
                                    {
                                        case 345 + 3434 + 111:
                                            break;
                                        case 235 + 2222 + 111:
                                            break;
                                        case 34345 + 342 + 111:
                                            break;
                                        default:
                                            throw new Exception("v");
                                    }
                                }
                                break;
                            case 35:
                                {
                                    nk35 += keyfirst;
                                    valuebuf.Clear();
                                    en.Current.CopyValueAppend(valuebuf);
                                    int v = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf);
                                    switch (v)
                                    {
                                        case 33 + 23454 + 11111:
                                            break;
                                        default:
                                            throw new Exception("v");
                                    }
                                }
                                break;
                            default:
                                throw new Exception("Bad.");
                        }
                    }
                }
                if (99 * 3 + 35 * 1 != npass
                    || 7828 + 334 + 55 != nk99
                    || 78 != nk35)
                {
                    throw new Exception("overboard");
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                acl.Close();

                Console.WriteLine("Done");
            }


            if ('b' == kc)
            {
                Console.WriteLine("Testing ArrayComboList reducing 0.5 GB at once, testing internal socket buffers");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("bigbuf", 4);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");
                //acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs1.txt|slaveid=1");

                acl.Open();

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                keybuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(35, keybuf);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(78, valuebuf);
                acl.Add(keybuf, valuebuf, valuebuf.Count);

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(@"
                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            output.Add(key, ByteSlice.Create(new byte[536870912])); // 0.5 GB
                        }
", null);
                int nvalues = 0;
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        nvalues++;
                        if (en.Current.GetValueLength() != 536870912)
                        {
                            throw new Exception("Bad value length");
                        }
                    }
                }
                if (1 != nvalues)
                {
                    throw new Exception("number of values not 1");
                }

                acl.Close();

                Console.WriteLine("Done");
            }


            if ('f' == kc) // ...
            {
                Console.WriteLine("Testing ArrayComboList reducing many, many values for one key");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("sdfgsdfg", 4);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");
                //acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs1.txt|slaveid=1");

                acl.Open();

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                keybuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(35, keybuf);
                valuebuf.Clear();
                MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(78, valuebuf);
                acl.Add(keybuf, valuebuf, valuebuf.Count);

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(@"
                        List<byte> keybuf = new List<byte>();
                        List<byte> valuebuf = new List<byte>();

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            for(int i = 0; i != 8388608; i++) // Ensure filling default batch buffer (8MB) at least 2 whole times.
                            {
                                if(1 != values.Length)
                                {
                                    throw new Exception('k'.ToString());
                                }
                                keybuf.Clear();
                                key.AppendTo(keybuf);
                                int ikey = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                                valuebuf.Clear();
                                values[0].Value.AppendTo(valuebuf);
                                int ivalue = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf);
                                if(ikey != 35)
                                {
                                    throw new Exception(ikey.ToString());
                                }
                                if(ivalue != 78)
                                {
                                    throw new Exception(ivalue.ToString());
                                }
                                output.Add(key, values[0].Value);
                            }
                        }
", null);
                int nvalues = 0;
                bool stop = false;
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        int ikey = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                        valuebuf.Clear();
                        en.Current.CopyValueAppend(valuebuf);
                        int ivalue = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf);
                        if (ikey != 35)
                        {
                            throw new Exception(ikey.ToString());
                        }
                        if (ivalue != 78)
                        {
                            throw new Exception(ivalue.ToString());
                        }

                        if ('f' != kc)
                        {
                            stop = true;
                            break;
                        }
                        nvalues++;
                        if (0 == (nvalues % (8 * 1024)))
                        {
                            Console.Write('.');
                        }
                    }
                    if (stop)
                    {
                        break;
                    }
                }
                if ('f' == kc)
                {
                    Console.WriteLine("Reduce output: {0} values", nvalues);
                    if (nvalues != 8388608)
                    {
                        throw new Exception("nvalues mismatch: " + nvalues.ToString());
                    }
                }

                acl.Close();

                Console.WriteLine("Done");
            }


            {
                Console.WriteLine("ArrayComboList test with dymamically created inline C# reducer");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("aclxobj2", 4);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                Random rnd = new Random();
                Dictionary<int, int> kv = new Dictionary<int, int>();
                int numkeys = 50;
                for (int i = 0; i != numkeys; i++)
                {
                    int x;
                    do
                    {
                        x = rnd.Next();
                    }
                    while (kv.ContainsKey(x));
                    kv[x] = rnd.Next();
                }
                if (kv.Count != numkeys)
                {
                    throw new Exception("kv.Count != numkeys");
                }

                foreach (KeyValuePair<int, int> pair in kv)
                {
                    keybuf.Clear();
                    MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(pair.Key, keybuf);
                    valuebuf.Clear();
                    MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(pair.Value, valuebuf);
                    acl.Add(keybuf, valuebuf);
                }

                acl.SortBlocks();

                StringBuilder dictcode = new StringBuilder(128);
                dictcode.Append("kv = new Dictionary<int, int>();");
                foreach (KeyValuePair<int, int> pair in kv)
                {
                    dictcode.Append("kv[" + pair.Key.ToString() + "] = " + pair.Value.ToString() + ";");
                }

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(@"
                        List<byte> keybuf = new List<byte>();
                        List<byte> valuebuf = new List<byte>();
                        Dictionary<int, int> kv;
                        bool finalized = false;
                        
                        public void ReduceInitialize()
                        {
                            " + dictcode.ToString() + @"
                        }
                        
                        public void ReduceFinalize()
                        {
                            if(finalized)
                            {
                                throw new Exception(" + "\"Double finalized!\"" + @");
                            }
                            finalized = true;
                            if(0 != kv.Count)
                            {
                                throw new Exception(" + "\"ReduceFinalize: \" + " + @"kv.Count.ToString());
                            }
                        }

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            if(finalized)
                            {
                                throw new Exception(" + "\"Reduce after finalized!\"" + @");
                            }
                            for(int i = 0; i != values.Length; i++)
                            {
                                output.Add(key, values[i].Value); // ...
                                keybuf.Clear();
                                key.AppendTo(keybuf);
                                int ikey = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                                valuebuf.Clear();
                                values[i].Value.AppendTo(valuebuf);
                                int ivalue = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf);
                                if(!kv.ContainsKey(ikey))
                                {
                                    throw new Exception(" + "\"Reduce doesn't contain key: \" + " + @"ikey.ToString());
                                }
                                if(kv[ikey] != ivalue)
                                {
                                    throw new Exception(" + "\"Reduce value mismatch for key: \" + " + @"ikey.ToString());
                                }
                                kv.Remove(ikey);
                            }
                        }
", null);
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        int ikey = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                        valuebuf.Clear();
                        en.Current.CopyValueAppend(valuebuf);
                        int ivalue = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf);
                        if (!kv.ContainsKey(ikey))
                        {
                            throw new Exception("Enumeration doesn't contain key: " + ikey.ToString());
                        }
                        if (kv[ikey] != ivalue)
                        {
                            throw new Exception("Enumeration value mismatch for key: " + ikey.ToString());
                        }
                        kv.Remove(ikey);
                    }
                }
                if (0 != kv.Count)
                {
                    throw new Exception("Enumeration failure: " + kv.Count.ToString());
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                acl.Close();

                Console.WriteLine("Done");
            }


            // This simple test should go first.
            {
                Console.WriteLine("Simple Distributed ArrayComboList");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("fooobj", 8);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 });

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                acl.Close();

                Console.WriteLine("Done");
            }

            // This simple test should go first too.
            {
                Console.WriteLine("Simple Distributed Hashtable:");

                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                Console.WriteLine("  AddBlock...");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");
                Console.WriteLine("  Done");
                //dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs1.txt|slaveid=1");
                //dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs2.txt|slaveid=2");
                //dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs3.txt|slaveid=3");
                //dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs4.txt|slaveid=4");

                Console.WriteLine("  Open...");
                dht.Open();
                Console.WriteLine("  Done");

                Console.WriteLine("  Setting values...");
                dht[DistTools.ToBytes("foo")] = DistTools.ToBytes("bar");
                dht[DistTools.ToBytes("all")] = DistTools.ToBytes("your");
                dht[DistTools.ToBytes(42)] = DistTools.ToBytes("goat");
                Console.WriteLine("  Done");

                byte[] bb;
                //string s;

                Console.WriteLine("  Getting and verifying values..");

                bb = dht[DistTools.ToBytes("foo")];
                if (null == bb)
                    throw new Exception("'foo' failure");
                Console.WriteLine("'foo' value is '{0}'", DistTools.BytesToString(bb));

                bb = dht[DistTools.ToBytes("all")];
                if (null == bb)
                    throw new Exception("'all' failure");
                Console.WriteLine("'all' value is '{0}'", DistTools.BytesToString(bb));

                bb = dht[DistTools.ToBytes(42)];
                if (null == bb)
                    throw new Exception("'42' failure");
                Console.WriteLine("'42' value is '{0}'", DistTools.BytesToString(bb));

                Console.WriteLine("  Done");

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                dht.Close();

                if (interactive)
                {
                    Console.WriteLine("Done (press a key)");
                    Console.ReadKey();
                }
            }

            // This other simple test should go (near) first.
            {
                Console.WriteLine("Simple IntComboList:");

                MySpace.DataMining.DistributedObjects5.IntComboList icl = new MySpace.DataMining.DistributedObjects5.IntComboList("myints");

                Console.WriteLine("  AddBlock...");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs0.txt|slaveid=0");
                Console.WriteLine("  Done");

                Console.WriteLine("  Open...");
                icl.Open();
                Console.WriteLine("  Done");

                Console.WriteLine("  Setting values...");
                long tot = 0;
                for (int i = 0; i != 200; i++)
                {
                    tot += i;
                    icl.Add(i, i + 1);
                }
                Console.WriteLine("  Done");

                Console.WriteLine("  Getting values...");
                MySpace.DataMining.DistributedObjects5.IntComboListEnumerator[] enums = icl.GetEnumerators();
                foreach (MySpace.DataMining.DistributedObjects5.IntComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        tot -= en.Current.A;
                    }
                }
                if (0 != tot)
                {
                    throw new Exception("numbers don't add up!");
                }
                Console.WriteLine("  Done");

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                icl.Close();

                if (interactive)
                {
                    Console.WriteLine("Done (press a key)");
                    Console.ReadKey();
                }
            }

            // This other simple test should go (near) first.
            {
                Console.WriteLine("Simple LongIntComboList:");

                MySpace.DataMining.DistributedObjects5.LongIntComboList icl = new MySpace.DataMining.DistributedObjects5.LongIntComboList("mylints");

                Console.WriteLine("  AddBlock...");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs0.txt|slaveid=0");
                Console.WriteLine("  Done");

                Console.WriteLine("  Open...");
                icl.Open();
                Console.WriteLine("  Done");

                Console.WriteLine("  Setting values...");
                long tot = 0;
                int numadded = 200;
                for (int i = 0; i != numadded; i++)
                {
                    tot += i;
                    icl.Add(i, i + 1);
                }
                Console.WriteLine("  Done");

                Console.WriteLine("  Getting values...");
                MySpace.DataMining.DistributedObjects5.LongIntComboListEnumerator[] enums = icl.GetEnumerators();
                int n = 0;
                foreach (MySpace.DataMining.DistributedObjects5.LongIntComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        n += en.Current.Count;
                        tot -= en.Current.Count * en.Current.A;
                    }
                }
                if (n != numadded)
                {
                    throw new Exception("numbers (Count) don't add up!");
                }
                if (0 != tot)
                {
                    throw new Exception("numbers don't add up!");
                }
                Console.WriteLine("  Done");

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                icl.Close();

                if (interactive)
                {
                    Console.WriteLine("Done (press a key)");
                    Console.ReadKey();
                }
            }

            // This other simple test should go (near) first.
            {
                Console.WriteLine("Simple IntComboListLocal (local):");

                MySpace.DataMining.DistributedObjects5.IntComboList icl = new MySpace.DataMining.DistributedObjects5.IntComboListLocal("myints");

                Console.WriteLine("  AddBlock...");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs0.txt|slaveid=0");
                Console.WriteLine("  Done");

                Console.WriteLine("  Open...");
                icl.Open();
                Console.WriteLine("  Done");

                Console.WriteLine("  Setting values...");
                for (int i = 0; i != 200; i++)
                {
                    icl.Add(i, i + 1);
                }
                Console.WriteLine("  Done");

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                icl.Close();

                if (interactive)
                {
                    Console.WriteLine("Done (press a key)");
                    Console.ReadKey();
                }
            }

            {
                Console.WriteLine("Simple dist ArrayComboList:");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("fooobj", 8);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 });

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumerators(); // Default.
                List<byte> keybuf = new List<byte>(acl.GetKeyBufferLength());
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        Console.WriteLine("Key: ");
                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        for (int i = 0; i != keybuf.Count; i++)
                        {
                            if (0 != i)
                            {
                                Console.Write(',');
                            }
                            Console.Write(keybuf[i]);
                        }
                        Console.WriteLine();
                    }
                }

                acl.Close();

                Console.WriteLine("Done");
            }

            {
                Console.WriteLine("Confirming correct zblock distribution with IntComboList:");

                if (interactive)
                {
                    Console.WriteLine("Press a key...");
                    Console.ReadKey();
                }

                MySpace.DataMining.DistributedObjects5.IntComboList icl = new MySpace.DataMining.DistributedObjects5.IntComboList("zfoo");

                icl.AddBlock("1024", @"127.0.0.1|intcombo_logs0.txt|slaveid=0");
                //acl.AddBlock("1024", @"127.0.0.1|intcombo_logs1.txt|slaveid=1");

                icl.Open();

                System.Collections.Generic.Queue<int> wheel = new Queue<int>();
                System.Collections.Generic.List<int> al = new List<int>();
                Random rnd = new Random(System.DateTime.Now.Millisecond);
                System.Collections.Hashtable ht = new System.Collections.Hashtable();

                //fill wheel
                for (int i = 1; i != 200; i++)
                {
                    int x = rnd.Next(5656, 76688989);
                    object o = ht[x];
                    int y = i;
                    if (null != o)
                    {
                        y += (int)o;
                    }
                    ht[x] = y;
                    for (int j = 0; j != i; j++)
                    {
                        al.Add(x);
                    }
                }
                foreach (int o in al)
                {
                    wheel.Enqueue(o);
                }

                int added = 0;
                while (0 != wheel.Count)
                {
                    int x = rnd.Next(0, wheel.Count - 1);
                    for (int foo = 0; foo != x; foo++)
                    {
                        wheel.Enqueue(wheel.Dequeue());
                    }
                    int key = (int)wheel.Dequeue();
                    icl.Add(key, key);
                    added++;
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to sort...");
                    Console.ReadKey();
                }

                icl.SortBlocks();

                IntComboListEnumerator[] enums = icl.GetEnumerators();
                System.Collections.Generic.List<int> outlist = new List<int>();
                foreach (IntComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        int key = en.Current.A;
                        int value = en.Current.B;
                        for (int ik = 0; ik != en.Current.Count; ik++) // Roll it back out for test!
                        {
                            outlist.Add(key);
                        }
                    }
                    Console.WriteLine();
                }

                int lastkey = 0;
                foreach (int key in outlist)
                {
                    if (key != lastkey)
                    {
                        if (0 != lastkey)
                        {
                            if (0 != (int)ht[lastkey])
                            {
                                throw new Exception("Order error");
                            }
                        }
                    }
                    lastkey = key;
                    object blah = ht[key];
                    ht[key] = (int)blah - 1;
                }

                al.Sort();
                outlist.Sort();
                if (al.Count != outlist.Count)
                {
                    throw new Exception("foo");
                }
                for (int i = 0; i != al.Count; i++)
                {
                    if ((int)al[i] != (int)outlist[i])
                    {
                        throw new Exception("bar");
                    }
                }

                Console.WriteLine("Done");

                if (interactive)
                {
                    Console.WriteLine("Press a key to close...");
                    Console.ReadKey();
                }

                icl.Close();
            }

            {
                Console.WriteLine("Confirming correct zblock distribution with LongIntComboList:");

                if (interactive)
                {
                    Console.WriteLine("Press a key...");
                    Console.ReadKey();
                }

                MySpace.DataMining.DistributedObjects5.LongIntComboList icl = new MySpace.DataMining.DistributedObjects5.LongIntComboList("zlfoo");

                icl.AddBlock("1024", @"127.0.0.1|intcombo_logs0.txt|slaveid=0");
                //acl.AddBlock("1024", @"127.0.0.1|intcombo_logs1.txt|slaveid=1");

                icl.Open();

                System.Collections.Generic.Queue<int> wheel = new Queue<int>();
                System.Collections.Generic.List<int> al = new List<int>();
                Random rnd = new Random(System.DateTime.Now.Millisecond);
                System.Collections.Hashtable ht = new System.Collections.Hashtable();

                //fill wheel
                for (int i = 1; i != 200; i++)
                {
                    //int x = rnd.Next(5656, 76688989);
                    int x = rnd.Next(4000, 5000);
                    object o = ht[x];
                    int y = i;
                    if (null != o)
                    {
                        y += (int)o;
                    }
                    ht[x] = y;
                    for (int j = 0; j != i; j++)
                    {
                        al.Add(x);
                    }
                }
                foreach (int o in al)
                {
                    wheel.Enqueue(o);
                }

                int added = 0;
                while (0 != wheel.Count)
                {
                    int x = rnd.Next(0, wheel.Count - 1);
                    for (int foo = 0; foo != x; foo++)
                    {
                        wheel.Enqueue(wheel.Dequeue());
                    }
                    int key = (int)wheel.Dequeue();
                    icl.Add(key, key);
                    added++;
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to sort...");
                    Console.ReadKey();
                }

                icl.SortBlocks();

                LongIntComboListEnumerator[] enums = icl.GetEnumerators();
                System.Collections.Generic.List<long> outlist = new List<long>();
                foreach (LongIntComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        long key = en.Current.A;
                        int value = en.Current.B;
                        for (int ik = 0; ik != en.Current.Count; ik++) // Roll it back out for test!
                        {
                            outlist.Add(key);
                        }
                    }
                    Console.WriteLine();
                }

                int lastkey = 0;
                foreach (int key in outlist)
                {
                    if (key != lastkey)
                    {
                        if (0 != lastkey)
                        {
                            if (0 != (int)ht[lastkey])
                            {
                                throw new Exception("Order error");
                            }
                        }
                    }
                    lastkey = key;
                    object blah = ht[key];
                    ht[key] = (int)blah - 1;
                }

                al.Sort();
                outlist.Sort();
                if (al.Count != outlist.Count)
                {
                    throw new Exception("foo");
                }
                for (int i = 0; i != al.Count; i++)
                {
                    if ((int)al[i] != (int)outlist[i])
                    {
                        throw new Exception("bar");
                    }
                }

                Console.WriteLine("Done");

                if (interactive)
                {
                    Console.WriteLine("Press a key to close...");
                    Console.ReadKey();
                }

                icl.Close();
            }

            {
                Console.WriteLine("Simple dist ArrayComboList with custom IBeforeReduce plugin:");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("fooobj", 8);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 });

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumerators(@"UserEnumerator3.dll", "UserEnumeratorPlugin");
                List<byte> keybuf = new List<byte>(acl.GetKeyBufferLength());
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        Console.WriteLine("Key: ");
                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        for (int i = 0; i != keybuf.Count; i++)
                        {
                            if (0 != i)
                            {
                                Console.Write(',');
                            }
                            Console.Write(keybuf[i]);
                        }
                        Console.WriteLine();
                    }
                }

                acl.Close();

                Console.WriteLine("Done");
            }

            {
                Console.WriteLine("ArrayComboList with EasyReducer custom Random-Access Reducer:");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("fooobj", 8);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                byte[] valuebuf = new byte[32];

                const string val1 = "test value";
                MySpace.DataMining.DistributedObjects.Entry.AsciiToBytes(val1, valuebuf, 0);
                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, valuebuf, val1.Length);
                const string val2 = "another value!";
                MySpace.DataMining.DistributedObjects.Entry.AsciiToBytes(val2, valuebuf, 0);
                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, valuebuf, val2.Length);
                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumerators("EasyReducer.dll", "MyReducer");
                List<byte> keybuf = new List<byte>(acl.GetKeyBufferLength());
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        {
                            Console.WriteLine("Key: ");
                            keybuf.Clear();
                            en.Current.CopyKeyAppend(keybuf);
                            for (int i = 0; i != keybuf.Count; i++)
                            {
                                if (0 != i)
                                {
                                    Console.Write(',');
                                }
                                Console.Write(keybuf[i]);
                            }
                            Console.WriteLine();
                        }

                        {
                            Console.WriteLine("Value: ");
                            keybuf.Clear();
                            en.Current.CopyValueAppend(keybuf);
                            for (int i = 0; i != keybuf.Count; i++)
                            {
                                if (0 != i)
                                {
                                    Console.Write(',');
                                }
                                Console.Write(keybuf[i]);
                            }
                            Console.WriteLine();
                        }
                    }
                }

                acl.Close();

                Console.WriteLine("Done");
            }

            {
                Console.WriteLine("ArrayComboList with simplified custom Random-Access Reducer code generator:");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("fooobj", 8);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                byte[] valuebuf = new byte[32];

                const string val1 = "test value";
                MySpace.DataMining.DistributedObjects.Entry.AsciiToBytes(val1, valuebuf, 0);
                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, valuebuf, val1.Length);
                const string val2 = "another value!";
                MySpace.DataMining.DistributedObjects.Entry.AsciiToBytes(val2, valuebuf, 0);
                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, valuebuf, val2.Length);
                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(
                @"
                        List<byte> lbuf = new List<byte>();

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            for (int i = 0; i < values.Length; i++)
                            {
                                ByteSlice value = values[i].Value;
                                output.Add(key, value);
                            }
                        }
                ", null);
                List<byte> keybuf = new List<byte>(acl.GetKeyBufferLength());
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        {
                            Console.WriteLine("Key: ");
                            keybuf.Clear();
                            en.Current.CopyKeyAppend(keybuf);
                            for (int i = 0; i != keybuf.Count; i++)
                            {
                                if (0 != i)
                                {
                                    Console.Write(',');
                                }
                                Console.Write(keybuf[i]);
                            }
                            Console.WriteLine();
                        }

                        {
                            Console.WriteLine("Value: ");
                            keybuf.Clear();
                            en.Current.CopyValueAppend(keybuf);
                            for (int i = 0; i != keybuf.Count; i++)
                            {
                                if (0 != i)
                                {
                                    Console.Write(',');
                                }
                                Console.Write(keybuf[i]);
                            }
                            Console.WriteLine();
                        }
                    }
                }

                acl.Close();

                Console.WriteLine("Done");
            }

            {
                Console.WriteLine("ArrayComboList with X custom Random-Access Reducer code generator:");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("fooobj", 8);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                byte[] valuebuf = new byte[32];

                const string val1 = "test value";
                MySpace.DataMining.DistributedObjects.Entry.AsciiToBytes(val1, valuebuf, 0);
                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, valuebuf, val1.Length);
                const string val2 = "another value!";
                MySpace.DataMining.DistributedObjects.Entry.AsciiToBytes(val2, valuebuf, 0);
                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, valuebuf, val2.Length);
                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(
                @"
                        List<byte> lbuf = new List<byte>();

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            // Value to string and trim spaces.
                            for (int i = 0; i != values.Length; i++)
                            {
                                lbuf.Clear();
                                values[i].Value.AppendTo(lbuf);

                                string str = Entry.BytesToAscii(lbuf);
                                str = str.Trim();

                                lbuf.Clear();
                                Entry.AsciiToBytesAppend(str, lbuf);

                                output.Add(key, ByteSlice.Create(lbuf));
                            }
                        }
                ", null);
                List<byte> keybuf = new List<byte>(acl.GetKeyBufferLength());
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        {
                            Console.WriteLine("Key: ");
                            keybuf.Clear();
                            en.Current.CopyKeyAppend(keybuf);
                            for (int i = 0; i != keybuf.Count; i++)
                            {
                                if (0 != i)
                                {
                                    Console.Write(',');
                                }
                                Console.Write(keybuf[i]);
                            }
                            Console.WriteLine();
                        }

                        {
                            Console.WriteLine("Value: ");
                            keybuf.Clear();
                            en.Current.CopyValueAppend(keybuf);
                            for (int i = 0; i != keybuf.Count; i++)
                            {
                                if (0 != i)
                                {
                                    Console.Write(',');
                                }
                                Console.Write(keybuf[i]);
                            }
                            Console.WriteLine();
                        }
                    }
                }

                acl.Close();

                Console.WriteLine("Done");
            }

            {
                Console.WriteLine("ArrayComboList with advanced custom Random-Access Reducer code generator:");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("fooobj", 8);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                byte[] valuebuf = new byte[32];

                const string val1 = "test value  ";
                MySpace.DataMining.DistributedObjects.Entry.AsciiToBytes(val1, valuebuf, 0);
                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, valuebuf, val1.Length);
                const string val2 = "another value!  ";
                MySpace.DataMining.DistributedObjects.Entry.AsciiToBytes(val2, valuebuf, 0);
                acl.Add(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }, valuebuf, val2.Length);
                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(
                @"
                        List<byte> lbuf = new List<byte>();

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            // Value to string and trim spaces.
                            for (int i = 0; i != values.Length; i++)
                            {
                                lbuf.Clear();
                                values[i].Value.AppendTo(lbuf);

                                string str = Entry.BytesToAscii(lbuf);
                                str = str.Trim();

                                lbuf.Clear();
                                Entry.AsciiToBytesAppend(str, lbuf);

                                output.Add(key, ByteSlice.Create(lbuf));
                            }

                            // First 4 bytes of value to integer and increment.
                            for (int i = 0; i != values.Length; i++)
                            {
                                lbuf.Clear();
                                for (int bi = 0; bi != 4; bi++)
                                {
                                    lbuf.Add(values[i].Value[bi]);
                                }

                                int x = Entry.BytesToInt(lbuf);
                                x++;

                                lbuf.Clear();
                                Entry.ToBytesAppend(x, lbuf);

                                output.Add(key, ByteSlice.Create(lbuf));
                            }
                        }
                ", null);
                List<byte> keybuf = new List<byte>(acl.GetKeyBufferLength());
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        {
                            Console.WriteLine("Key: ");
                            keybuf.Clear();
                            en.Current.CopyKeyAppend(keybuf);
                            for (int i = 0; i != keybuf.Count; i++)
                            {
                                if (0 != i)
                                {
                                    Console.Write(',');
                                }
                                Console.Write(keybuf[i]);
                            }
                            Console.WriteLine();
                        }

                        {
                            Console.WriteLine("Value: ");
                            keybuf.Clear();
                            en.Current.CopyValueAppend(keybuf);
                            for (int i = 0; i != keybuf.Count; i++)
                            {
                                if (0 != i)
                                {
                                    Console.Write(',');
                                }
                                Console.Write(keybuf[i]);
                            }
                            Console.WriteLine();
                        }
                    }
                }

                acl.Close();

                Console.WriteLine("Done");
            }

            {
                Console.WriteLine("ArrayComboList: Testing adding many values...");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("fooobj", 4);
                acl.atype = true;

                acl.AddBlock("1024", "4", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");
                acl.AddBlock("1024", "4", @"127.0.0.1|arraycombo_logs0.txt|slaveid=1");
                acl.AddBlock("1024", "4", @"127.0.0.1|arraycombo_logs0.txt|slaveid=2");

                acl.Open();

                byte[] intbuf2 = new byte[8];

                int adds = 400;
                for (int i = (adds / 2) - 1; ; i--)
                {
                    MySpace.DataMining.DistributedObjects.Entry.ToBytes(i, intbuf2, 0);
                    MySpace.DataMining.DistributedObjects.Entry.ToBytes(i / 2, intbuf2, 4);
                    acl.Add(intbuf2, 0, intbuf2, 4, 4);

                    if (0 == i)
                    {
                        break;
                    }
                }
                for (int i = adds / 2; i != adds; i++)
                {
                    MySpace.DataMining.DistributedObjects.Entry.ToBytes(i, intbuf2, 0);
                    MySpace.DataMining.DistributedObjects.Entry.ToBytes(i / 2, intbuf2, 4);
                    acl.Add(intbuf2, 0, intbuf2, 4, 4);
                }

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumerators();
                List<byte> keybuf = new List<byte>(acl.GetKeyBufferLength());
                List<byte> valuebuf = new List<byte>(acl.GetValueBufferLength());
                int nkeys = 0;
                foreach (ArrayComboListEnumerator en in enums)
                {
                    int prev = int.MinValue;
                    while (en.MoveNext())
                    {
                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        valuebuf.Clear();
                        en.Current.CopyValueAppend(valuebuf);
                        int key = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                        int value = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf);
                        //Console.Write("{0}={1} ", key, value);
                        Console.Write("{0} ", key);
                        nkeys++;
                        if (key / 2 != value)
                        {
                            throw new Exception("Key/value error");
                        }
                        prev = key;
                    }
                    Console.WriteLine();
                }
                if (nkeys != adds)
                {
                    throw new Exception("Did not enumerate all keys that were added");
                }

                acl.Close();
                Console.WriteLine("Done");
            }

            {
                Console.WriteLine(">----------------");

                MySpace.DataMining.DistributedObjects5.IntComboList icl = new MySpace.DataMining.DistributedObjects5.IntComboList("myints");

                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs0.txt|slaveid=0");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs1.txt|slaveid=1");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs2.txt|slaveid=2");

                //icl.EnableAddBuffer();

                icl.Open();

                int adds = 200;
                for (int i = adds / 2; ; i--)
                {
                    icl.Add(i, i / 2);
                    if (0 == i)
                    {
                        break;
                    }
                }
                for (int i = adds - 1; i > adds / 2; i--)
                {
                    icl.Add(i, i / 2);
                }

                //icl.Flush();

                Console.WriteLine("Sorting...");
                icl.SortBlocks();

                //icl.Flush();

                MySpace.DataMining.DistributedObjects5.IntComboListEnumerator[] enums = icl.GetEnumerators();
                bool[] tens = new bool[adds / 10];
                for (int i = 0; i != tens.Length; i++)
                {
                    tens[i] = false;
                }
                int nenum = 0;
                int nkeys = 0;
                foreach (MySpace.DataMining.DistributedObjects5.IntComboListEnumerator en in enums)
                {
                    Console.WriteLine(" In enumerator index " + nenum.ToString());
                    nenum++;
                    while (en.MoveNext())
                    {
                        nkeys++;
                        int a, b;
                        a = en.Current.A;
                        b = en.Current.B;
                        if (a / 2 != b)
                        {
                            throw new Exception("Bad values");
                        }
                        Console.Write(" {0} ", a);
                        if ((a % 10) == 0)
                        {
                            tens[a / 10] = true;
                        }
                    }
                    Console.WriteLine();
                }
                for (int i = 0; i != tens.Length; i++)
                {
                    if (!tens[i])
                    {
                        throw new Exception("Bad values (did not find all that were set)");
                    }
                }

                icl.Close();

                Console.WriteLine("Enumerated {0} keys", nkeys);

                Console.WriteLine("----------------<");
            }

            {
                Console.WriteLine(">----------------");

                MySpace.DataMining.DistributedObjects5.LongIntComboList icl = new MySpace.DataMining.DistributedObjects5.LongIntComboList("mylongints");

                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs0.txt|slaveid=0");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs1.txt|slaveid=1");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs2.txt|slaveid=2");

                //icl.EnableAddBuffer();

                icl.Open();

                int adds = 200;
                for (int i = adds / 2; ; i--)
                {
                    icl.Add(i, i / 2);
                    if (0 == i)
                    {
                        break;
                    }
                }
                for (int i = adds - 1; i > adds / 2; i--)
                {
                    icl.Add(i, i / 2);
                }

                //icl.Flush();

                Console.WriteLine("Sorting...");
                icl.SortBlocks();

                //icl.Flush();

                MySpace.DataMining.DistributedObjects5.LongIntComboListEnumerator[] enums = icl.GetEnumerators();
                bool[] tens = new bool[adds / 10];
                for (int i = 0; i != tens.Length; i++)
                {
                    tens[i] = false;
                }
                int nenum = 0;
                int nkeys = 0;
                foreach (MySpace.DataMining.DistributedObjects5.LongIntComboListEnumerator en in enums)
                {
                    Console.WriteLine(" In enumerator index " + nenum.ToString());
                    nenum++;
                    while (en.MoveNext())
                    {
                        nkeys++;
                        long a, b;
                        a = en.Current.A;
                        b = en.Current.B;
                        if (a / 2 != b)
                        {
                            throw new Exception("Bad values");
                        }
                        Console.Write(" {0} ", a);
                        if ((a % 10) == 0)
                        {
                            tens[a / 10] = true;
                        }
                    }
                    Console.WriteLine();
                }
                for (int i = 0; i != tens.Length; i++)
                {
                    if (!tens[i])
                    {
                        throw new Exception("Bad values (did not find all that were set)");
                    }
                }

                icl.Close();

                Console.WriteLine("Enumerated {0} keys", nkeys);

                Console.WriteLine("----------------<");
            }

            {

                MySpace.DataMining.DistributedObjects5.IntComboList icl = new MySpace.DataMining.DistributedObjects5.IntComboListLocal("myints");

                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs0.txt|slaveid=0");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs1.txt|slaveid=1");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs2.txt|slaveid=2");

                //icl.EnableAddBuffer();

                icl.Open();

                int adds = 200;
                for (int i = 0; i != adds; i++)
                {
                    icl.Add(i, i + 1);
                }

                //icl.Flush();

                MySpace.DataMining.DistributedObjects5.IntComboListEnumerator[] enums = icl.GetEnumerators();
                bool[] tens = new bool[adds / 10];
                for (int i = 0; i != tens.Length; i++)
                {
                    tens[i] = false;
                }
                int nenum = 0;
                int nkeys = 0;
                foreach (MySpace.DataMining.DistributedObjects5.IntComboListEnumerator en in enums)
                {
                    Console.WriteLine(" In enumerator index " + nenum.ToString());
                    nenum++;
                    while (en.MoveNext())
                    {
                        nkeys++;
                        int a, b;
                        a = en.Current.A;
                        b = en.Current.B;
                        if (a + 1 != b)
                        {
                            throw new Exception("Bad values");
                        }
                        Console.Write(" {0} ", a);
                        if ((a % 10) == 0)
                        {
                            tens[a / 10] = true;
                        }
                    }
                    Console.WriteLine();
                }
                Console.WriteLine("Enumerated {0} values", nkeys);
                for (int i = 0; i != tens.Length; i++)
                {
                    if (!tens[i])
                    {
                        throw new Exception("Bad values (did not find all that were set)");
                    }
                }

                icl.Close();
            }

            /*
            {
                Console.WriteLine("100 MB memory test...");

                MySpace.DataMining.DistributedObjects5.IntComboList icl = new MySpace.DataMining.DistributedObjects5.IntComboList("myints");

                //icl.EnableAddBuffer();

                // 100 MB == 104857600
                // div by 8 for capacity since each element is 8 bytes, total to 100 MB.
                icl.AddBlock((104857600 / 8).ToString(), @"127.0.0.1|intcombo100MB_logs0.txt|slaveid=0");

                icl.Open();

                for (int i = 0; i != 104857600 / 8; i++)
                {
                    icl.Add(i, i + 1);

                    if (i == 104857600 / 8 / 2)
                    {
                        Console.WriteLine("  Read in 50 MB; press a key to continue to 100 MB...");
                    }
                }

                //icl.Flush();
                
                if (interactive)
                {
                    Console.WriteLine("Read in 100 MB; press a key to release");
                    Console.ReadKey();
                }

                icl.Close();
            }
             * */

            {
                Console.WriteLine("Confirming 'Minimum Duplicate Count'");

                MySpace.DataMining.DistributedObjects5.IntComboList icl = new MySpace.DataMining.DistributedObjects5.IntComboList("myints");

                icl.AddBlock("4334", @"127.0.0.1|intcombo100MB_logs0.txt|slaveid=0");
                icl.AddBlock("4", @"127.0.0.1|intcombo100MB_logs1.txt|slaveid=1");

                icl.SetMinimumDuplicateCount(2);

                icl.Open();

                icl.Add(1, 0); // 0

                icl.Add(2, 0); // 1
                icl.Add(2, 0);

                icl.Add(3, 0); // 0

                icl.Add(4, 0); // 1
                icl.Add(4, 0);
                icl.Add(4, 0);

                icl.Add(5, 0); // 1 ...

                icl.Add(6, 0); // 0

                icl.Add(5, 0); // ...

                icl.SortBlocks();

                IntComboListEnumerator[] enums = icl.GetEnumerators();
                int xk = 0;
                int nk = 0;
                foreach (IntComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        xk++;
                        nk += en.Current.Count;
                    }
                }
                if (3 != xk)
                {
                    throw new Exception("'Minimum Duplicate Count' fail: number of duplicates");
                }
                if (7 != nk)
                {
                    throw new Exception("'Minimum Duplicate Count' fail: number of keys");
                }

                icl.Close();

                Console.WriteLine("Done");
            }

            {
                Console.WriteLine("Add time test...");

                MySpace.DataMining.DistributedObjects5.IntComboList icl = new MySpace.DataMining.DistributedObjects5.IntComboList("myints");

                //icl.EnableAddBuffer();
                //icl.EnableAddBuffer(1048576); // 1 MB
                //icl.EnableAddBuffer(1048576 / 4); // *
                //icl.EnableAddBuffer(1024 * 64);

                // 100 MB == 104857600
                // div by 8 for capacity since each element is 8 bytes, total to 100 MB.
                icl.AddBlock((104857600 / 8).ToString(), @"127.0.0.1|intcombo100MB_logs0.txt|slaveid=0");

                icl.Open();

                unchecked
                {
                    System.GC.Collect();

                    long start;
                    QueryPerformanceCounter(out start);

                    for (int i = 0; i != 104857600 / 8; i++)
                    {
                        icl.Add(i, i + 1);
                    }

                    //icl.Flush();

                    long stop;
                    QueryPerformanceCounter(out stop);

                    long freq;
                    if (QueryPerformanceFrequency(out freq))
                    {
                        double ddiff = (double)stop - (double)start;
                        Console.WriteLine("  100 MB seconds: {0}", ddiff / (double)freq);
                        Console.WriteLine("    1 GB seconds: {0}", 10.24 * (ddiff / (double)freq));
                    }

                    if (interactive)
                    {
                        Console.WriteLine("Press a key...");
                        Console.ReadKey();
                    }

                    Console.WriteLine("Enumerate time test...");

                    IntComboListEnumerator[] enums = icl.GetEnumerators();
                    //int dummy = 0;

                    System.GC.Collect();

                    QueryPerformanceCounter(out start);
                    foreach (IntComboListEnumerator en in enums)
                    {
                        while (en.MoveNext())
                        {
                            //dummy += en.Current.A;
                            //dummy += en.Current.B;
                        }
                    }
                    QueryPerformanceCounter(out stop);

                    if (QueryPerformanceFrequency(out freq))
                    {
                        double ddiff = (double)stop - (double)start;
                        Console.WriteLine("  100 MB seconds: {0}", ddiff / (double)freq);
                        Console.WriteLine("    1 GB seconds: {0}", 10.24 * (ddiff / (double)freq));
                    }

                    if (interactive)
                    {
                        Console.WriteLine("Press a key...");
                        Console.ReadKey();
                    }
                }

                icl.Close();
            }

#if INVALID_TESTS
            {

                MySpace.DataMining.DistributedObjects5.IntComboList icl = new MySpace.DataMining.DistributedObjects5.IntComboList("myints");

                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs0.txt|slaveid=0");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs1.txt|slaveid=1");
                icl.AddBlock("10KB", @"127.0.0.1|intcombo_logs2.txt|slaveid=2");

                //icl.EnableAddBuffer();

                icl.Open();

                for (int i = 100; i > 0; i--)
                {
                    icl.Add(i, i + 1);
                }

                for (int i = 200; i > 100; i--)
                {
                    icl.Add(i, i + 1);
                }

                //icl.Flush();

                Console.WriteLine("Sorting...");
                icl.SortBlocks();

                MySpace.DataMining.DistributedObjects5.IntComboListEnumerator[] enums = icl.GetEnumerators();
                int nenum = 0;
                foreach (MySpace.DataMining.DistributedObjects5.IntComboListEnumerator en in enums)
                {
                    Console.WriteLine(" In enumerator index " + nenum.ToString());
                    nenum++;
                    int lastnum = -1;
                    while (en.MoveNext())
                    {
                        int a, b;
                        a = en.Current.A;
                        b = en.Current.B;
                        if (a + 1 != b)
                        {
                            throw new Exception("Bad values");
                        }
                        if (a <= lastnum)
                        {
                            throw new Exception("Sort error: values are not sorted");
                        }
                        lastnum = a;
                        Console.Write(" {0} ", a);
                    }
                    Console.WriteLine();
                }

                icl.Close();
            }
#endif

            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");
                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs1.txt|slaveid=1");

                dht.EnableAppendBuffer(100);

                dht.Open();

                // Stressing sets and appends of same key.
                for (int i = 0; i != 100; i++)
                {
                    byte[] key = DistTools.ToBytes("x");
                    byte[] value = new byte[1] { (byte)'u' };
                    dht.Add(key, new byte[0]);
                    for (int j = 0; j != 100; j++)
                    {
                        dht.Append(key, value);
                    }
                }
                byte[] val = dht[DistTools.ToBytes("x")];
                if (val.Length != 100)
                {
                    throw new Exception("test failed: expected 100 bytes");
                }
                foreach (byte b in val)
                {
                    if ((byte)'u' != b)
                    {
                        throw new Exception("test failed: expected 100 bytes containing 'u'");
                    }
                }

                dht.Close();
            }

            {
                Console.WriteLine("Opening from multiple threads...");

                Thread thd1 = new Thread(new ThreadStart(threadprocOpen));
                thd1.Name = "1";
                Thread thd2 = new Thread(new ThreadStart(threadprocOpen));
                thd2.Name = "2";
                Thread thd3 = new Thread(new ThreadStart(threadprocOpen));
                thd3.Name = "3";
                thd1.Start();
                thd2.Start();
                thd3.Start();
                thd1.Join();
                thd2.Join();
                thd3.Join();

                Console.WriteLine("Done opening from multiple threads");
            }

            {
                Console.WriteLine("Opening and closing from multiple threads...");

                Thread thd1 = new Thread(new ThreadStart(threadprocBusyClose));
                thd1.Name = "1";
                Thread thd2 = new Thread(new ThreadStart(threadprocBusyClose));
                thd2.Name = "2";
                Thread thd3 = new Thread(new ThreadStart(threadprocBusyClose));
                thd3.Name = "3";
                thd1.Start();
                thd2.Start();
                thd3.Start();
                thd1.Join();
                thd2.Join();
                thd3.Join();

                Console.WriteLine("Done opening and closing from multiple threads");
            }

            {
                Console.WriteLine("Appending from multiple threads...");

                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");
                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs1.txt|slaveid=1");
                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs2.txt|slaveid=2");

                dht.EnableAppendBuffer(100);

                dht.Open();

                Thread thd1 = new Thread(new ParameterizedThreadStart(threadprocAppendMany));
                thd1.Name = "1";
                Thread thd2 = new Thread(new ParameterizedThreadStart(threadprocAppendMany));
                thd2.Name = "2";
                Thread thd3 = new Thread(new ParameterizedThreadStart(threadprocAppendMany));
                thd3.Name = "3";
                thd1.Start(dht);
                thd2.Start(dht);
                thd3.Start(dht);
                thd1.Join();
                thd2.Join();
                thd3.Join();

                ValidateAppendMany(dht);

                dht.Close();

                Console.WriteLine("Done appending from multiple threads");
            }

            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");

                dht.EnableAppendBuffer(100);

                dht.Open();

                dht[DistTools.ToBytes("asdfasdfasdfasdf")] = DistTools.ToBytes("lalasls");

                //dht[DistTools.ToBytes("asdfasdfasdfasdf")] = new byte[100000000];

                dht.ContainsKey(DistTools.ToBytes("dsfljaskdfjlasdk"));

                dht.Close();
            }

            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");

                dht.EnableAppendBuffer(100);

                dht.Open();

                dht[DistTools.ToBytes("myint")] = DistTools.ToBytes(667);
                dht.MathAdd(DistTools.ToBytes("myint"), 667);
                dht.MathAdd(DistTools.ToBytes("myint"), 3);

                if (1337 != DistTools.BytesToInt(dht[DistTools.ToBytes("myint")]))
                {
                    throw new Exception("MathAdd failure! got: " + DistTools.BytesToInt(dht[DistTools.ToBytes("myint")]).ToString());
                }

                dht.Close();
            }

            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs1.txt|slaveid=1");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs2.txt|slaveid=2");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs3.txt|slaveid=3");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs4.txt|slaveid=4");

                dht.Open();

                dht[DistTools.ToBytes("foo")] = DistTools.ToBytes("bar");
                dht[DistTools.ToBytes("all")] = DistTools.ToBytes("your");
                dht[DistTools.ToBytes(42)] = DistTools.ToBytes("goat");

                byte[] bb;
                //string s;

                bb = dht[DistTools.ToBytes("foo")];
                if (null == bb)
                    throw new Exception("'foo' failure");
                Console.WriteLine("'foo' value is '{0}'", DistTools.BytesToString(bb));

                bb = dht[DistTools.ToBytes("all")];
                if (null == bb)
                    throw new Exception("'all' failure");
                Console.WriteLine("'all' value is '{0}'", DistTools.BytesToString(bb));

                bb = dht[DistTools.ToBytes(42)];
                if (null == bb)
                    throw new Exception("'42' failure");
                Console.WriteLine("'42' value is '{0}'", DistTools.BytesToString(bb));

                if (interactive)
                {
                    //Console.WriteLine("Press a key to close");
                    //Console.ReadKey();
                }

                dht.Close();

                if (interactive)
                {
                    //Console.WriteLine("Done (press a key)");
                    //Console.ReadKey();
                }
            }

            {
                Console.WriteLine("Opening many blocks...");

                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                int stress_amount = 40;

                string computer_name = System.Environment.GetEnvironmentVariable("COMPUTERNAME");
               // if (computer_name == "MAPDDRULE")
                {
                    stress_amount = 4;
                }

                for (int sn = 0; sn != stress_amount; sn++)
                {
                    string ssn = sn.ToString();
                    dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs" + ssn + @".txt|slaveid=" + ssn + "");
                }

                dht.Open();

                dht[DistTools.ToBytes("foo")] = DistTools.ToBytes("bar");
                dht[DistTools.ToBytes("all")] = DistTools.ToBytes("your");
                dht[DistTools.ToBytes(42)] = DistTools.ToBytes("goat");

                byte[] bb;
                //string s;

                bb = dht[DistTools.ToBytes("foo")];
                if (null == bb)
                    throw new Exception("'foo' failure");
                //Console.WriteLine("'foo' value is '{0}'", DistTools.BytesToString(bb));

                bb = dht[DistTools.ToBytes("all")];
                if (null == bb)
                    throw new Exception("'all' failure");
                //Console.WriteLine("'all' value is '{0}'", DistTools.BytesToString(bb));

                bb = dht[DistTools.ToBytes(42)];
                if (null == bb)
                    throw new Exception("'42' failure");
                //Console.WriteLine("'42' value is '{0}'", DistTools.BytesToString(bb));

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                dht.Close();

                if (interactive)
                {
                    //Console.WriteLine("Done (press a key)");
                    //Console.ReadKey();
                }

                Console.WriteLine("Done");
            }

            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");
                MySpace.DataMining.DistributedObjects5.Hashtable dht2
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo2");

                dht.AddBlock("10KB", @"127.0.0.1|foo_hashtable_logs0.txt|slaveid=0");
                dht2.AddBlock("10KB", @"127.0.0.1|foo2_hashtable_logs0.txt|slaveid=0");//notice starts at 0 again

                dht.Open();
                dht2.Open();

                dht[DistTools.ToBytes("foo")] = DistTools.ToBytes("bar");
                dht[DistTools.ToBytes("all")] = DistTools.ToBytes("your");
                dht[DistTools.ToBytes(42)] = DistTools.ToBytes("goat");
                dht2[DistTools.ToBytes("foo")] = DistTools.ToBytes("bar");
                dht2[DistTools.ToBytes("all")] = DistTools.ToBytes("your");
                dht2[DistTools.ToBytes(42)] = DistTools.ToBytes("goat");

                byte[] bb;
                byte[] bb2;
                //string s;

                {
                    bb = dht[DistTools.ToBytes("foo")];
                    if (null == bb)
                        throw new Exception("'foo' failure");
                    Console.WriteLine("'foo' value is '{0}'", DistTools.BytesToString(bb));

                    bb = dht[DistTools.ToBytes("all")];
                    if (null == bb)
                        throw new Exception("'all' failure");
                    Console.WriteLine("'all' value is '{0}'", DistTools.BytesToString(bb));

                    bb = dht[DistTools.ToBytes(42)];
                    if (null == bb)
                        throw new Exception("'42' failure");
                    Console.WriteLine("'42' value is '{0}'", DistTools.BytesToString(bb));

                    if (interactive)
                    {
                        //Console.WriteLine("Press a key to close");
                        //Console.ReadKey();
                    }
                }

                {
                    bb2 = dht2[DistTools.ToBytes("foo")];
                    if (null == bb2)
                        throw new Exception("'foo' failure");
                    Console.WriteLine("'foo' value is '{0}'", DistTools.BytesToString(bb2));

                    bb2 = dht2[DistTools.ToBytes("all")];
                    if (null == bb2)
                        throw new Exception("'all' failure");
                    Console.WriteLine("'all' value is '{0}'", DistTools.BytesToString(bb2));

                    bb2 = dht2[DistTools.ToBytes(42)];
                    if (null == bb2)
                        throw new Exception("'42' failure");
                    Console.WriteLine("'42' value is '{0}'", DistTools.BytesToString(bb2));

                    if (interactive)
                    {
                        //Console.WriteLine("Press a key to close");
                        //Console.ReadKey();
                    }
                }

                dht.Close();
                dht2.Close();

                if (interactive)
                {
                    //Console.WriteLine("Done (press a key)");
                    //Console.ReadKey();
                }
            }

            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");
                //dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs1.txt|slaveid=1");
                //dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs2.txt|slaveid=2");
                //dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs3.txt|slaveid=3");
                //dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs4.txt|slaveid=4");

                dht.EnableAppendBuffer(120);

                dht.Open();

                if (DistTools.ToBytes(22).Length != 4)
                    throw new Exception("(int)22 to bytes is not 4 bytes; was: " + DistTools.ToBytes(22).Length.ToString());
                dht[DistTools.ToBytes("zoom")] = DistTools.ToBytes(22);
                dht.Append(DistTools.ToBytes("zoom"), DistTools.ToBytes(9000001));
                if (dht.Length(DistTools.ToBytes("zoom")) != 8)
                    throw new Exception("'zoom' failure; not 8 bytes (2 ints); was: " + dht.Length(DistTools.ToBytes("zoom")).ToString());
                int[] ix = DistTools.BytesToIntArray(dht[DistTools.ToBytes("zoom")]);
                if (ix.Length != 2)
                    throw new Exception("'zoom' failure; not 2 ints; was: " + ix.Length.ToString());
                if (ix[0] != 22 || ix[1] != 9000001)
                    throw new Exception("'zoom' failure; int-byte conversion error");

                dht.Close();

                if (interactive)
                {
                    //Console.WriteLine("Done (press a key)");
                    //Console.ReadKey();
                }
            }

            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs1.txt|slaveid=1");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs2.txt|slaveid=2");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs3.txt|slaveid=3");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs4.txt|slaveid=4");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs5.txt|slaveid=5");
                dht.AddBlock("10KB", @"127.0.0.1|hashtable_logs6.txt|slaveid=6");

                dht.EnableAppendBuffer(120);

                dht.Open();

                dht[DistTools.ToBytes("one")] = DistTools.ToBytes("1!");
                dht[DistTools.ToBytes("two")] = DistTools.ToBytes("2 2!");
                dht[DistTools.ToBytes("three")] = DistTools.ToBytes("3 3 3!");
                dht[DistTools.ToBytes("sure")] = DistTools.ToBytes("right");
                dht[DistTools.ToBytes("ok")] = DistTools.ToBytes("ok");
                dht[DistTools.ToBytes("a")] = DistTools.ToBytes("b!");
                dht[DistTools.ToBytes("cc")] = DistTools.ToBytes("dd!");
                dht[DistTools.ToBytes("eeeee")] = DistTools.ToBytes("E");
                dht[DistTools.ToBytes("F")] = DistTools.ToBytes("F!");
                dht[DistTools.ToBytes("Gg")] = DistTools.ToBytes("gG");
                dht[DistTools.ToBytes("a")] = DistTools.ToBytes("b!");
                dht[DistTools.ToBytes("a")] = DistTools.ToBytes("b!");
                dht[DistTools.ToBytes("a")] = DistTools.ToBytes("b!");
                dht[DistTools.ToBytes("x")] = DistTools.ToBytes("x!");
                dht[DistTools.ToBytes("y")] = DistTools.ToBytes("y!");
                dht[DistTools.ToBytes("z")] = DistTools.ToBytes("z!");
                dht[DistTools.ToBytes("z")] = DistTools.ToBytes("z!");
                dht[DistTools.ToBytes("234")] = DistTools.ToBytes("234!");
                dht[DistTools.ToBytes("2343")] = DistTools.ToBytes("2343!");
                dht[DistTools.ToBytes("234344")] = DistTools.ToBytes("234344!");
                dht[DistTools.ToBytes("IntArray")] = DistTools.ToBytes(1111);
                dht.Append(DistTools.ToBytes("IntArray"), DistTools.ToBytes(22222222));

                System.Collections.IDictionaryEnumerator[] enums = dht.GetEnumerators();
                int nenum = 0;
                foreach (System.Collections.IDictionaryEnumerator en in enums)
                {
                    Console.WriteLine(" In enumerator index " + nenum.ToString());
                    nenum++;
                    while (en.MoveNext())
                    {
                        if (DistTools.BytesToString((byte[])en.Key) == "IntArray")
                        {
                            int[] ia = DistTools.BytesToIntArray((byte[])en.Value);
                            Console.WriteLine("  <found IntArray> {0}, {1}", ia[0], ia[1]);
                        }
                        else
                        {
                            Console.WriteLine("  '{0}'='{1}'", DistTools.BytesToString((byte[])en.Key), DistTools.BytesToString((byte[])en.Value));
                        }

                        if (!dht.ContainsKey(DistTools.ToBytes("sure")))
                        {
                            throw new Exception("Contains error: 'sure' not found");
                        }

                        if (dht.ContainsKey(DistTools.ToBytes("blabberblab")))
                        {
                            throw new Exception("Contains error: 'blabberblab' was found when it shouldn't");
                        }
                    }
                }

                dht.Close();
            }

            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");

                dht.Open();

                dht.ContainsKey(DistTools.ToBytes("dsfljaskdfjlasdk"));

                dht.Close();
            }

            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");

                dht.EnableAppendBuffer(100);

                dht.Open();

                dht.ContainsKey(DistTools.ToBytes("dsfljaskdfjlasdk"));

                dht.Close();
            }


            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                    = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");
                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=1");

                //dht.EnableAppendBuffer(20);

                dht.Open();

                Console.WriteLine("Adding {0} fruits to hashtable...", FRUIT.Length);

                Thread thd1 = new Thread(new ParameterizedThreadStart(threadprocAdd));
                thd1.Name = "1";
                Thread thd2 = new Thread(new ParameterizedThreadStart(threadprocAdd));
                thd2.Name = "2";
                thd1.Start(dht);
                thd2.Start(dht);
                thd1.Join();
                thd2.Join();

                Console.WriteLine("Added. Enumerating and printing...");

                thd1 = new Thread(new ParameterizedThreadStart(threadprocEnum));
                thd1.Name = "1";
                thd2 = new Thread(new ParameterizedThreadStart(threadprocEnum));
                thd2.Name = "2";
                System.Collections.IDictionaryEnumerator[] enums = dht.GetEnumerators();
                thd1.Start(enums[0]);
                thd2.Start(enums[1]);
                thd1.Join();
                thd2.Join();

                Console.WriteLine();
                Console.WriteLine("Done");

                Console.WriteLine("{0} fruits of {0} were enumerated", enumd, FRUIT.Length);
                if (enumd != FRUIT.Length)
                {
                    throw new Exception("Not all fruits were enumerated!");
                }

                dht.Close();
            }


        buffer_limits: ;

            {
                Console.WriteLine("Pushing limits of IntComboList buffering (this may take a while!)...");

                MySpace.DataMining.DistributedObjects5.IntComboList icl
                    = new MySpace.DataMining.DistributedObjects5.IntComboList("foo");

                icl.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");

                icl.Open();


                int end = IntComboList.FILE_BUFFER_SIZE + IntComboList.FILE_BUFFER_SIZE / 8 + 1;

                Console.WriteLine("    0% adding");
                for (int i = 0; i != end; i++)
                {
                    icl.Add(i, 0);
                    if (i == end / 4)
                    {
                        Console.WriteLine("   25% adding");
                    }
                    if (i == end / 2)
                    {
                        Console.WriteLine("   50% adding");
                    }
                    if (i == end / 2 + end / 4)
                    {
                        Console.WriteLine("   75% adding");
                    }
                }
                Console.WriteLine("  100% adding");

                //icl.SortBlocks();

                Console.WriteLine("    0% enumerating");
                IntComboListEnumerator[] enums = icl.GetEnumerators();
                int nkeys = 0;
                foreach (IntComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        if (1 != en.Current.Count)
                        {
                            int i32333 = 32333;
                        }
                        for (int ik = 0; ik != en.Current.Count; ik++) // Roll it back out for test!
                        {
                            nkeys++;
                            if (nkeys == end / 4)
                            {
                                Console.WriteLine("   25% enumerating");
                            }
                            if (nkeys == end / 2)
                            {
                                Console.WriteLine("   50% enumerating");
                            }
                            if (nkeys == end / 2 + end / 4)
                            {
                                Console.WriteLine("   75% enumerating");
                            }
                        }
                    }
                }
                if (nkeys != end)
                {
                    throw new Exception("Buffer test failed! Only got back " + nkeys.ToString() + " of " + end.ToString() + " keys");
                }
                Console.WriteLine("  100% enumerating");
                Console.WriteLine("Done");

                icl.Close();
            }

            {
                Console.WriteLine("Pushing limits of LongIntComboList buffering (this may take a while!)...");

                MySpace.DataMining.DistributedObjects5.LongIntComboList icl
                    = new MySpace.DataMining.DistributedObjects5.LongIntComboList("lfoo");

                icl.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");

                icl.Open();


                int end = IntComboList.FILE_BUFFER_SIZE + IntComboList.FILE_BUFFER_SIZE / 8 + 1;

                Console.WriteLine("    0% adding");
                for (int i = 0; i != end; i++)
                {
                    icl.Add(i, 0);
                    if (i == end / 4)
                    {
                        Console.WriteLine("   25% adding");
                    }
                    if (i == end / 2)
                    {
                        Console.WriteLine("   50% adding");
                    }
                    if (i == end / 2 + end / 4)
                    {
                        Console.WriteLine("   75% adding");
                    }
                }
                Console.WriteLine("  100% adding");

                //icl.SortBlocks();

                Console.WriteLine("    0% enumerating");
                LongIntComboListEnumerator[] enums = icl.GetEnumerators();
                int nkeys = 0;
                foreach (LongIntComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        if (1 != en.Current.Count)
                        {
                            int i32333 = 32333;
                        }
                        for (int ik = 0; ik != en.Current.Count; ik++) // Roll it back out for test!
                        {
                            nkeys++;
                            if (nkeys == end / 4)
                            {
                                Console.WriteLine("   25% enumerating");
                            }
                            if (nkeys == end / 2)
                            {
                                Console.WriteLine("   50% enumerating");
                            }
                            if (nkeys == end / 2 + end / 4)
                            {
                                Console.WriteLine("   75% enumerating");
                            }
                        }
                    }
                }
                if (nkeys != end)
                {
                    throw new Exception("Buffer test failed! Only got back " + nkeys.ToString() + " of " + end.ToString() + " keys");
                }
                Console.WriteLine("  100% enumerating");
                Console.WriteLine("Done");

                icl.Close();
            }


            {
                Console.WriteLine("Pushing limits of ArrayComboList reducer buffering (this may take a while!)...");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("stressbuf", 4);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                int end = (1024 * 8) * 8 * 8;

                Console.WriteLine("    0% adding");
                for (int i = 0; i != end / 8; i++)
                {
                    keybuf.Clear();
                    MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(i, keybuf);
                    valuebuf.Clear();
                    MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(0, valuebuf);
                    acl.Add(keybuf, valuebuf);
                    acl.Add(keybuf, valuebuf);
                    acl.Add(keybuf, valuebuf);
                    acl.Add(keybuf, valuebuf);
                    acl.Add(keybuf, valuebuf);
                    acl.Add(keybuf, valuebuf);
                    acl.Add(keybuf, valuebuf);
                    acl.Add(keybuf, valuebuf);
                    if (i == end / 8 / 4)
                    {
                        Console.WriteLine("   25% adding");
                    }
                    if (i == end / 8 / 2)
                    {
                        Console.WriteLine("   50% adding");
                    }
                    if (i == end / 8 / 2 + end / 8 / 4)
                    {
                        Console.WriteLine("   75% adding");
                    }
                }
                Console.WriteLine("  100% adding");

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(@"
                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            for(int i = 0; i != values.Length; i++)
                            {  
                                output.Add(key, values[i].Value);
                            }
                        }
", null);
                int nkeys = 0;
                Console.WriteLine("    0% enumerating");
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        nkeys++;
                        if (nkeys == end / 4)
                        {
                            Console.WriteLine("   25% enumerating");
                        }
                        if (nkeys == end / 2)
                        {
                            Console.WriteLine("   50% enumerating");
                        }
                        if (nkeys == end / 2 + end / 4)
                        {
                            Console.WriteLine("   75% enumerating");
                        }
                    }
                }
                Console.WriteLine("  100% enumerating");
                if (nkeys != end)
                {
                    throw new Exception("nkeys != end");
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                acl.Close();

                Console.WriteLine("Done");
            }



            //if (interactive)
            {
                // Keep this last...
                Console.WriteLine("DONE! (press a key)");
                Console.ReadKey();
            }
        }


        static string[] FRUIT = { "Apple", "Crabapple", "Hawthorn", "Pear", "Apricot", "Peach", "Nectarines", "Plum", "Cherry", "Blackberry", "Raspberry", "Mulberry", "Strawberry", "Cranberry", "Blueberry", "Barberry", "Currant", "Gooseberry", "Elderberry", "Grapes", "Grapefruit", "Kiwi fruit", "Rhubarb", "Pawpaw", "Papaya", "Melon", "Watermelon", "Figs", "Dates", "Olive", "Jujube", "Pomegranate", "Lemon", "Lime", "Key Lime", "Mandarin", "Orange", "Sweet Lime", "Tangerine", "Avocado", "Guava", "Kumquat", "Lychee", "Passion Fruit", "Tomato", "Banana", "Gourd ", "Bitter Gourd", "Bottle Gourd", "Cashew Fruit", "Cacao", "Coconut", "Custard Apple", "Jackfruit", "Mango", "Neem", "Okra", "Pineapple", "Vanilla", "Carrot" };

        public static void threadprocAdd(object obj)
        {
            MySpace.DataMining.DistributedObjects5.Hashtable dht = (MySpace.DataMining.DistributedObjects5.Hashtable)obj;

            int half = FRUIT.Length / 2;
            int i, end;
            if (Thread.CurrentThread.Name == "1")
            {
                i = 0;
                end = half;
            }
            else //if (Thread.CurrentThread.Name == "2")
            {
                i = half;
                end = FRUIT.Length;
            }

            for (; i != end; i++)
            {
                dht.Add(DistTools.ToBytes("Fruit" + i.ToString()), DistTools.ToBytes(FRUIT[i]));
            }
        }

        static int enumd = 0;

        public static void threadprocEnum(object obj)
        {
            System.Collections.IDictionaryEnumerator en = (System.Collections.IDictionaryEnumerator)obj;

            while (en.MoveNext())
            {
                Thread.Sleep(enumd % 24);
                Interlocked.Increment(ref enumd);
                Console.Write("  thread{0}.'{1}'='{2}'; ", Thread.CurrentThread.Name, DistTools.BytesToString((byte[])en.Key), DistTools.BytesToString((byte[])en.Value));
            }
        }


        static byte[][] MANY = {
                               new byte[] { (byte)'x', (byte)'y', (byte)'z' },
                               new byte[] { (byte)'a', (byte)'b', (byte)'c' },
                               new byte[] { (byte)'c', (byte)'c', (byte)'c' },
                               new byte[] { (byte)'a', (byte)'c', (byte)'c' },
                               new byte[] { (byte)'b', (byte)'c', (byte)'c' },
                               new byte[] { (byte)'d', (byte)'b', (byte)'c' },
                               new byte[] { (byte)'r', (byte)'b', (byte)'c' },
                               new byte[] { (byte)'m', (byte)'b', (byte)'c' },
                               new byte[] { (byte)'p', (byte)'b', (byte)'c' },
                               new byte[] { (byte)'x', (byte)'y', (byte)'a' },
                               new byte[] { (byte)'x', (byte)'y', (byte)'b' },
                               new byte[] { (byte)'x', (byte)'y', (byte)'c' },
                               new byte[] { (byte)'x', (byte)'y', (byte)'d' },
                               new byte[] { (byte)'x', (byte)'y', (byte)'e' },
                               new byte[] { (byte)'x', (byte)'y', (byte)'f' },
                               new byte[] { (byte)'x', (byte)'y', (byte)'g' },
                               new byte[] { (byte)'b', (byte)'e', (byte)'c' },
                               new byte[] { (byte)'b', (byte)'f', (byte)'c' },
                               new byte[] { (byte)'b', (byte)'g', (byte)'c' },
                               new byte[] { (byte)'b', (byte)'h', (byte)'c' },
                               new byte[] { (byte)'b', (byte)'i', (byte)'c' },
                               new byte[] { (byte)'b', (byte)'j', (byte)'c' },
                        };

        public static void ValidateAppendMany(MySpace.DataMining.DistributedObjects5.Hashtable dht)
        {
            // Validate...
            foreach (byte[] xx in MANY)
            {
                if (dht[xx].Length != 200 * 3 * 3) // Looped 200 times, appending 3 chars from 3 threads.
                {
                    throw new Exception("Bad appends in multiple threads");
                }
            }
        }

        public static void threadprocAppendMany(object obj)
        {
            MySpace.DataMining.DistributedObjects5.Hashtable dht = (MySpace.DataMining.DistributedObjects5.Hashtable)obj;

            Random rnd = new Random();
            /*

            for (int i = 0; i != 200; i++)
            {
                dht.Append(MANY[rnd.Next() % MANY.Length], MANY[rnd.Next() % MANY.Length]);
            }
             * */

            for (int rep = 0; rep != 200; rep++)
            {
                if (0 == (rnd.Next() % 8))
                {
                    Thread.Sleep(10);
                }

                if (Thread.CurrentThread.Name == "1")
                {
                    for (int i = 0; i != MANY.Length; i++)
                    {
                        dht.Append(MANY[i], MANY[i]);
                    }
                }
                else if (Thread.CurrentThread.Name == "2")
                {
                    for (int i = MANY.Length - 1; ; i--)
                    {
                        dht.Append(MANY[i], MANY[i]);

                        if (0 == i)
                        {
                            break;
                        }
                    }
                }
                else
                {
                    for (int i = 0; i != MANY.Length / 2; i++)
                    {
                        dht.Append(MANY[i], MANY[i]);
                    }
                    for (int i = MANY.Length / 2; i != MANY.Length; i++)
                    {
                        dht.Append(MANY[i], MANY[i]);
                    }
                }
            }
        }


        public static void threadprocOpen()
        {
            Random rnd = new Random();

            for (int i = 0; i != 10; i++)
            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                        = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");
                dht.AddBlock("2KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=1");
                dht.AddBlock("3KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=2");
                if (0 == (rnd.Next() % 3))
                {
                    dht.AddBlock("1MB", @"127.0.0.1|hashtable_logs0.txt|slaveid=3");
                    dht.AddBlock("2MB", @"127.0.0.1|hashtable_logs0.txt|slaveid=4");
                    dht.AddBlock("3MB", @"127.0.0.1|hashtable_logs0.txt|slaveid=5");
                }

                if (0 == (rnd.Next() % 3))
                {
                    dht.EnableAppendBuffer(rnd.Next() % 1000);
                }

                dht.Open();

                Thread.Sleep(rnd.Next() % 100);

                dht[MANY[0]] = MANY[1];
                if (!dht.ContainsKey(MANY[0]))
                {
                    throw new Exception("key expected");
                }
                if (dht.ContainsKey(MANY[1]))
                {
                    throw new Exception("key NOT expected");
                }

                dht.Close();
            }
        }


        public static void threadprocBusyClose()
        {
            Random rnd = new Random();

            for (int j = 0; j != 10; j++)
            {
                MySpace.DataMining.DistributedObjects5.Hashtable dht
                        = new MySpace.DataMining.DistributedObjects5.Hashtable("foo");

                dht.AddBlock("1KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=0");
                dht.AddBlock("2KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=1");
                dht.AddBlock("3KB", @"127.0.0.1|hashtable_logs0.txt|slaveid=2");
                if (0 == (rnd.Next() % 3))
                {
                    dht.AddBlock("2", @"127.0.0.1|hashtable_logs0.txt|slaveid=3");
                    dht.AddBlock("3", @"127.0.0.1|hashtable_logs0.txt|slaveid=4");
                    dht.AddBlock("4", @"127.0.0.1|hashtable_logs0.txt|slaveid=5");
                }

                if (0 == (rnd.Next() % 3))
                {
                    dht.EnableAppendBuffer(rnd.Next() % 1000);
                }

                Console.Write("O");
                dht.Open();

                if (0 == (rnd.Next() % 3))
                {
                    Thread.Sleep(rnd.Next() % 100);
                }

                dht[MANY[0]] = MANY[1];
                if (!dht.ContainsKey(MANY[0]))
                {
                    throw new Exception("key expected");
                }
                if (dht.ContainsKey(MANY[1]))
                {
                    throw new Exception("key NOT expected");
                }

                for (int i = 0; i != MANY.Length / 2; i++)
                {
                    dht.Append(MANY[i], MANY[i]);
                }
                if ((j % 2) == 0 && Thread.CurrentThread.Name == "1")
                {
                    Console.Write("C");
                    dht.Close();
                    continue;
                }
                else
                {
                    for (int i = MANY.Length / 2; i != MANY.Length; i++)
                    {
                        dht.Append(MANY[i], MANY[i]);
                    }
                }

                Console.Write("C");
                dht.Close();
            }
        }


        [DllImport("kernel32.dll")]
        private static extern bool QueryPerformanceCounter(out long lpPerformanceCount);

        [DllImport("kernel32.dll")]
        private static extern bool QueryPerformanceFrequency(out long lpFrequency);

    }
}
