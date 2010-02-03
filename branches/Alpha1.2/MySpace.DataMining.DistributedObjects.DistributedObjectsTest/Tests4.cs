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
    public class Tests4
    {
        public static void RunTests(bool interactive, char kc)
        {
            {
                using (MySpace.DataMining.DistributedObjects.GzipFastLoader gz = new MySpace.DataMining.DistributedObjects.GzipFastLoader("intlines.txt.gz", 64, 10))
                {
                    int a;
                    gz.ReadLine(out a);
                    gz.ReadLine(out a);
                    for (int i = 0; i < 100; i++)
                    {
                        gz.ReadLine(out a);
                    }
                    int i3 = 333 + 333;
                }
                int i333 = 333 + 333;
            }

            {
                List<int> nums = new List<int>();
                using (MySpace.DataMining.DistributedObjects.GzipFastLoader gz = new MySpace.DataMining.DistributedObjects.GzipFastLoader("intlines.txt.gz", 64, 10))
                {
                    for(;;)
                    {
                        nums.Clear();
                        if (!gz.ReadLineAppend(nums))
                        {
                            break;
                        }
                        int i3 = 333 + 333;
                    }
                }
            }


            {
                Console.WriteLine("Testing FixedArrayComboList with int key and 0-length value.");

                MySpace.DataMining.DistributedObjects5.FixedArrayComboList facl = new MySpace.DataMining.DistributedObjects5.FixedArrayComboList("ffooobj", 4, 0);

                facl.AddBlock(@"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                facl.Open();

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
                        facl.Add(keybuf, valuebuf);
                    }
                }

                facl.SortBlocks();

                int n = 0;
                FixedArrayComboListEnumerator[] enums = facl.GetEnumerators();
                foreach (FixedArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        int x = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                        //Console.Write(" {0} ", x);
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

                facl.Close();

                Console.WriteLine("Done");
            }


            {
                Console.WriteLine("Testing FixedArrayComboList with int key and 0-length value in reducer.");

                MySpace.DataMining.DistributedObjects5.FixedArrayComboList facl = new MySpace.DataMining.DistributedObjects5.FixedArrayComboList("ffooobj", 4, 0);

                facl.AddBlock(@"127.0.0.1|arraycombo_logs0.txt|slaveid=0");
                facl.AddBlock(@"127.0.0.1|arraycombo_logs1.txt|slaveid=1");

                facl.Open();

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
                        facl.Add(keybuf, valuebuf); // 0-length value.
                    }
                }

                facl.SortBlocks();

                FixedArrayComboListEnumerator[] enums = facl.GetEnumeratorsWithCode(@"
                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            output.Add(key, values[0].Value);
                            if(" + maxtimes.ToString() + @" != values.Length)
                            {
                                throw new OverflowException();
                            }
                        }
");
                int n = 0;
                foreach (FixedArrayComboListEnumerator en in enums)
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

                facl.Close();

                Console.WriteLine("Done");
            }


            {
                Console.WriteLine("Testing FixedArrayComboList before-load: random data stress test and validation.");

                MySpace.DataMining.DistributedObjects5.FixedArrayComboList facl = new MySpace.DataMining.DistributedObjects5.FixedArrayComboList("fstv", 4, 4);

                facl.AddBlock(@"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                facl.Open();

                Random rnd = new Random();
                List<int> input = new List<int>(5000);
                Dictionary<int, bool> hi = new Dictionary<int, bool>(5000);

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                {
                    for (int i = 0; i < 5000; i++)
                    {
                        int x;
                        do
                        {
                            if (0 == (i % 300))
                            {
                                x = rnd.Next(30, 300);
                            }
                            else
                            {
                                x = rnd.Next();
                            }
                        }
                        while (hi.ContainsKey(x));
                        hi[x] = true;
                        input.Add(x);
                    }
                }

                StringBuilder blinput = new StringBuilder(@"int[] all = new int[] { ", 10000);
                for (int i = 0; i < input.Count; i++)
                {
                    if (0 != i)
                    {
                        blinput.Append(", ");
                    }
                    blinput.Append(input[i]);
                }
                blinput.Append(" };");

                facl.BeforeLoad(@"
        public virtual void OnBeforeLoad(MySpace.DataMining.DistributedObjects.LoadOutput output)
        {
            " + blinput.ToString() + @"

            List<byte> keybuf = new List<byte>(128);
            List<byte> valuebuf = new List<byte>(128);

            for (int i = 0; i < all.Length; i++)
            {
                int k = 1 + all[i] % 8;
                if(all[i] < 500)
                {
                    k += all[i];
                }
                for(int j = 0; j != k; j++)
                {
                    keybuf.Clear();
                    Entry.ToBytesAppend(all[i], keybuf);
                    valuebuf.Clear();
                    Entry.ToBytesAppend(all[j], valuebuf);
                    output.Add(ByteSlice.Create(keybuf), ByteSlice.Create(valuebuf));
                }
            }
        }");

                facl.SortBlocks();

                FixedArrayComboListEnumerator[] enums = facl.GetEnumeratorsWithCode(@"
                        List<byte> valuebuf = new List<byte>(128);

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            output.Add(key, values[0].Value);
                        }
");
                int n = 0;
                foreach (FixedArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        if (en.Current.GetKeyLength() != 4)
                        {
                            throw new Exception("Key length mismatch");
                        }
                        if (4 != en.Current.GetValueLength())
                        {
                            throw new Exception("Value length incorrect");
                        }
                        n++;

                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        int ikey = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                        if (!hi.ContainsKey(ikey))
                        {
                            throw new Exception("Unexpected key");
                        }
                        int k = 1 + ikey % 8;
                        if (ikey < 500)
                        {
                            k += ikey;
                        }
                    }
                }
                if (input.Count != n)
                {
                    throw new Exception("Test failed: expected " + input.Count.ToString() + " key/values, got " + n.ToString());
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                facl.Close();

                Console.WriteLine("Done");
            }


            {
                Console.WriteLine("FixedArrayComboList test with dymamically created inline C# reducer");

                MySpace.DataMining.DistributedObjects5.FixedArrayComboList facl = new MySpace.DataMining.DistributedObjects5.FixedArrayComboList("faclxobj2", 4, 4);

                facl.AddBlock(@"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                facl.Open();

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
                    facl.Add(keybuf, valuebuf);
                }

                facl.SortBlocks();

                StringBuilder dictcode = new StringBuilder(128);
                dictcode.Append("kv = new Dictionary<int, int>();");
                foreach (KeyValuePair<int, int> pair in kv)
                {
                    dictcode.Append("kv[" + pair.Key.ToString() + "] = " + pair.Value.ToString() + ";");
                }

                FixedArrayComboListEnumerator[] enums = facl.GetEnumeratorsWithCode(@"
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
");
                foreach (FixedArrayComboListEnumerator en in enums)
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

                facl.Close();

                Console.WriteLine("Done");
            }


            {
                Console.WriteLine("Confirming correct zblock distribution with FixedArrayComboList:");

                if (interactive)
                {
                    Console.WriteLine("Press a key...");
                    Console.ReadKey();
                }

                MySpace.DataMining.DistributedObjects5.FixedArrayComboList facl = new MySpace.DataMining.DistributedObjects5.FixedArrayComboList("faclzfoo", 4, 4);

                facl.AddBlock(@"127.0.0.1|intcombo_logs0.txt|slaveid=0");

                facl.Open();

                System.Collections.Generic.Queue<int> wheel = new Queue<int>();
                System.Collections.Generic.List<int> al = new List<int>();
                Random rnd = new Random(System.DateTime.Now.Millisecond);
                System.Collections.Hashtable ht = new System.Collections.Hashtable();

                List<byte> keybuf = new List<byte>();

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
                    keybuf.Clear();
                    MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(key, keybuf);
                    facl.Add(keybuf, keybuf);
                    added++;
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to sort...");
                    Console.ReadKey();
                }

                facl.SortBlocks();

                FixedArrayComboListEnumerator[] enums = facl.GetEnumerators();
                System.Collections.Generic.List<int> outlist = new List<int>();
                foreach (FixedArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        int key = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                        outlist.Add(key);
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

                facl.Close();
            }


            {
                Console.WriteLine("Testing GzipFastLoader...");
                MySpace.DataMining.DistributedObjects.GzipFastLoader gz = new MySpace.DataMining.DistributedObjects.GzipFastLoader("intlines.txt.gz", 64);
                int i = 0;
                for (; !gz.EOF; i++)
                {
                    int a, b, c, d;
                    if (!gz.ReadLine(out a, out b, out c, out d))
                    {
                        throw new Exception("Whoops, it was supposed to hit gz.EOF first");
                    }
                    if (0 == (i % 4))
                    {
                        if (0 != a || 0 != b || 0 != c || 0 != d)
                        {
                            throw new Exception("Line " + (i + 1).ToString() + " should give me 0 but it doesn't!!");
                        }
                    }
                    else
                    {
                        if (i != a)
                        {
                            throw new Exception("i != a");
                        }
                        switch (i)
                        {
                            case 1: if (0 != b) throw new Exception(i.ToString()); break;
                            case 2: if (0 != c) throw new Exception(i.ToString()); break;
                            case 3: if (0 != d) throw new Exception(i.ToString()); break;
                            default: if (0 == d) throw new Exception(i.ToString()); break;
                        }
                    }
                }
                if (10 != i)
                {
                    throw new Exception("Bad number of lines!");
                }
                Console.WriteLine("Done");
            }


#if OBSOLETE
            {
                Console.WriteLine("Testing ArrayComboList with simple before-load (primitive mapper).");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("foomap", 4);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                acl.BeforeLoad(@"
        public virtual void OnBeforeLoad(MySpace.DataMining.DistributedObjects.LoadOutput output)
        {
            List<byte> keybuf = new List<byte>(128);
            List<byte> valuebuf = new List<byte>(128);

            for (int i = 0; i < 20; i++)
            {
                keybuf.Clear();
                Entry.ToBytesAppend(i, keybuf);
                valuebuf.Clear();
                Entry.ToBytesAppend64(42, valuebuf);
                output.Add(ByteSlice.Create(keybuf), ByteSlice.Create(valuebuf));
            }
        }");

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumerators();
                int n = 0;
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        if (en.Current.GetKeyLength() != 4)
                        {
                            throw new Exception("Key length mismatch");
                        }
                        if (en.Current.GetValueLength() != 8)
                        {
                            throw new Exception("Value length mismatch");
                        }
                        n++;
                    }
                }
                if (20 != n)
                {
                    throw new Exception("Test failed: expected 20 key/values, only got " + n.ToString());
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
                Console.WriteLine("Testing ArrayComboList before-load against regular add.");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("foomap2", 4);
                acl.atype = true;

                // Must have exactly 1 block(slave) for this test to work!!
                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                {
                    for (int i = 0; i < 20; i++)
                    {
                        keybuf.Clear();
                        MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(i, keybuf);
                        valuebuf.Clear();
                        MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend64(42, valuebuf);
                        acl.Add(keybuf, valuebuf);
                    }
                }

                acl.BeforeLoad(@"
        public virtual void OnBeforeLoad(MySpace.DataMining.DistributedObjects.LoadOutput output)
        {
            List<byte> keybuf = new List<byte>(128);
            List<byte> valuebuf = new List<byte>(128);

            for (int i = 0; i < 20; i++)
            {
                keybuf.Clear();
                Entry.ToBytesAppend(i, keybuf);
                valuebuf.Clear();
                Entry.ToBytesAppend64(42, valuebuf);
                output.Add(ByteSlice.Create(keybuf), ByteSlice.Create(valuebuf));
            }
        }");

                {
                    for (int i = 0; i < 20; i++)
                    {
                        keybuf.Clear();
                        MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend(i, keybuf);
                        valuebuf.Clear();
                        MySpace.DataMining.DistributedObjects.Entry.ToBytesAppend64(42, valuebuf);
                        acl.Add(keybuf, valuebuf);
                    }
                }

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(@"
                        List<byte> valuebuf = new List<byte>();

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            if(3 != values.Length)
                            {
                                throw new Exception(3.ToString() + '!'.ToString() + values.Length.ToString());
                            }
                            
                            for(int i = 0; i != values.Length; i++)
                            {
                                if(8 != values[i].Value.Length)
                                {
                                    throw new Exception(3.ToString() + '!'.ToString() + values[i].Value.Length.ToString());
                                }
                                valuebuf.Clear();
                                values[i].Value.AppendTo(valuebuf);
                                long ivalue = MySpace.DataMining.DistributedObjects.Entry.BytesToLong(valuebuf);
                                if(42 != ivalue)
                                {
                                    throw new Exception(42.ToString() + '!'.ToString() + ivalue.ToString());
                                }
                            }

                            valuebuf.Clear();
                            Entry.ToBytesAppend64(42 * 3, valuebuf);
                            output.Add(key, ByteSlice.Create(valuebuf));
                        }
");
                int n = 0;
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        if (en.Current.GetKeyLength() != 4)
                        {
                            throw new Exception("Key length mismatch");
                        }
                        if (en.Current.GetValueLength() != 8)
                        {
                            throw new Exception("Value length mismatch");
                        }
                        n++;
                    }
                }
                if (20 != n)
                {
                    throw new Exception("Test failed: expected 20 key/values, only got " + n.ToString());
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
                Console.WriteLine("Testing ArrayComboList with GzipFastLoader in before-load.");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("gzmap", 4);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                acl.BeforeLoad(@"
        public virtual void OnBeforeLoad(MySpace.DataMining.DistributedObjects.LoadOutput output)
        {
            List<byte> keybuf = new List<byte>(128);
            List<byte> valuebuf = new List<byte>(128);

            MySpace.DataMining.DistributedObjects.GzipFastLoader gz = new MySpace.DataMining.DistributedObjects.GzipFastLoader(" + "\"intlines.txt.gz\"" + @", 64);
                int i = 0;
                for (; !gz.EOF; i++)
                {
                    int a, b, c, d;
                    gz.ReadLine(out a, out b, out c, out d);
                    keybuf.Clear();
                    Entry.ToBytesAppend(a, keybuf);
                    valuebuf.Clear();
                    Entry.ToBytesAppend(b, valuebuf);
                    Entry.ToBytesAppend(c, valuebuf);
                    Entry.ToBytesAppend(d, valuebuf);
                    output.Add(ByteSlice.Create(keybuf), ByteSlice.Create(valuebuf));
                }
                if (10 != i)
                {
                    throw new Exception('!'.ToString());
                }
        }");

                acl.SortBlocks();

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                ArrayComboListEnumerator[] enums = acl.GetEnumerators();
                int n = 0;
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        if (en.Current.GetKeyLength() != 4)
                        {
                            throw new Exception("Key length mismatch");
                        }
                        if (en.Current.GetValueLength() != 12)
                        {
                            throw new Exception("Value length mismatch");
                        }
                        n++;

                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        int a = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);

                        valuebuf.Clear();
                        en.Current.CopyValueAppend(valuebuf);
                        int b = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf, 0);
                        int c = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf, 4);
                        int d = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf, 8);

                        //if(0 != a)
                        {
                            switch (a)
                            {
                                case 0: break;
                                case 1: if (0 != b) throw new Exception(a.ToString()); break;
                                case 2: if (0 != c) throw new Exception(a.ToString()); break;
                                case 3: if (0 != d) throw new Exception(a.ToString()); break;
                                default: if (0 == d) throw new Exception(a.ToString()); break;
                            }
                        }
                    }
                }
                if (10 != n)
                {
                    throw new Exception("Test failed: expected 10 key/values, only got " + n.ToString());
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
                Console.WriteLine("Testing ArrayComboList before-load: random data stress test and validation.");

                MySpace.DataMining.DistributedObjects5.ArrayComboList acl = new MySpace.DataMining.DistributedObjects5.ArrayComboList("stv", 4);
                acl.atype = true;

                acl.AddBlock("64", "128", @"127.0.0.1|arraycombo_logs0.txt|slaveid=0");

                acl.Open();

                Random rnd = new Random();
                List<int> input = new List<int>(5000);
                Dictionary<int, bool> hi = new Dictionary<int, bool>(5000);

                List<byte> keybuf = new List<byte>();
                List<byte> valuebuf = new List<byte>();

                {
                    for (int i = 0; i < 5000; i++)
                    {
                        int x;
                        do
                        {
                            if (0 == (i % 300))
                            {
                                x = rnd.Next(30, 300);
                            }
                            else
                            {
                                x = rnd.Next();
                            }
                        }
                        while (hi.ContainsKey(x));
                        hi[x] = true;
                        input.Add(x);
                    }
                }

                StringBuilder blinput = new StringBuilder(@"int[] all = new int[] { ", 10000);
                for (int i = 0; i < input.Count; i++)
                {
                    if (0 != i)
                    {
                        blinput.Append(", ");
                    }
                    blinput.Append(input[i]);
                }
                blinput.Append(" };");

                acl.BeforeLoad(@"
        public virtual void OnBeforeLoad(MySpace.DataMining.DistributedObjects.LoadOutput output)
        {
            " + blinput.ToString() + @"

            List<byte> keybuf = new List<byte>(128);
            List<byte> valuebuf = new List<byte>(128);

            for (int i = 0; i < all.Length; i++)
            {
                int k = 1 + all[i] % 8;
                if(all[i] < 500)
                {
                    k += all[i];
                }
                for(int j = 0; j != k; j++)
                {
                    keybuf.Clear();
                    Entry.ToBytesAppend(all[i], keybuf);
                    valuebuf.Clear();
                    Entry.ToBytesAppend(all[j], valuebuf);
                    output.Add(ByteSlice.Create(keybuf), ByteSlice.Create(valuebuf));
                }
            }
        }");

                acl.SortBlocks();

                ArrayComboListEnumerator[] enums = acl.GetEnumeratorsWithCode(@"
                        List<byte> valuebuf = new List<byte>(128);

                        public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
                        {
                            valuebuf.Clear();
                            for(int i = 0; i < values.Length; i++)
                            {
                                for(int j = 0; j < values[i].Value.Length; j++)
                                {
                                    valuebuf.Add(values[i].Value[j]);
                                }
                            }
                            output.Add(key, ByteSlice.Create(valuebuf));
                        }
");
                int n = 0;
                foreach (ArrayComboListEnumerator en in enums)
                {
                    while (en.MoveNext())
                    {
                        if (en.Current.GetKeyLength() != 4)
                        {
                            throw new Exception("Key length mismatch");
                        }
                        if (0 != (en.Current.GetValueLength() % 4))
                        {
                            throw new Exception("Uneven value length");
                        }
                        n++;

                        keybuf.Clear();
                        en.Current.CopyKeyAppend(keybuf);
                        int ikey = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(keybuf);
                        if (!hi.ContainsKey(ikey))
                        {
                            throw new Exception("Unexpected key");
                        }
                        int k = 1 + ikey % 8;
                        if (ikey < 500)
                        {
                            k += ikey;
                        }
                        if (k * 4 != en.Current.GetValueLength())
                        {
                            throw new Exception("Value length incorrect");
                        }
                        /*
                        valuebuf.Clear();
                        en.Current.CopyValueAppend(valuebuf); // Note: not in same order as added.
                        for (int vv = 0; vv < en.Current.GetValueLength(); vv += 4)
                        {
                            int x = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(valuebuf, vv);
                        }
                         * */
                    }
                }
                if (input.Count != n)
                {
                    throw new Exception("Test failed: expected " + input.Count.ToString() + " key/values, got " + n.ToString());
                }

                if (interactive)
                {
                    Console.WriteLine("Press a key to close");
                    Console.ReadKey();
                }

                acl.Close();

                Console.WriteLine("Done");
            }
#endif


        }
    }

}

