using System;
using System.Collections.Generic;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace MySpace.DataMining.EasyReducer
{
    public class RandomAccessReducer : IBeforeReduce
    {
        int i = 0;
        RandomAccessOutput raout = null;
        RandomAccessEntries raentries = null;

        public bool OnGetEnumerator(EntriesInput input, EntriesOutput output)
        {
            if (null == raout)
            {
                raout = new RandomAccessOutput(output);
            }
            if (null == raentries)
            {
                raentries = new RandomAccessEntries();
            }
            raentries.SetInput(input);
            byte[] firstkeybuf, xkeybuf;
            int firstkeyoffset, xkeyoffset;
            int firstkeylen, xkeylen;
            for (; i < input.entries.Count; )
            {
                input.entries[i].LocateKey(input, out firstkeybuf, out firstkeyoffset, out firstkeylen);
                int len = 1;
                for (int j = i + 1; j < input.entries.Count; j++)
                {
                    bool nomatch = false;
                    input.entries[j].LocateKey(input, out xkeybuf, out xkeyoffset, out xkeylen);
                    if (firstkeylen != xkeylen)
                    {
                        break;
                    }
                    for (int ki = 0; ki != xkeylen; ki++)
                    {
                        if (xkeybuf[xkeyoffset + ki] != firstkeybuf[firstkeyoffset + ki])
                        {
                            nomatch = true;
                            break;
                        }
                    }
                    if (nomatch)
                    {
                        break;
                    }
                    len++;
                }
                raentries.set(i, len);
                Reduce(raentries[0].Key, raentries, raout);
                i += len;
                return true; // Continue.
            }
            i = 0;
            return false; // At end; stop.
        }


        public class RandomAccessEntries
        {
            public RandomAccessEntries()
            {
            }


            internal void SetInput(EntriesInput input)
            {
                this.input = input;
            }


            internal void set(int offset, int length)
            {
                this.offset = offset;
                this.length = length;
            }


            public int Length
            {
                get
                {
                    return length;
                }
            }


            public ReduceEntry this[int index]
            {
                get
                {
                    if (index < 0 || index > length)
                    {
                        throw new ArgumentOutOfRangeException();
                    }
                    return ReduceEntry.Create(input, input.entries[offset + index]);
                }
            }


            int offset, length;
            EntriesInput input;
        }


        public class RandomAccessOutput
        {
            public RandomAccessOutput(EntriesOutput eoutput)
            {
                this.eoutput = eoutput;
            }


            public void Add(ByteSlice key, ByteSlice value)
            {
                eoutput.Add(key, value);
            }


            EntriesOutput eoutput;
        }


        public virtual void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
        {
            for (int i = 0; i < values.Length; i++)
            {
                ByteSlice value = values[i].Value;
                output.Add(key, value);
            }
        }
    }


    class MyReducer : RandomAccessReducer
    {
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

                output.Add(key, ByteSlice.Create(lbuf, 0, lbuf.Count));
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
    }

}
