using System;
using System.Collections.Generic;
using System.Text;


namespace MySpace.DataMining.DistributedObjects
{
    public class UserEnumeratorPlugin : IBeforeReduce
    {
        int i = 0;

        public bool OnGetEnumerator(EntriesInput input, EntriesOutput output)
        {
            int count = input.entries.Count;
            if (i >= count)
            {
                i = 0;
                return false; // At end; stop.
            }

            Entry entry = input.entries[i++];

            byte[] keybuf;
            int keyoffset;
            int keylength;
            entry.LocateKey(input, out keybuf, out keyoffset, out keylength);

            byte[] valuebuf;
            int valueoffset;
            int valuelength;
            entry.LocateValue(input, out valuebuf, out valueoffset, out valuelength);

            output.Add(keybuf, keyoffset, keylength, valuebuf, valueoffset, valuelength);
            return true; // Continue.
        }
    }


    // Low-level implementation of the above.
    public class UserEnumeratorPluginLL : IBeforeReduce
    {
        int i = 0;

        public bool OnGetEnumerator(EntriesInput input, EntriesOutput output)
        {
            int count = input.entries.Count;
            if (i >= count)
            {
                i = 0;
                return false; // At end; stop.
            }
            List<byte[]> net = input.net;
            List<Entry> entries = input.entries;
            int KeyLength = input.KeyLength;

            Entry entry = input.entries[i++];

            byte[] keybuf = net[entry.NetIndex];
            int keyoffset = entry.NetEntryOffset;
            int keylength = KeyLength; // Direct from input.

            byte[] valuebuf = net[entry.NetIndex];
            int valueoffset = entry.NetEntryOffset + KeyLength + 4; // Internal storage layout.
            int valuelength = Entry.BytesToInt(valuebuf, entry.NetEntryOffset + KeyLength); // Internal storage layout.

            output.Add(keybuf, keyoffset, keylength, valuebuf, valueoffset, valuelength);
            return true; // Continue.
        }
    }

}
