using System;
using System.Collections.Generic;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace UserLoader
{

    public class BeforeLoader : MySpace.DataMining.DistributedObjects.IBeforeLoad
    {
        //------------

        public virtual void OnBeforeLoad(MySpace.DataMining.DistributedObjects.LoadOutput output)
        {
            List<byte> keybuf = new List<byte>(128);
            List<byte> valuebuf = new List<byte>(128);

            for (int i = 0; i < 20; i++)
            {
                keybuf.Clear();
                Entry.ToBytesAppend(i, keybuf);
                valuebuf.Clear();
                Entry.ToBytesAppend(42, valuebuf);
                output.Add(ByteSlice.Create(keybuf), ByteSlice.Create(valuebuf));
            }
        }
        //------------
    }

}

