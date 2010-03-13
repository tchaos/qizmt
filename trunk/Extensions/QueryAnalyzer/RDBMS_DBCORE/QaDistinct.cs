using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        public class DistinctMap : MapReduce
        {

            public override void Map(ByteSlice line, MapOutput output)
            {
                output.Add(line, ByteSlice.Prepare());
            }

        }

        public class DistinctReduce : MapReduce
        {

            public override void Reduce(ByteSlice key, IEnumerator<ByteSlice> values, MapReduceOutput output)
            {
                output.Add(key);
            }

        }

    }

}