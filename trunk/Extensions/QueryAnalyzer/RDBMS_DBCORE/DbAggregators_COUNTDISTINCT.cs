using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{

    // Important: only call Eval* ONCE per DbValue, or it will re-evaluate.
    public partial class DbAggregators
    {
        private class ByteSliceEqualityComparer : IEqualityComparer<ByteSlice>
        {
            bool IEqualityComparer<ByteSlice>.Equals(ByteSlice x, ByteSlice y)
            {
                if (x.Length != y.Length)
                {
                    return false;
                }

                for (int i = 0; i < x.Length; i++)
                {
                    if (x[i] != y[i])
                    {
                        return false;
                    }
                }
                return true;
            }

            int IEqualityComparer<ByteSlice>.GetHashCode(ByteSlice obj)
            {
                unchecked
                {
                    int result = 0;
                    int len = obj.Length;
                    for (int i = 0; i < len; i++)
                    {
                        result += obj[i];
                    }
                    return result;
                }
            }
        }

        public static DbValue COUNTDISTINCT(DbFunctionTools tools, DbAggregatorArguments args)
        {
            string AggregatorName = "COUNTDISTINCT";

            Dictionary<ByteSlice, short> dict = new Dictionary<ByteSlice, short>(new ByteSliceEqualityComparer());

            for (int iarg = 0; iarg < args.Length; iarg++)
            {
                args[iarg].EnsureCount(AggregatorName, 1);
                DbType arg0type;
                ByteSlice arg0 = args[iarg][0].Eval(out arg0type);
                if (Types.IsNullValue(arg0))    //ignore null
                {
                    continue;
                }

                if (!dict.ContainsKey(arg0))
                {
                    dict.Add(arg0, 0);
                }               
            }
            return tools.AllocValue((long)dict.Count);
        }
    }
}

