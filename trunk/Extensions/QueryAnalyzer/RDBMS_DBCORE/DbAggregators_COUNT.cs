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
        public static DbValue COUNT(DbFunctionTools tools, DbAggregatorArguments args)
        {
            string AggregatorName = "COUNT";

            long count = 0;
            for (int iarg = 0; iarg < args.Length; iarg++)
            {
                args[iarg].EnsureCount(AggregatorName, 1);
                DbType arg0type;
                ByteSlice arg0 = args[iarg][0].Eval(out arg0type);
                if (Types.IsNullValue(arg0))    //ignore null
                {
                    continue;
                }
                count++;
            }
            return tools.AllocValue(count);
        }
    }
}

