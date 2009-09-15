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

        public static DbValue LAST(DbFunctionTools tools, DbAggregatorArguments args)
        {
            string AggregatorName = "LAST";

            if (args.Length > 0)
            {
                int lastindex = args.Length - 1;
                args[lastindex].EnsureCount(AggregatorName, 1);
                return args[lastindex][0];
            }
            else
            {
                return tools.AllocNullValue();
            }
        }
    }
}