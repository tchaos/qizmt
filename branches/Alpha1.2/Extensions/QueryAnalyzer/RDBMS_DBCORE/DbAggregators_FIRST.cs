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

        public static DbValue FIRST(DbFunctionTools tools, DbAggregatorArguments args)
        {
            string AggregatorName = "FIRST";

            if (args.Length > 0)
            {
                args[0].EnsureCount(AggregatorName, 1);
                return args[0][0];
            }
            else
            {
                return tools.AllocNullValue();
            }
        }
    }
}