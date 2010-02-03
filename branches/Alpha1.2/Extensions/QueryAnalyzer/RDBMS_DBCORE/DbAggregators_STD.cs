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
        private static double _STD(string aggregatorName, DbFunctionTools tools, DbAggregatorArguments args, bool sample)
        {
            return Math.Sqrt(_VAR(aggregatorName, tools, args, sample));
        }

        public static DbValue STD(DbFunctionTools tools, DbAggregatorArguments args)
        {
            return tools.AllocValue(_STD("STD", tools, args, false));
        }
    }
}

