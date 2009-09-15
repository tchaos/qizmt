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
        public static DbValue BIT_OR(DbFunctionTools tools, DbAggregatorArguments args)
        {
            return _BITWISE("BIT_OR", tools, args, 2);
        }
    }
}

