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
        public static DbValue VAR_SAMP(DbFunctionTools tools, DbAggregatorArguments args)
        {
            return tools.AllocValue(_VAR("VAR_SAMP", tools, args, true));
        }
    }
}

