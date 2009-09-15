using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{

    // Important: only call Eval* ONCE per DbValue, or it will re-evaluate.
    public partial class DbFunctions
    {

        public static DbValue PI(DbFunctionTools tools, DbFunctionArguments args)
        {
            double x = Math.PI;
            return tools.AllocValue(x);
        }
    }
}

