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

        public static DbValue MAX(DbFunctionTools tools, DbAggregatorArguments args)
        {
            string AggregatorName = "MAX";

            DbValue highest = null;
            DbValue nullest = null;
            DbFunctionArguments compareargs = new DbFunctionArguments(new DbValue[2]);
            ImmediateValue argval = null;
            for (int iarg = 0; iarg < args.Length; iarg++)
            {
                args[iarg].EnsureCount(AggregatorName, 1);
                DbType arg0type;
                ByteSlice arg0 = args[iarg][0].Eval(out arg0type);
                if (Types.IsNullValue(arg0))
                {
                    if (null == nullest)
                    {
                        nullest = tools.AllocValue(arg0, arg0type);
                    }
                }
                else
                {
                    if (null == argval)
                    {
                        argval = tools.AllocValue(arg0type.ID);
                    }
                    argval.SetValue(arg0);
                    compareargs[0] = argval;
                    compareargs[1] = highest;
                    if (null == highest || tools.GetInt(DbFunctions.COMPARE(tools, compareargs)) > 0)
                    {
                        highest = argval; // Keep this one.
                        argval = null; // New one next time.
                    }
                }
            }
            if (null == highest)
            {
                if (null == nullest)
                {
                    return tools.AllocNullValue();
                }
                return nullest;
            }
            return highest;

        }

    }

}

