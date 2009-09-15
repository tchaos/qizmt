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

        public static DbValue AVG(DbFunctionTools tools, DbAggregatorArguments args)
        {
            string AggregatorName = "AVG";

            double sum = 0;
            int count = 0;
            for (int iarg = 0; iarg < args.Length; iarg++)
            {
                args[iarg].EnsureCount(AggregatorName, 1);
                DbType arg0type;
                ByteSlice arg0 = args[iarg][0].Eval(out arg0type);
                if (!Types.IsNullValue(arg0))   //ignore null
                {
                    count++;
                    switch (arg0type.ID)
                    {
                        case DbTypeID.INT:
                            sum += (double)tools.GetInt(arg0);
                            break;

                        case DbTypeID.LONG:
                            sum += (double)tools.GetLong(arg0);
                            break;

                        case DbTypeID.DOUBLE:
                            sum += tools.GetDouble(arg0);
                            break;

                        default:
                            args[iarg].Expected(AggregatorName, 0, "input INT, LONG or DOUBLE, got " + arg0type.Name.ToUpper());
                            return null; // Doesn't reach here.
                    }
                }
            }
            double avg = 0;
            if (count > 0)
            {
                avg = sum / (double)count;
            }
            return tools.AllocValue(avg);   
        }
    }
}

