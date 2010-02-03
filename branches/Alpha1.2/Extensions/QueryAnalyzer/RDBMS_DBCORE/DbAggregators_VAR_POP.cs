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
        private static double _VAR(string aggregatorName, DbFunctionTools tools, DbAggregatorArguments args, bool sample)
        {
            List<double> values = new List<double>();
            double x = 0;
            double sum = 0;
            for (int iarg = 0; iarg < args.Length; iarg++)
            {
                args[iarg].EnsureCount(aggregatorName, 1);
                DbType arg0type;
                ByteSlice arg0 = args[iarg][0].Eval(out arg0type);
                if (!Types.IsNullValue(arg0))   //ignore null
                {
                    switch (arg0type.ID)
                    {
                        case DbTypeID.INT:
                            x = (double)tools.GetInt(arg0);
                            break;

                        case DbTypeID.LONG:
                            x = (double)tools.GetLong(arg0);
                            break;

                        case DbTypeID.DOUBLE:
                            x = tools.GetDouble(arg0);
                            break;

                        default:
                            args[iarg].Expected(aggregatorName, 0, "input INT, LONG or DOUBLE, got " + arg0type.Name.ToUpper());
                            return 0; // Doesn't reach here.
                    }
                    values.Add(x);
                    sum += x;
                }
            }
            double dev = 0;
            if (values.Count > 0)
            {
                double avg = sum / (double)values.Count;
                for (int i = 0; i < values.Count; i++)
                {
                    dev += Math.Pow(values[i] - avg, 2);
                }
                dev = dev / (double)(sample ? values.Count - 1 : values.Count);
            }
            return dev;
        }

        public static DbValue VAR_POP(DbFunctionTools tools, DbAggregatorArguments args)
        {
            return tools.AllocValue(_VAR("VAR_POP", tools, args, false));
        }
    }
}

