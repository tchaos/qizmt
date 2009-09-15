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

        public static DbValue SIGN(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "SIGN";

            args.EnsureCount(FunctionName, 1);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }

            int sign = 0;
            switch (arg0type.ID)
            {
                case DbTypeID.INT:
                    {
                        int x = tools.GetInt(arg0);                       
                        if (x > 0)
                        {
                            sign = 1;
                        }
                        else if (x < 0)
                        {
                            sign = -1;
                        }
                    }
                    break;

                case DbTypeID.LONG:
                    {
                        long x = tools.GetLong(arg0);
                        if (x > 0)
                        {
                            sign = 1;
                        }
                        else if (x < 0)
                        {
                            sign = -1;
                        }
                    }
                    break;

                case DbTypeID.DOUBLE:
                    {
                        double x = tools.GetDouble(arg0);
                        if (x > 0)
                        {
                            sign = 1;
                        }
                        else if (x < 0)
                        {
                            sign = -1;
                        }
                    }
                    break;

                default:
                    args.Expected(FunctionName, 0, "input INT, LONG or DOUBLE, got " + arg0type.Name.ToUpper());
                    return null; // Doesn't reach here.
            }
            return tools.AllocValue(sign);
        }
    }
}

