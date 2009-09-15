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
        public static DbValue DATEADD(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "DATEADD";

            args.EnsureCount(FunctionName, 3);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            DbType arg2type;
            ByteSlice arg2 = args[2].Eval(out arg2type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg0type.Name.ToUpper());
            }
            if (Types.IsNullValue(arg1))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg1type.ID != DbTypeID.INT)
            {
                args.Expected(FunctionName, 0, "input INT, got " + arg1type.Name.ToUpper());
            }
            if (Types.IsNullValue(arg2))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg2type.ID != DbTypeID.DATETIME)
            {
                args.Expected(FunctionName, 0, "input DATETIME, got " + arg2type.Name.ToUpper());
            }

            string datepart = tools.GetString(arg0).ToUpperM().ToString();
            int num = tools.GetInt(arg1);
            DateTime dt = tools.GetDateTime(arg2);
            DateTime newdt = dt;

            switch (datepart)
            {
                case "YEAR":
                case "YY":
                case "YYYY":                    
                    newdt = dt.AddYears(num);
                    break;
                    
                case "QUARTER":
                case "QQ":
                case "Q":
                    newdt = dt.AddMonths(3);
                    break;

                case "MONTH":
                case "MM":
                case "M":
                    newdt = dt.AddMonths(num);
                    break;

                case "DAY":
                case "DD":
                case "D":
                    newdt = dt.AddDays(num);
                    break;

                case "WEEK":
                case "WK":
                case "WW":
                    newdt = dt.AddDays(7 * num);
                    break;

                case "HOUR":
                case "HH":
                    newdt = dt.AddHours(num);
                    break;

                case "MINUTE":
                case "MI":
                case "N":
                    newdt = dt.AddMinutes(num);
                    break;

                case "SECOND":
                case "SS":
                case "S":
                    newdt = dt.AddSeconds(num);
                    break;

                case "MILLISECOND":
                case "MS":
                    newdt = dt.AddMilliseconds(num);
                    break;

                default:
                    args.Expected(FunctionName, 0, "input datepart invalid");
                    return null;
            }

            return tools.AllocValue(newdt);
        }
    }
}