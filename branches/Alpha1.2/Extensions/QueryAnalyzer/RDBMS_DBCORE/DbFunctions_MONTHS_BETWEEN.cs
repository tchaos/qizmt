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
        public static DbValue MONTHS_BETWEEN(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "MONTHS_BETWEEN";

            args.EnsureCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.DATETIME)
            {
                args.Expected(FunctionName, 0, "input DATETIME, got " + arg0type.Name.ToUpper());
            }
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (Types.IsNullValue(arg1))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg1type.ID != DbTypeID.DATETIME)
            {
                args.Expected(FunctionName, 0, "input DATETIME, got " + arg1type.Name.ToUpper());
            }

            DateTime dt0 = tools.GetDateTime(arg0);
            DateTime dt1 = tools.GetDateTime(arg1);
            int daysInMonth0 = DateTime.DaysInMonth(dt0.Year, dt0.Month);
            int daysInMonth1 = DateTime.DaysInMonth(dt1.Year, dt1.Month);
            double btw = 0;

            if (dt0.Year == dt1.Year && dt0.Month == dt1.Month) //same month and same year
            {
                btw = (double)(dt0.Day - dt1.Day) / (double)daysInMonth0;
            }
            else if (dt0.Day == daysInMonth0 && dt1.Day == daysInMonth1) //both fall on the last day of their months
            {
                btw = 12 * (dt0.Year - dt1.Year) + dt0.Month - dt1.Month;
            }
            else
            {
                TimeSpan sp = dt0 - dt1;
                btw = sp.TotalDays / 31d;
            }
            
            return tools.AllocValue(btw);
        }
    }
}