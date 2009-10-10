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
        public static DbValue DATEDIFF(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "DATEDIFF";

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
            if (arg1type.ID != DbTypeID.DATETIME)
            {
                args.Expected(FunctionName, 0, "input DATETIME, got " + arg1type.Name.ToUpper());
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
            DateTime startdate = tools.GetDateTime(arg1);            
            DateTime enddate = tools.GetDateTime(arg2);        
            double partdiff = 0;

            switch (datepart)
            {
                case "YEAR":
                case "YY":
                case "YYYY":
                    partdiff = enddate.Year - startdate.Year;
                    break;

                case "QUARTER":
                case "QQ":
                case "Q":                    
                    partdiff = GetMonthDiff(startdate.Year, startdate.Month, enddate.Year, enddate.Month) / 3;                                        
                    break;

                case "MONTH":
                case "MM":
                case "M":
                    partdiff = GetMonthDiff(startdate.Year, startdate.Month, enddate.Year, enddate.Month);
                    break;

                case "DAY":
                case "DD":
                case "D":
                    {
                        DateTime sdate = new DateTime(startdate.Year, startdate.Month, startdate.Day);
                        DateTime edate = new DateTime(enddate.Year, enddate.Month, enddate.Day);
                        TimeSpan sp = edate - sdate;
                        partdiff = sp.TotalDays;
                    }                    
                    break;

                case "WEEK":
                case "WK":
                case "WW":
                    {
                        DateTime sdate = new DateTime(startdate.Year, startdate.Month, startdate.Day);
                        DateTime edate = new DateTime(enddate.Year, enddate.Month, enddate.Day);
                        TimeSpan sp = edate - sdate;
                        partdiff = (int)sp.TotalDays / 7;
                    }     
                    break;

                case "HOUR":
                case "HH":
                    {
                        DateTime sdate = new DateTime(startdate.Year, startdate.Month, startdate.Day, startdate.Hour, 0, 0);
                        DateTime edate = new DateTime(enddate.Year, enddate.Month, enddate.Day, enddate.Hour, 0, 0);
                        TimeSpan sp = edate - sdate;
                        partdiff = sp.TotalHours;
                    }     
                    break;

                case "MINUTE":
                case "MI":
                case "N":
                    {
                        DateTime sdate = new DateTime(startdate.Year, startdate.Month, startdate.Day, startdate.Hour, startdate.Minute, 0);
                        DateTime edate = new DateTime(enddate.Year, enddate.Month, enddate.Day, enddate.Hour, enddate.Minute, 0);
                        TimeSpan sp = edate - sdate;
                        partdiff = sp.TotalMinutes;
                    }     
                    break;

                case "SECOND":
                case "SS":
                case "S":
                    {
                        DateTime sdate = new DateTime(startdate.Year, startdate.Month, startdate.Day, startdate.Hour, startdate.Minute, startdate.Second);
                        DateTime edate = new DateTime(enddate.Year, enddate.Month, enddate.Day, enddate.Hour, enddate.Minute, enddate.Second);
                        TimeSpan sp = edate - sdate;
                        partdiff = sp.TotalSeconds;
                    }  
                    break;

                case "MILLISECOND":
                case "MS":
                    {
                        TimeSpan sp = enddate - startdate;
                        partdiff = sp.TotalMilliseconds;
                    }                    
                    break;

                default:
                    args.Expected(FunctionName, 0, "input datepart invalid");
                    return null;
            }

            return tools.AllocValue(partdiff);
        }

        private static int GetMonthDiff(int startyear, int startmonth, int endyear, int endmonth)
        {
            return (endyear - startyear) * 12 - startmonth + endmonth;
        }        
    }
}