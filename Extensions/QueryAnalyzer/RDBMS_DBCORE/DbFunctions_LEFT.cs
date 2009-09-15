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

        public static DbValue LEFT(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "LEFT";

            args.EnsureCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg0type.Name.ToUpper());
            }
            
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (arg1type.ID != DbTypeID.INT)
            {
                args.Expected(FunctionName, 1, "count INT, got " + arg1type.Name.ToUpper());
            }

            mstring x = tools.GetString(arg0);
            int LeftCount = tools.GetInt(arg1);
            if (LeftCount >= x.Length)
            {
                // User requested the whole string.
                return tools.AllocValue(arg0, arg0type);
            }
            else if (LeftCount < 0)
            {
                throw new ArgumentException(FunctionName + " count cannot be negative");
            }
            else
            {
                x = x.SubstringM(0, LeftCount);
                return tools.AllocValue(x);
            }

        }

    }

}

