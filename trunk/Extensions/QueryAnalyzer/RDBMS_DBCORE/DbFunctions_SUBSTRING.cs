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

        public static DbValue SUBSTRING(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "SUBSTRING";

            args.EnsureCount(FunctionName, 3);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.CHARS)
            {
                args.Expected(FunctionName, 0, "input CHAR(n), got " + arg0type.Name.ToUpper());
                return null;
            }

            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            if (arg1type.ID != DbTypeID.INT)
            {
                args.Expected(FunctionName, 1, "count INT, got " + arg1type.Name.ToUpper());
                return null;
            }

            DbType arg2type;
            ByteSlice arg2 = args[2].Eval(out arg2type);
            if (arg2type.ID != DbTypeID.INT)
            {
                args.Expected(FunctionName, 1, "count INT, got " + arg2type.Name.ToUpper());
                return null;
            }

            mstring x = tools.GetString(arg0);
            int startIndex = tools.GetInt(arg1);
            if (startIndex < 0)
            {
                startIndex = 0;
            }
            int len = tools.GetInt(arg2);
            if (len < 0)
            {
                throw new ArgumentException(FunctionName + " length cannot be negative");
            }

            if (startIndex + len > x.Length)
            {
                return tools.AllocValue(mstring.Prepare());
            }
            else
            {
                mstring sub = x.SubstringM(startIndex, len);
                return tools.AllocValue(sub);
            }
        }
    }
}

