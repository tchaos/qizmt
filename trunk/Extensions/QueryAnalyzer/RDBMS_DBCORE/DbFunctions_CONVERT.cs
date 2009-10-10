using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    /*
    // Important: only call Eval* ONCE per DbValue, or it will re-evaluate.
    public partial class DbFunctions
    {
        public static DbValue CONVERT(DbFunctionTools tools, DbFunctionArguments args)
        {
            return null;
            string FunctionName = "CONVERT";

            args.EnsureCount(FunctionName, 2);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
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
                //args.Expected(FunctionName, 0, "input INT, got " + arg1type.Name.ToUpper());
            }

            int charsize = 0;
            string targetdatatype = "";
            {
                mstring dt = tools.GetString(arg0).ToLowerM();
                if (dt.StartsWith("char("))
                {
                    mstring ssize = dt.SubstringM(5, dt.Length - 5 - 1);
                    charsize = ssize.ToInt();
                    targetdatatype = "char";
                }
                else
                {
                    targetdatatype = dt.ToString();
                }
            }
            





        }
    }*/
}