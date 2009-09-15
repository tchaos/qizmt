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

        public static DbValue SPACE(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "SPACE";

            args.EnsureCount(FunctionName, 1);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            if (Types.IsNullValue(arg0))
            {
                return tools.AllocNullValue(); // Give a null, take a null.
            }
            if (arg0type.ID != DbTypeID.INT)
            {
                args.Expected(FunctionName, 0, "input INT, got " + arg0type.Name.ToUpper());
                return null;
            }

            int len = tools.GetInt(arg0);
            if (len < 1)
            {
                return tools.AllocValue(mstring.Prepare(""));
            }
            else
            {
                mstring s = mstring.Prepare();

                for (int i = 0; i < len; i++)
                {
                    s = s.AppendM(" ");
                }

                return tools.AllocValue(s);
            }            
        }
    }
}

