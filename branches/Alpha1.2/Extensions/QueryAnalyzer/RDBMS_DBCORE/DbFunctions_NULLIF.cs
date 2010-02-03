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
        public static DbValue NULLIF(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "NULLIF";

            args.EnsureCount(FunctionName, 2);
            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);

            ImmediateValue argval = null;
            argval = tools.AllocValue(arg0type.ID);
            argval.SetValue(arg0);

            DbFunctionArguments compareargs = new DbFunctionArguments(new DbValue[2]);
            compareargs[0] = argval;
            compareargs[1] = args[1];
            DbValue result = COMPARE(tools, compareargs);
            int iresult = tools.GetInt(result);
            if (iresult == 0)
            {
                List<byte> buf = tools.AllocBuffer(arg0type.Size);
                buf.Add(1);
                for (int i = 0; i < arg0type.Size - 1; i++)                
                {
                    buf.Add(0);
                }
                return tools.AllocValue(ByteSlice.Prepare(buf), arg0type);
            }
            else
            {
                return args[0];
            }
        }
    }
}

