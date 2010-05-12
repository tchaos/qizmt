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

        public static DbValue IIF(DbFunctionTools tools, DbFunctionArguments args)
        {
            string FunctionName = "IIF";

            args.EnsureCount(FunctionName, 3);

            DbType arg0type;
            ByteSlice arg0 = args[0].Eval(out arg0type);
            DbType arg1type;
            ByteSlice arg1 = args[1].Eval(out arg1type);
            DbType arg2type;
            ByteSlice arg2 = args[2].Eval(out arg2type);

            bool truecond;
            if (Types.IsNullValue(arg0))
            {
                truecond = false;
            }
            else if (DbTypeID.INT == arg0type.ID)
            {
                truecond = 0 != tools.GetInt(arg0);
            }
            else if (DbTypeID.LONG == arg0type.ID)
            {
                truecond = 0 != tools.GetLong(arg0);
            }
            else if (DbTypeID.DOUBLE == arg0type.ID)
            {
                truecond = 0 != tools.GetDouble(arg0);
            }
            else
            {
                bool allzero = true;
                for (int i = 1; i < arg0.Length; i++)
                {
                    if (arg0[i] != 0)
                    {
                        allzero = false;
                        break;
                    }
                }
                truecond = !allzero;
            }

            byte[] resultbuf = new byte[arg1.Length > arg2.Length ? arg1.Length : arg2.Length];
            DbType resulttype;
            if (truecond)
            {
                resulttype = DbType.Prepare(resultbuf.Length, arg1type.ID);
                for (int i = 0; i < arg1.Length; i++)
                {
                    resultbuf[i] = arg1[i];
                }
            }
            else
            {
                resulttype = DbType.Prepare(resultbuf.Length, arg2type.ID);
                for (int i = 0; i < arg2.Length; i++)
                {
                    resultbuf[i] = arg2[i];
                }
            }

            return tools.AllocValue(ByteSlice.Prepare(resultbuf), resulttype);

        }

    }

}

