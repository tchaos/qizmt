using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    // Important: only call Eval* ONCE per DbValue, or it will re-evaluate.
    public partial class DbAggregators
    {        
        private static byte[] bitopbuf = null;
        private static DbValue _BITWISE(string aggregatorName, DbFunctionTools tools, DbAggregatorArguments args, int whatop)
        {
            if (args.Length == 0)
            {
                return tools.AllocNullValue();
            }

            DbType arg0type = DbType.PrepareNull();

            for (int iarg = 0; iarg < args.Length; iarg++)
            {
                args[iarg].EnsureCount(aggregatorName, 1);
                ByteSlice arg0 = args[iarg][0].Eval(out arg0type);
                if (Types.IsNullValue(arg0))
                {
                    return tools.AllocNullValue();
                }

                if (bitopbuf == null || bitopbuf.Length != arg0type.Size)
                {
                    bitopbuf = new byte[arg0type.Size];                   
                }

                if (iarg == 0)
                {
                    for (int ib = 0; ib < arg0.Length; ib++)
                    {
                        bitopbuf[ib] = arg0[ib];
                    }
                    continue;
                }

                for (int ib = 1; ib < arg0.Length; ib++)
                {
                    switch (whatop)
                    {
                        case 1:
                            bitopbuf[ib] = (byte)(bitopbuf[ib] & arg0[ib]);
                            break;
                        case 2:
                            bitopbuf[ib] = (byte)(bitopbuf[ib] | arg0[ib]);
                            break;
                        default:
                            bitopbuf[ib] = (byte)(bitopbuf[ib] ^ arg0[ib]);
                            break;
                    }
                }
            }
            ByteSlice bs = ByteSlice.Prepare(bitopbuf);
            return tools.AllocValue(bs, arg0type);
        }

        public static DbValue BIT_AND(DbFunctionTools tools, DbAggregatorArguments args)
        {
            return _BITWISE("BIT_AND", tools, args, 1);
        }
    }
}

