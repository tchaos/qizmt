using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RDBMS_DBCORE;
using MySpace.DataMining.DistributedObjects;

namespace RDBMS_DBCORE_RegressionTests
{
    public partial class Program
    {
        public static void DbFunctions_CAST()
        {
            DbFunctionTools tools = new DbFunctionTools();

            DateTime dt = DateTime.Now;
            DbValue[] conv = new DbValue[]
                {
                    tools.AllocValue((int)-372), tools.AllocValue((int)-372),
                    tools.AllocValue((int)-372), tools.AllocValue((long)-372),
                    tools.AllocValue((int)-372), tools.AllocValue((double)-372),
                    //tools.AllocValue((int)dt.Ticks), tools.AllocValue(new DateTime((int)dt.Ticks)),
                    tools.AllocValue((int)-372), tools.AllocValue(mstring.Prepare("-372")),
                    tools.AllocValue((int)-372), tools.AllocValue(mstring.Prepare("-372"), 51),

                    tools.AllocValue((long)-372), tools.AllocValue((int)-372),
                    tools.AllocValue((long)-372), tools.AllocValue((long)-372),
                    tools.AllocValue((long)-372), tools.AllocValue((double)-372),
                    tools.AllocValue((long)dt.Ticks), tools.AllocValue(new DateTime((long)dt.Ticks)),
                    tools.AllocValue((long)-372), tools.AllocValue(mstring.Prepare("-372")),
                    tools.AllocValue((long)-372), tools.AllocValue(mstring.Prepare("-372"), 51),

                    tools.AllocValue((double)-372), tools.AllocValue((int)-372),
                    tools.AllocValue((double)-372), tools.AllocValue((long)-372),
                    tools.AllocValue((double)-372), tools.AllocValue((double)-372),
                    tools.AllocValue((double)dt.Ticks), tools.AllocValue(new DateTime((long)(double)dt.Ticks)),
                    tools.AllocValue((double)-372), tools.AllocValue(mstring.Prepare("-372")),
                    tools.AllocValue((double)-372), tools.AllocValue(mstring.Prepare("-372"), 51),
                    // Extra double ones:
                    tools.AllocValue((double)101.1), tools.AllocValue((int)101),
                    tools.AllocValue((double)101.1), tools.AllocValue((long)101),
                    tools.AllocValue((double)101.1), tools.AllocValue(mstring.Prepare((double)101.1)),
                    tools.AllocValue((double)(22.0/7.0)), tools.AllocValue(mstring.Prepare((double)(22.0/7.0))),

                    tools.AllocValue(dt), tools.AllocValue((int)dt.Ticks),
                    tools.AllocValue(dt), tools.AllocValue((long)dt.Ticks),
                    tools.AllocValue(dt), tools.AllocValue((double)dt.Ticks),
                    tools.AllocValue(dt), tools.AllocValue(dt),
                    tools.AllocValue(dt), tools.AllocValue(mstring.Prepare(dt.ToString())),
                    tools.AllocValue(dt), tools.AllocValue(mstring.Prepare(dt.ToString()), 51),

                    tools.AllocValue(mstring.Prepare("-372")), tools.AllocValue((int)-372),
                    tools.AllocValue(mstring.Prepare("-372")), tools.AllocValue((long)-372),
                    tools.AllocValue(mstring.Prepare("-372")), tools.AllocValue((double)-372),
                    tools.AllocValue(mstring.Prepare(dt.ToString())), tools.AllocValue(dt),
                    tools.AllocValue(mstring.Prepare("-372")), tools.AllocValue(mstring.Prepare("-372")),
                    tools.AllocValue(mstring.Prepare("-372")), tools.AllocValue(mstring.Prepare("-372"), 51),
                    // Extra string ones:
                    tools.AllocValue(mstring.Prepare("-372"), 51), tools.AllocValue(mstring.Prepare("-372"), 101),
                    tools.AllocValue(mstring.Prepare("-372"), 101), tools.AllocValue(mstring.Prepare("-372"), 51),

                    null
                };


            for (int ic = 0; ic + 2 <= conv.Length; ic += 2)
            {
                DbValue a = conv[ic + 0];
                DbType atype;
                ByteSlice abytes = a.Eval(out atype);
                
                DbValue b = conv[ic + 1];
                DbType btype;
                ByteSlice bbytes = b.Eval(out btype);

                Console.WriteLine("Testing DbFunctions.CAST({0} AS {1})...", atype.Name, btype.Name);
                
                List<DbValue> args = new List<DbValue>();
                args.Add(a);
                args.Add(tools.AllocValue(mstring.Prepare("AS " + btype.Name)));
                DbFunctionArguments fargs = new DbFunctionArguments(args);

                DbValue r = DbFunctions.CAST(tools, fargs);
                DbType rtype;
                ByteSlice rbytes = r.Eval(out rtype);

                if (rtype.ID != btype.ID)
                {
                    throw new Exception(string.Format("DbFunctions.CAST({0} AS {1}) resulted in type {2}: result has unexpected ID of {3}",
                        atype.Name, btype.Name, btype.Name, rtype.ID));
                }
                if (rtype.Size != btype.Size)
                {
                    throw new Exception(string.Format("DbFunctions.CAST({0} AS {1}) resulted in type {2}: result has unexpected size of {3}",
                        atype.Name, btype.Name, btype.Name, rtype.Size));
                }
                if (rtype.ID == DbTypeID.DATETIME)
                {
                    if (tools.GetDateTime(rbytes).ToString() != tools.GetDateTime(bbytes).ToString())
                    {
                        throw new Exception(string.Format("DbFunctions.CAST({0} AS {1}) resulted in type {2}: result has unexpected value",
                            atype.Name, btype.Name, btype.Name));
                    }
                }
                else
                {
                    for (int ix = 0; ix < rtype.Size; ix++)
                    {
                        if (rbytes[ix] != bbytes[ix])
                        {
                            throw new Exception(string.Format("DbFunctions.CAST({0} AS {1}) resulted in type {2}: result has unexpected value",
                                atype.Name, btype.Name, btype.Name));
                        }
                    }
                }

            }


        }
    }
}
