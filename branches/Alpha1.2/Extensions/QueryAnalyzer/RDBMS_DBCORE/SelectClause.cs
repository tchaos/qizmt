using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{

    public class SelectClause: IValueContext
    {

        public SelectClause(DbFunctionTools tools, IList<DbColumn> cols)
        {
            this.functools = tools;
            this.cols = cols;
        }


        CallInfo _GetSelectPartCalls(ref string calls)
        {
            CallInfo ci = new CallInfo();
            {
                string colpart = Qa.NextPart(ref calls);
                if (0 == colpart.Length)
                {
                    throw new Exception("Expected value");
                }
                if ("-" == colpart || "+" == colpart)
                {
                    colpart += Qa.NextPart(ref calls);
                }
                string s;
                s = Qa.NextPart(ref calls);
                if ("(" == s)
                {
                    List<CallInfoArg> args = new List<CallInfoArg>(4);
                    {
                        string xcalls = calls;
                        if (")" == Qa.NextPart(ref xcalls))
                        {
                            calls = xcalls;
                            ci.func = colpart;
                            ci.args = args;
                            return ci;
                        }
                    }
                    for (; ; )
                    {
                        CallInfo nestci = _GetSelectPartCalls(ref calls);
                        CallInfoArg arg = new CallInfoArg();
                        if (nestci.func == null)
                        {
#if DEBUG
                            if (1 != nestci.args.Count)
                            {
                                throw new Exception("DEBUG:  (1 != nestci.args.Count)");
                            }
#endif
                            arg = nestci.args[0];
                        }
                        else
                        {
                            arg.nest = nestci;
                        }
                        args.Add(arg);
                        s = Qa.NextPart(ref calls);
                        if (0 == string.Compare("AS", s, true))
                        {
                            arg = new CallInfoArg();
                            {
                                StringBuilder sbas = new StringBuilder();
                                int asparens = 0;
                                for (; ; )
                                {
                                    s = Qa.NextPart(ref calls);
                                    if (0 == s.Length || "," == s)
                                    {
                                        //calls += s;
                                        break;
                                    }
                                    else if ("(" == s)
                                    {
                                        asparens++;
                                        sbas.Append(s);
                                    }
                                    else if (")" == s)
                                    {
                                        if (0 == asparens)
                                        {
                                            //calls += s;
                                            break;
                                        }
                                        asparens--;
                                        sbas.Append(s);
                                    }
                                    else
                                    {
                                        sbas.Append(s);
                                    }
                                }
                                if (0 == sbas.Length)
                                {
                                    throw new Exception("Expected type after AS");
                                }
                                {
                                    DbValue iterval = functools.AllocValue(mstring.Prepare("AS " + sbas.ToString()));
                                    // Need to copy the value out of the functools memory
                                    // so that it survives this map/reduce iteration...
                                    DbType xtype;
                                    ByteSlice iterbs = iterval.Eval(out xtype);
                                    List<byte> newbuf = new List<byte>(iterbs.Length);
                                    iterbs.AppendTo(newbuf);
                                    arg.value = new ImmediateValue(null, ByteSlice.Prepare(newbuf), xtype);
                                }
                                args.Add(arg);
                            }
                            //s = Qa.NextPart(ref calls);
                        }
                        if (s == ",")
                        {
                            continue;
                        }
                        if (s == ")")
                        {
                            string xnc = calls;
                            s = Qa.NextPart(ref xnc);
                            if (0 == s.Length || "," == s || ")" == s)
                            {
                                ci.func = colpart;
                                ci.args = args;
                                return ci;
                            }
                            else
                            {
                                throw new Exception("Unexpected: " + s);
                            }
                            break;
                        }
                        else
                        {
                            if (s.Length != 0)
                            {
                                throw new Exception("Unexpected: " + s);
                            }
                            throw new Exception("Unexpected end of select clause");
                        }
                    }
                    // Doesn't reach here.
                }
                else
                {
                    //if (0 == s.Length || "," == s || ")" == s)
                    {
                        calls = s + " " + calls; // Undo.
                        //ci.func = null;
                        CallInfoArg arg;
                        arg = new CallInfoArg();
                        //arg.s = colpart;
                        if (colpart.Length > 0
                            && (char.IsLetter(colpart[0]) || '_' == colpart[0]))
                        {
                            int icol = DbColumn.IndexOf(cols, colpart);
                            if (-1 == icol)
                            {
                                throw new Exception("No such column named " + colpart);
                            }
                            arg.value = new ColValue(this, cols[icol]);
                        }
                        else
                        {
                            DbTypeID typeid;
                            ByteSlice bs = Types.LiteralToValue(colpart, out typeid);
                            arg.value = new ImmediateValue(null, bs, DbType.Prepare(bs.Length, typeid));
                        }
                        ci.args = new CallInfoArg[1];
                        ci.args[0] = arg;
                        return ci;
                    }
                    /*else
                    {
                        throw new Exception("Unexpected: " + s);
                    }*/
                }
            }
        }


        // Converts the CallInfoArgs of a CallInfo into arguments for DbExec.
        // frowsIndex is not preserved to the caller!
        List<DbFunctionArguments> _CallArgsToExecArgs(CallInfo ci)
        {
            List<List<DbValue>> fvargs = _NextCallList();
            int frowsCount = frows.Count;
            for (int irow = 0; irow < frowsCount; irow++)
            {
                fvargs.Add(functools.AllocDbValueList());
            }
            int argsCount = ci.args.Count;
            for (int iarg = 0; iarg < argsCount; iarg++)
            {
                if (null != ci.args[iarg].value)
                {
                    DbValue favalue = ci.args[iarg].value;
                    for (this.frowsIndex = 0; this.frowsIndex < frowsCount; this.frowsIndex++)
                    {
                        // Evaluate favalue now that this.frowsIndex (IValueContext) is updated.
                        // Field evaluation...
                        DbType etype;
                        ByteSlice ebs = favalue.Eval(out etype);
                        DbValue evalue = functools.AllocValue(ebs, etype);

                        fvargs[this.frowsIndex].Add(evalue);
                    }
                }
                else //if (null == args[iarg].value)
                {
                    CallInfo nestci = ci.args[iarg].nest;
                    List<DbValue> nestresults;
                    {
                        int save_frowsIndex = this.frowsIndex;
                        nestresults = _ProcessSelectCallInfo(nestci);
                        this.frowsIndex = save_frowsIndex;
                    }
                    int nestresultsCount = nestresults.Count;
                    if (1 == nestresultsCount)
                    {
                        DbValue favalue = nestresults[0];
                        for (int irow = 0; irow < frowsCount; irow++)
                        {
                            List<DbValue> dbvaluelist = fvargs[irow];
                            fvargs[irow].Add(favalue);
                            fvargs[irow] = dbvaluelist;
                        }
                    }
                    else if (frowsCount == nestresultsCount)
                    {
                        for (int irow = 0; irow < frowsCount; irow++)
                        {
                            List<DbValue> dbvaluelist = fvargs[irow];
                            fvargs[irow].Add(nestresults[irow]);
                            fvargs[irow] = dbvaluelist;
                        }
                    }
                    else
                    {
                        throw new Exception("Unexpected number of values returned; on a group of " + frowsCount.ToString() + " rows, function returned " + nestresultsCount.ToString() + " values");
                    }

                }
            }
            List<DbFunctionArguments> fargs = _NextCallArgs();
            for (int irow = 0; irow < frowsCount; irow++)
            {
                DbFunctionArguments fa = new DbFunctionArguments(fvargs[irow]);
                fargs.Add(fa);
            }
            return fargs;
        }

        // frowsIndex is not preserved to the caller!
        List<DbValue> _ProcessSelectCallInfo(CallInfo ci)
        {
            int frowsTotal = frows.Count;
            List<DbValue> results = functools.AllocDbValueList();
            bool IsAggregator = (null != ci.func && DbExec.DbAggregatorExists(ci.func));
            List<DbFunctionArguments> fargs = _CallArgsToExecArgs(ci);
            if (IsAggregator)
            {
                DbValue value = DbExec.ExecDbAggregator(ci.func, functools, new DbAggregatorArguments(fargs));
                results.Add(value);
            }
            else
            {
                int fargsCount = fargs.Count;
                if (null != ci.func)
                {
                    for (int ifarg = 0; ifarg < fargsCount; ifarg++)
                    {
                        DbValue value = DbExec.ExecDbScalarFunction(ci.func, functools, fargs[ifarg]);
                        results.Add(value);
                    }
                }
                else //if (null == ci.func)
                {
                    for (int ifarg = 0; ifarg < fargsCount; ifarg++)
                    {
#if DEBUG
                        if (1 != fargs[ifarg].Length)
                        {
                            throw new Exception("DEBUG:  (1 != fargs[ifarg].Length)");
                        }
#endif
                        DbValue value = fargs[ifarg][0];
                        results.Add(value);
                    }
                }
            }
            return results;

        }


        // calls is e.g. "foo(hello(world))" in: "SELECT foo(hello(world)),bar(hi),baz FROM ..."
        // calls can be null to work like the overload without calls.
        public List<DbValue> ProcessSelectPart(string calls, IList<ByteSlice> rows)
        {
            if (null != calls)
            {
#if DEBUG
                //System.Diagnostics.Debugger.Launch();
#endif
                fcall = _GetSelectPartCalls(ref calls);
            }
            return ProcessSelectPart(rows);
        }

        // Uses the same calls as previous.
        public List<DbValue> ProcessSelectPart(IList<ByteSlice> rows)
        {
#if DEBUG
            if (fcall.func == null && (null == fcall.args || 0 == fcall.args.Count))
            {
                throw new Exception("DEBUG:  ProcessSelectPart(rows) called first without setting calls via ProcessSelectPart(calls, rows); or invalid CallInfo fcall");
            }
#endif
            this.fcallargsIndex = 0;
            this.fcalllistIndex = 0;
            this.frowsIndex = -1;
            this.frows = rows;
            return _ProcessSelectCallInfo(fcall);
        }

        IList<ByteSlice> frows;
        int frowsIndex = -1;
        CallInfo fcall;

        List<List<DbFunctionArguments>> fcallargs = new List<List<DbFunctionArguments>>();
        int fcallargsIndex;
        List<DbFunctionArguments> _NextCallArgs()
        {
            List<DbFunctionArguments> result;
            if (fcallargsIndex >= fcallargs.Count)
            {
                result = new List<DbFunctionArguments>();
                fcallargs.Add(result);
            }
            else
            {
                result = fcallargs[fcallargsIndex];
                result.Clear();
            }
            fcallargsIndex++;
            return result;
        }
        List<List<List<DbValue>>> fcalllist = new List<List<List<DbValue>>>();
        int fcalllistIndex;
        List<List<DbValue>> _NextCallList()
        {
            List<List<DbValue>> result;
            if (fcalllistIndex >= fcallargs.Count)
            {
                result = new List<List<DbValue>>();
                fcalllist.Add(result);
            }
            else
            {
                result = fcalllist[fcalllistIndex];
                result.Clear();
            }
            fcalllistIndex++;
            return result;
        }


        struct CallInfo
        {
            internal string func; // If null, not a function, and args is one dimension with a literal or column reference (value).
            internal IList<CallInfoArg> args;
        }

        struct CallInfoArg
        {
            internal DbValue value;
            internal CallInfo nest;
        }


        DbFunctionTools functools;
        IList<DbColumn> cols;


        #region IValueContext Members

        ByteSlice IValueContext.CurrentRow
        {
            get { return frows[frowsIndex]; }
        }

        IList<DbColumn> IValueContext.ColumnTypes
        {
            get { return cols; }
        }

        DbValue IValueContext.ExecDbFunction(string name, DbFunctionArguments args)
        {
            return DbExec.ExecDbScalarFunction(name, functools, args);
        }

        DbFunctionTools IValueContext.Tools
        {
            get { return functools; }
        }

        #endregion
    }

}

