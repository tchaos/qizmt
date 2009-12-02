using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{

    public abstract class DbEval
    {
        public DbEval(IValueContext Context)
        {
            this.Context = Context;
        }


        public abstract bool Test();


        protected internal IValueContext Context;
    }

    public class DbEvalNot : DbEval
    {
        public DbEvalNot(DbEval eval)
            : base(eval.Context)
        {
            this.eval = eval;
        }


        public override bool Test()
        {
            return !eval.Test();
        }


        DbEval eval;
    }

    // val returns an int which is treated as a bool (val!=0)
    public class DbEvalIntBoolValue : DbEval
    {

        public DbEvalIntBoolValue(IValueContext Context, DbValue val)
            : base(Context)
        {
            this.val = val;
        }

        public override bool Test()
        {
            DbType type;
            ByteSlice bs = val.Eval(out type);
            if (DbTypeID.INT != type.ID)
            {
                throw new Exception("DbEvalIntBoolValue: expected INT value, not " + type.Name.ToUpper());
            }
            int x = Context.Tools.GetInt(bs);
            return x != 0;
        }

        DbValue val;
    }

    public class DbEvalAnd : DbEval
    {

        public DbEvalAnd(IValueContext Context, DbEval eval1, DbEval eval2)
            : base(Context)
        {
            this.eval1 = eval1;
            this.eval2 = eval2;
        }

        public override bool Test()
        {
            return eval1.Test() && eval2.Test();
        }

        DbEval eval1, eval2;
    }

    public class DbEvalOr : DbEval
    {

        public DbEvalOr(IValueContext Context, DbEval eval1, DbEval eval2)
            : base(Context)
        {
            this.eval1 = eval1;
            this.eval2 = eval2;
        }

        public override bool Test()
        {
            return eval1.Test() || eval2.Test();
        }

        DbEval eval1, eval2;
    }

    public class CompareEval : DbEval
    {
        DbValue val1, val2;
        DbFunctionArguments args;

        public CompareEval(IValueContext Context, DbValue val1, DbValue val2)
            : base(Context)
        {
            this.val1 = val1;
            this.val2 = val2;
            List<DbValue> argsbuf = new List<DbValue>(2);
            argsbuf.Add(val1);
            argsbuf.Add(val2);
            args = new DbFunctionArguments(argsbuf);
        }


        // 0 is match, <0 is less than, >0 is greater than.
        public int Compare()
        {
            DbValue result = Context.ExecDbFunction("COMPARE", args);
            DbType typeresult;
            ByteSlice bsresult = result.Eval(out typeresult);
#if DEBUG
            if (DbTypeID.INT != typeresult.ID)
            {
                throw new Exception("DEBUG:  COMPARE should return INT, not " + typeresult.Name.ToUpper());
            }
#endif
            return Context.Tools.GetInt(bsresult);
        }


        public override bool Test()
        {
            return 0 == Compare();
        }

    }

    public class CompareNotEqualEval : CompareEval
    {
        public CompareNotEqualEval(IValueContext Context, DbValue val1, DbValue val2)
            : base(Context, val1, val2)
        {
        }
        public override bool Test()
        {
            return Compare() != 0;
        }
    }

    public class GreaterThanEval : CompareEval
    {
        public GreaterThanEval(IValueContext Context, DbValue val1, DbValue val2)
            : base(Context, val1, val2)
        {
        }
        public override bool Test()
        {
            return Compare() > 0;
        }
    }
    public class GreaterThanOrEqualEval : CompareEval
    {
        public GreaterThanOrEqualEval(IValueContext Context, DbValue val1, DbValue val2)
            : base(Context, val1, val2)
        {
        }
        public override bool Test()
        {
            return Compare() >= 0;
        }
    }
    public class LessThanEval : CompareEval
    {
        public LessThanEval(IValueContext Context, DbValue val1, DbValue val2)
            : base(Context, val1, val2)
        {
        }
        public override bool Test()
        {
            return Compare() < 0;
        }
    }
    public class LessThanOrEqualEval : CompareEval
    {
        public LessThanOrEqualEval(IValueContext Context, DbValue val1, DbValue val2)
            : base(Context, val1, val2)
        {
        }
        public override bool Test()
        {
            return Compare() <= 0;
        }
    }


    public class WhereClause : PartReader, IValueContext
    {

        DbFunctionTools functools = new DbFunctionTools();


        public bool TestRow(ByteSlice row)
        {
            functools.ResetBuffers();
            _CurRow = row;

            return this.eval.Test();
        }


        public ByteSlice CurrentRow
        {
            get
            {
                return _CurRow;
            }
        }

        ByteSlice _CurRow;


        public IList<DbColumn> ColumnTypes
        {
            get
            {
                return _RowColTypes;
            }
        }

        IList<DbColumn> _RowColTypes;


        public DbFunctionTools Tools
        {
            get
            {
                return functools;
            }
        }


        public WhereClause(IList<DbColumn> RowColTypes)
        {
            this._RowColTypes = RowColTypes;
        }


        public DbValue ExecDbFunction(string name, DbFunctionArguments args)
        {
            return DbExec.ExecDbScalarFunction(name, functools, args);
        }


        public void Parse()
        {
            this.eval = ParseBase();
        }

        DbEval eval;


        DbEval ParsingCheckCondOp(DbEval result)
        {
            string next = PeekPart();
            if (0 == string.Compare(next, "AND", true))
            {
                NextPart(); // Eat the AND.
                DbEval nexteval = ParseBase();
                result = new DbEvalAnd(this, result, nexteval);
            }
            else if (0 == string.Compare(next, "OR", true))
            {
                NextPart(); // Eat the OR.
                DbEval nexteval = ParseBase();
                result = new DbEvalOr(this, result, nexteval);
            }
            return result;
        }


        public virtual DbEval ParseBase()
        {
#if DEBUG
            //System.Diagnostics.Debugger.Launch();
#endif

            bool NOT = false;
            if (0 == string.Compare("NOT", PeekPart(), true))
            {
                NextPart(); // Eat the "NOT".
                NOT = true;
            }

            string op;
            DbValue val1, val2;
            {

                {
                    if ("(" == PeekPart())
                    {
                        NextPart(); // Eat the "(".
                        DbEval leval = ParseBase();
                        leval = ParsingCheckCondOp(leval);
                        string srp = NextPart();
                        if (")" != srp)
                        {
                            throw new Exception("Expected ) not " + srp);
                        }
                        if (NOT)
                        {
                            leval = new DbEvalNot(leval);
                        }
                        return ParsingCheckCondOp(leval);
                    }
                    Types.ExpressionType letype;
                    string lvalue = Types.ReadNextBasicExpression(this, out letype);
                    val1 = ExpressionToDbValue(lvalue, letype);
                }

                op = NextPart();
                if (">" == op)
                {
                    if (PeekPart() == "=")
                    {
                        NextPart();
                        op = ">=";
                    }
                }
                else if ("<" == op)
                {
                    string np = PeekPart();
                    if (np == "=")
                    {
                        NextPart();
                        op = "<=";
                    }
                    else if (np == ">")
                    {
                        NextPart();
                        op = "<>";
                    }
                }
                else if (0 == string.Compare("IS", op, true))
                {
                    string isx = NextPart();
                    if (0 == string.Compare("NULL", isx, true))
                    {
                        // IS NULL...?
                        DbValue val = new FuncEvalValue(this, "ISNULL", val1);
                        DbEval isresult = new DbEvalIntBoolValue(this, val);
                        if (NOT)
                        {
                            isresult = new DbEvalNot(isresult);
                        }
                        return ParsingCheckCondOp(isresult);
                    }
                    else if (0 == string.Compare("NOT", isx, true))
                    {
                        isx = NextPart();
                        if (0 == string.Compare("NULL", isx, true))
                        {
                            // IS NOT NULL...?
                            DbValue val = new FuncEvalValue(this, "ISNOTNULL", val1);
                            DbEval isresult = new DbEvalIntBoolValue(this, val);
                            if (NOT)
                            {
                                isresult = new DbEvalNot(isresult);
                            }
                            return ParsingCheckCondOp(isresult);
                        }
                        else
                        {
                            throw new Exception("Expected NULL after IS NOT, not " + isx);
                        }
                    }
                    else
                    {
                        throw new Exception("Expected NULL or NOT NULL after IS, not " + isx);
                    }
                }

                {
                    if ("(" == PeekPart())
                    {
                        NextPart(); // Eat the "(".
                        DbEval reval = ParseBase();
                        reval = ParsingCheckCondOp(reval);
                        string srp = NextPart();
                        if (")" != srp)
                        {
                            throw new Exception("Expected ) not " + srp);
                        }
                        if (NOT)
                        {
                            reval = new DbEvalNot(reval);
                        }
                        return ParsingCheckCondOp(reval);
                    }
                    Types.ExpressionType retype;
                    string rvalue = Types.ReadNextBasicExpression(this, out retype);
                    val2 = ExpressionToDbValue(rvalue, retype);
                }

            }

            DbEval result;

            if ("=" == op)
            {
                result = new CompareEval(this, val1, val2);
            }
            else if (">" == op)
            {
                result = new GreaterThanEval(this, val1, val2);
            }
            else if (">=" == op)
            {
                result = new GreaterThanOrEqualEval(this, val1, val2);
            }
            else if ("<" == op)
            {
                result = new LessThanEval(this, val1, val2);
            }
            else if ("<=" == op)
            {
                result = new LessThanOrEqualEval(this, val1, val2);
            }
            else if ("<>" == op)
            {
                result = new CompareNotEqualEval(this, val1, val2);
            }
            else if (0 == string.Compare(op, "LIKE", true))
            {
                DbValue val = new FuncEvalValue(this, "ISLIKE", val1, val2);
                result = new DbEvalIntBoolValue(this, val);
            }
            else
            {
                throw new Exception("Unknown operation: " + op);
            }

            if (NOT)
            {
                result = new DbEvalNot(result);
            }
            return ParsingCheckCondOp(result);

        }


        DbValue ExpressionToDbValue(string value, Types.ExpressionType etype)
        {
            switch(etype)
            {
                case Types.ExpressionType.FUNCTION:
                    {
                        int ilp = value.IndexOf('(');
#if DEBUG
                        if (-1 == ilp)
                        {
                            throw new Exception("DEBUG:  ExpressionToDbValue: function expects (");
                        }
                        if (!value.EndsWith(")"))
                        {
                            throw new Exception("DEBUG:  ExpressionToDbValue: function expects )");
                        }
#endif
                        string fname = value.Substring(0, ilp);
                        string sargs = value.Substring(ilp + 1, value.Length - ilp - 1 - 1);
                        List<DbValue> args = new List<DbValue>();
                        StringPartReader argsparser = new StringPartReader(sargs);
                        if (argsparser.PeekPart().Length != 0)
                        {
                            for (; ; )
                            {
                                if (0 == string.Compare("AS", argsparser.PeekPart(), true))
                                {
                                    argsparser.NextPart(); // Eat "AS".
                                    {
                                        StringBuilder sbas = new StringBuilder();
                                        int asparens = 0;
                                        for (; ; )
                                        {
                                            string s = argsparser.PeekPart();
                                            if (0 == s.Length || "," == s)
                                            {
                                                break;
                                            }
                                            else if ("(" == s)
                                            {
                                                asparens++;
                                                sbas.Append(argsparser.NextPart());
                                            }
                                            else if (")" == s)
                                            {
                                                if (0 == asparens)
                                                {
                                                    break;
                                                }
                                                asparens--;
                                                sbas.Append(argsparser.NextPart());
                                            }
                                            else
                                            {
                                                sbas.Append(argsparser.NextPart());
                                            }
                                        }
                                        if (0 == sbas.Length)
                                        {
                                            throw new Exception("Expected type after AS");
                                        }
                                        args.Add(ExpressionToDbValue("'AS " + sbas.Replace("'", "''") + "'",
                                            Types.ExpressionType.AS));

                                    }
                                }
                                else
                                {
                                    string argvalue;
                                    Types.ExpressionType argetype;

                                    argvalue = Types.ReadNextBasicExpression(argsparser, out argetype);
                                    args.Add(ExpressionToDbValue(argvalue, argetype));
                                }

                                {
                                    string s = argsparser.PeekPart();
                                    if (s != ",")
                                    {
                                        if (0 == string.Compare("AS", s, true))
                                        {
                                            continue;
                                        }
                                        if (s.Length != 0)
                                        {
                                            throw new Exception("Unexpected in function arguments: " + argsparser.RemainingString);
                                        }
                                        break;
                                    }
                                }
                                argsparser.NextPart(); // Eat the ','.
                            }
                        }

                        return new FuncEvalValue(this, fname, args);

                    }
                    break;

                case Types.ExpressionType.NAME:
                    {
                        int FieldIndex = _IndexOfRowColType_ensure(value);
                        return new ColValue(this, ColumnTypes[FieldIndex]);
                    }
                    break;

                case Types.ExpressionType.NUMBER:
                case Types.ExpressionType.STRING:
                case Types.ExpressionType.AS:
                case Types.ExpressionType.NULL:
                    {
                        DbTypeID id;
                        ByteSlice bs = Types.LiteralToValue(value, out id);
                        return new ImmediateValue(this, bs, DbType.Prepare(bs.Length, id));
                    }
                    break;

                default:
                    throw new Exception("Unexpected value: " + value);
            }
        }


        int _IndexOfRowColType(string name)
        {
            return DbColumn.IndexOf(ColumnTypes, name);
        }

        int _IndexOfRowColType_ensure(string name)
        {
            int result = _IndexOfRowColType(name);
            if (-1 == result)
            {
                throw new Exception("Column named '" + name + "' not found");
            }
            return result;
        }


    }


}
