using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_DataProvider
{  
    public class QaParameter: DbParameter
    {
        private string name = "";
        private DbType dbtype;
        private int size = 0;
        private object pvalue = null;
        private ParameterDirection direction = ParameterDirection.Input;
        internal QaParameterCollection parent = null;

        public QaParameter()
        {
        }

        public QaParameter(string paramname, DbType type, int paramsize, object value)
        {
            name = paramname;
            dbtype = type;
            size = paramsize;
            pvalue = value;
        }

        public override object Value
        {
            get
            {
                return pvalue;
            }
            set
            {
                pvalue = value;
            }
        }

        public override System.Data.DataRowVersion SourceVersion
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override bool SourceColumnNullMapping
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override string SourceColumn
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override int Size
        {
            get
            {
                return size;
            }
            set
            {
                size = value;
            }
        }

        public override string ParameterName
        {
            get
            {
                return name;
            }
            set
            {
                name = value;
            }
        }

        public override bool IsNullable
        {
            get
            {
                return true;
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override System.Data.ParameterDirection Direction
        {
            get
            {
                return direction;
            }
            set
            {
                if (value != ParameterDirection.Input)
                {
                    throw new Exception("Only ParameterDirection.Input is supported for QaParameter.");
                }
                direction = value;
            }
        }

        public override DbType DbType
        {
            get
            {
                return dbtype;
            }
            set
            {
                dbtype = value;
            }
        }

        public override void ResetDbType()
        {
            throw new NotImplementedException();
        }

        internal void ResetParent()
        {
            parent = null;
        }
    }
}