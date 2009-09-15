using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_DataProvider
{
    public class QaClientFactory: DbProviderFactory
    {
        public static QaClientFactory Instance = new QaClientFactory();

        public override DbCommand CreateCommand()
        {
            return new QaCommand();
        }

        public override DbCommandBuilder CreateCommandBuilder()
        {
            throw new NotImplementedException();
        }

        public override DbConnection CreateConnection()
        {
            return new QaConnection();
        }

        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            throw new NotImplementedException();
        }

        public override DbDataAdapter CreateDataAdapter()
        {
            throw new NotImplementedException();
        }

        public override DbDataSourceEnumerator CreateDataSourceEnumerator()
        {
            throw new NotImplementedException();
        }

        public override DbParameter CreateParameter()
        {
            throw new NotImplementedException();
        }

        public override System.Security.CodeAccessPermission CreatePermission(System.Security.Permissions.PermissionState state)
        {
            throw new NotImplementedException();
        }

        public override bool CanCreateDataSourceEnumerator
        {
            get
            {
                return false;
            }
        }
    }
}
