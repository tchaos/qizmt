using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        public class PrepareImportLines : Local
        {

            protected override void Run()
            {
                string TableName = DSpace_ExecArgs[0];
                string ImportDfsFile = DSpace_ExecArgs[1];
                string OutputDfsFile = DSpace_ExecArgs[2]; // Gets combined into DfsTableFile.
                string QlArgDelim = DSpace_ExecArgs[3];

                if (GetDfsFileRecordLength(ImportDfsFile) > 0)
                {
                    throw new Exception("Import file '" + ImportDfsFile + "' not found or has incorrect type");
                }

                System.Xml.XmlDocument systables;
                using (GlobalCriticalSection.GetLock())
                {
                    systables = LoadSysTables_unlocked();
                }

                System.Xml.XmlElement xeTable = FindTable(systables, TableName);
                if (null == xeTable)
                {
                    throw new Exception("Table '" + TableName + "' does not exist");
                }

                string DfsTableFile = xeTable["file"].InnerText;

                string RowInfo;
                string TypeInfo; // Type
                int ExpectedImportRowLength = 0;
                int OutputRowLength = 0;
                {
                    StringBuilder sbRowInfo = new StringBuilder();
                    StringBuilder sbTypeInfo = new StringBuilder(); // Type
                    foreach (System.Xml.XmlNode xn in xeTable.SelectNodes("column"))
                    {
                        if (0 != sbRowInfo.Length)
                        {
                            sbRowInfo.Append(',');
                            sbTypeInfo.Append(','); // Type
                        }
                        string stsize = xn["bytes"].InnerText;
                        int tsize = int.Parse(stsize);
                        ExpectedImportRowLength += tsize - 1; // Import rows don't include Nullable byte.
                        OutputRowLength += tsize;
                        sbRowInfo.Append(stsize);
                        sbTypeInfo.Append(xn["type"].InnerText); // Type
                    }
                    RowInfo = sbRowInfo.ToString();
                    TypeInfo = sbTypeInfo.ToString(); // Type
                }

                DSpace_Log(Shell("dspace exec \"//Job[@Name='RDBMS_ImportLines']/IOSettings/DFSInput=" + ImportDfsFile + "\" \"//Job[@Name='RDBMS_ImportLines']/IOSettings/DFSOutput=" + OutputDfsFile + "@" + OutputRowLength.ToString() + "\" RDBMS_ImportLines.DBCORE \"" + TableName + "\" \"" + ImportDfsFile + "\" \"" + OutputDfsFile + "\" \"" + DfsTableFile + "\" \"" + RowInfo + "\" \"" + TypeInfo + "\" \"" + QlArgDelim + "\"").Trim());
            }

        }

    }
}
