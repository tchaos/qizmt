using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        public class WriteTopRemote : Remote
        {

            public override void OnRemote(IEnumerator<ByteSlice> input, System.IO.Stream output)
            {

                long TopCount = long.Parse(DSpace_ExecArgs[0]);

                while ((TopCount == -1 || TopCount > 0) && input.MoveNext())
                {
                    ByteSlice row = input.Current;
                    for (int i = 0; i < row.Length; i++)
                    {
                        output.WriteByte(row[i]);
                    }

                    if (TopCount != -1)
                    {
                        TopCount--;
                    }
                }

            }

        }

    }

}