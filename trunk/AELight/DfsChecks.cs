/**************************************************************************************
 *  MySpace’s Mapreduce Framework is a mapreduce framework for distributed computing  *
 *  and developing distributed computing applications on large clusters of servers.   *
 *                                                                                    *
 *  Copyright (C) 2008  MySpace Inc. <http://qizmt.myspace.com/>                      *
 *                                                                                    *
 *  This program is free software: you can redistribute it and/or modify              *
 *  it under the terms of the GNU General Public License as published by              *
 *  the Free Software Foundation, either version 3 of the License, or                 *
 *  (at your option) any later version.                                               *
 *                                                                                    *
 *  This program is distributed in the hope that it will be useful,                   *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of                    *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                     *
 *  GNU General Public License for more details.                                      *
 *                                                                                    *
 *  You should have received a copy of the GNU General Public License                 *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.             *
***************************************************************************************/


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {

        internal static void DoDfsCheck(string[] args, bool fix)
        {
            if (fix)
            {
                if (args.Length == 0)
                {
                    Console.Error.WriteLine(" -all switch required for dfsfix");
                    SetFailure();
                    return;
                }
            }
            string userspec = null;
            bool userspecchunk = false;
            bool userspecdfsfile = false;
            if (args.Length > 0)
            {
                if (0 != string.Compare(args[0], "-all", true))
                {
                    if ('-' == args[0][0])
                    {
                        Console.Error.WriteLine("Unknown switch: {0}", args[0]);
                        SetFailure();
                        return;
                    }
                    userspec = args[0];
                    if (userspec.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        userspec = userspec.Substring(6);
                        userspecdfsfile = true;
                    }
                    else if (userspec.StartsWith("zd.", StringComparison.OrdinalIgnoreCase)
                        && userspec.EndsWith(".zd", StringComparison.OrdinalIgnoreCase)
                        && -1 != userspec.IndexOf('-')
                        && userspec.Length > 45)
                    {
                        userspecchunk = true;
                    }
                    else
                    {
                        userspecdfsfile = true;
                    }
                }
            }

            dfs dc = LoadDfsConfig();

            if (dc.Files.Count == 0)
            {
                Console.WriteLine("No DFS files to scan");
                return;
            }

            string tempfnpost = "." + Guid.NewGuid().ToString() + "." + System.Diagnostics.Process.GetCurrentProcess().Id.ToString();

            string[] hosts = dc.Slaves.SlaveList.Split(';');

            Dictionary<string, StringBuilder> sbdfsfiles = new Dictionary<string, StringBuilder>(StringComparer.OrdinalIgnoreCase);
            foreach (string host in hosts)
            {
                StringBuilder sb = new StringBuilder(1024);
                sbdfsfiles[host] = sb;
            }
            bool addedone = false;
            foreach (dfs.DfsFile df in dc.Files)
            {
                if (0 == string.Compare(DfsFileTypes.NORMAL, df.Type, true)
                    || 0 == string.Compare(DfsFileTypes.BINARY_RECT, df.Type, true))
                {
                    if (userspecdfsfile)
                    {
                        if (0 != string.Compare(df.Name, userspec, true))
                        {
                            continue;
                        }
                    }
                    foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                    {
                        if (userspecchunk)
                        {
                            if (0 != string.Compare(fn.Name, userspec, true))
                            {
                                continue;
                            }
                        }
                        foreach (string rhost in fn.Host.Split(';'))
                        {
                            StringBuilder sb = sbdfsfiles[rhost];
                            sb.AppendLine(fn.Host + @"?" + df.Name + @"?" + fn.Name);
                        }
                        addedone = true;
                    }
                }
            }
            if (!addedone)
            {
                Console.WriteLine("Nothing to scan");
                return;
            }
            foreach (KeyValuePair<string, StringBuilder> kvp in sbdfsfiles)
            {
                try
                {
                    System.IO.File.WriteAllText(Surrogate.NetworkPathForHost(kvp.Key) + @"\dfscheck" + tempfnpost, kvp.Value.ToString());
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("Skipping host {0} due to error: {1}", kvp.Key, e.Message);
                    LogOutputToFile(e.ToString());
                }
            }

            string jobsfn = "dfschecks-jobs.xml" + tempfnpost;
            try
            {
                using (System.IO.StreamWriter sw = System.IO.File.CreateText(jobsfn))
                {
                    sw.Write((@"<SourceCode>
  <Jobs>
     <Job Name=`DfsCheck` Custodian=`` Email=`` Description=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter></DFSWriter>
          <Mode>ALL MACHINES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
			
            int numproblems = 0;
            int numfixed = 0;

            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                    const bool fix = " + (fix ? "true" : "false") + @";
                    string dfsfilesinfofp = `dfscheck" + tempfnpost + @"`;
                    string[] dfsfilesinfos = System.IO.File.ReadAllLines(dfsfilesinfofp);
                    if(dfsfilesinfos.Length == 0)
                    {
                        return;
                    }
                    MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                        new Action<string>(
                        delegate(string dfsfilesinf)
                        {
                            string[] qq = dfsfilesinf.Split('?');
                            string chunkhost = qq[0]; // This machine is the first one.
                            string dfsfilename = qq[1];
                            string dfschunkname = qq[2];
                            try
                            {
                                using(System.IO.FileStream fs = new System.IO.FileStream(dfschunkname,
                                    System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                                {
                                    for(;;)
                                    {
                                        if(-1 == fs.ReadByte())
                                        {
                                            break;
                                        }
                                    }
                                }
                            }
                            catch
                            {
                                lock(this)
                                {
                                    numproblems++;
                                }
                                Qizmt_Log(`  DFS file '` + dfsfilename + `' has problem with chunk '` + dfschunkname + `' on host ` + Qizmt_MachineHost);
                                if(fix)
                                {
                                    string[] rhosts = chunkhost.Split(';');
                                    if(rhosts.Length < 2)
                                    {
                                        Qizmt_Log(`    Not enough replicates`);
                                    }
                                    else
                                    {
                                        for(int ir = 1; ir < rhosts.Length; ir++)
                                        {
                                            try
                                            {
                                                string newdfschunkname = `zd.dfscheck.` + Guid.NewGuid().ToString() + `.zd`;
                                                System.IO.File.Copy(MySpace.DataMining.AELight.Surrogate.NetworkPathForHost(rhosts[ir]) + @`\` + dfschunkname,
                                                    newdfschunkname, false);
                                                System.IO.File.Delete(dfschunkname);
                                                System.IO.File.Move(newdfschunkname, dfschunkname);
                                                numfixed++;
                                                break;
                                            }
                                            catch(Exception e)
                                            {
                                                if(ir >= rhosts.Length - 1)
                                                {
                                                    Qizmt_Log(`    Unable to repair DFS file '` + dfsfilename + `' chunk '` + dfschunkname + `' on host ` + Qizmt_MachineHost + ` because: ` + e.ToString());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }), dfsfilesinfos);
                if(fix)
                {
                    Qizmt_Log(`      ` + numfixed + ` of ` + numproblems + ` problems fixed on host ` + Qizmt_MachineHost);
                }
                else
                {
                    Qizmt_Log(`      ` + numproblems + ` problems found on host ` + Qizmt_MachineHost);
                }
            }
            
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                }

                if (userspecchunk)
                {
                    Console.WriteLine("Checking DFS chunk file {0}...", userspec);
                }
                else if (userspecdfsfile)
                {
                    Console.WriteLine("Checking DFS file dfs://{0}...", userspec);
                }
                else
                {
                    Console.WriteLine("Checking DFS...");
                }
                Exec("", LoadConfig(jobsfn), new string[] { }, false, false);
            }
            finally
            {
                try
                {
                    System.IO.File.Delete(jobsfn);
                }
                catch
                {
                }
                foreach (KeyValuePair<string, StringBuilder> kvp in sbdfsfiles)
                {
                    try
                    {
                        System.IO.File.Delete(Surrogate.NetworkPathForHost(kvp.Key) + @"\dfscheck" + tempfnpost);
                    }
                    catch
                    {
                    }
                }
            }

        }


        public static void DfsCheck(string[] args)
        {
            EnterAdminCmd();
            DoDfsCheck(args, false);
        }


        public static void DfsFix(string[] args)
        {
            EnterAdminCmd();
            DoDfsCheck(args, true);
        }


        public static void ChkDfs(string[] args)
        {
            EnterAdminCmd();
            //....
        }

    }

}

