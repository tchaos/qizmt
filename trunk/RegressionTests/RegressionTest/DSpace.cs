using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RegressionTest
{
    public class DSpace
    {
        public static void TestDSpaceGetFilePermissions(string[] args)
        {
            string exe = Exec.GetQizmtExe();
            string username = Environment.UserName.ToLower();
            string userdomain = Environment.UserDomainName.ToLower();
            string useraccount = userdomain + @"\" + username;

            Console.WriteLine();
            Console.WriteLine("-");
            Console.WriteLine("Testing: Qizmt get file permissions...");

            Exec.Shell(exe + " del TestDSpaceGetFilePermissions*", false);
            Exec.Shell(exe + " gen TestDSpaceGetFilePermissionsA1.txt 100MB", false);
            Exec.Shell(exe + " gen TestDSpaceGetFilePermissionsA2.txt 100MB", false);
            Exec.Shell(exe + " gen TestDSpaceGetFilePermissionsB1.txt 100MB", false);
            Exec.Shell(exe + " gen TestDSpaceGetFilePermissionsB2.txt 100MB", false);

            string dir = Environment.CurrentDirectory + @"\TestDSpaceGetFilePermissions\";
            if (System.IO.Directory.Exists(dir))
            {
                System.IO.Directory.Delete(dir, true);
            }
            System.IO.Directory.CreateDirectory(dir);

            dir = dir.Replace(":", "$");
            dir = @"\\" + System.Net.Dns.GetHostName() + @"\" + dir;

            Exec.Shell(exe + " get TestDSpaceGetFilePermissionsA1.txt " + dir + "A1.txt", false);
            Exec.Shell(exe + " get TestDSpaceGetFilePermissionsA1.txt " + dir + "A1.gz", false);
            Exec.Shell(exe + " get TestDSpaceGetFilePermissionsA*.txt " + dir, false);
            Exec.Shell(exe + " get parts=0-1 TestDSpaceGetFilePermissionsB1.txt " + dir + "B1.txt", false);
            Exec.Shell(exe + " get parts=0-1 TestDSpaceGetFilePermissionsB1.txt " + dir + "B1.gz", false);
            Exec.Shell(exe + " get parts=0-1 TestDSpaceGetFilePermissionsB*.txt " + dir, false);

            if (HasFullControl(useraccount, dir))
            {
                Console.WriteLine("[PASSED] - " + string.Join(" ", args));
            }
            else
            {
                Console.WriteLine("[FAILED] - " + string.Join(" ", args));
            }

            System.IO.Directory.Delete(dir, true);
            Exec.Shell(exe + " del TestDSpaceGetFilePermissions*", false);
        }

        public static void TestDSpaceGetBinaryFilePermissions(string[] args)
        {
            string exe = Exec.GetQizmtExe();
            string username = Environment.UserName.ToLower();
            string userdomain = Environment.UserDomainName.ToLower();
            string useraccount = userdomain + @"\" + username;

            Console.WriteLine();
            Console.WriteLine("-");
            Console.WriteLine("Testing: Qizmt getbinary file permissions...");

            Exec.Shell(exe + " del TestDSpaceGetBinaryFilePermissions*", false);
            Exec.Shell(exe + " gen TestDSpaceGetBinaryFilePermissions1.txt 40MB", false);
            Exec.Shell(exe + " gen TestDSpaceGetBinaryFilePermissions2.txt 40MB", false);

            string dir = Environment.CurrentDirectory + @"\TestDSpaceGetBinaryFilePermissions";
            if (System.IO.Directory.Exists(dir))
            {
                System.IO.Directory.Delete(dir, true);
            }
            System.IO.Directory.CreateDirectory(dir);
            dir = dir.Replace(":", "$");
            dir = @"\\" + System.Net.Dns.GetHostName() + @"\" + dir;

            string dir2 = dir + "2";
            if (System.IO.Directory.Exists(dir2))
            {
                System.IO.Directory.Delete(dir2, true);
            }
            System.IO.Directory.CreateDirectory(dir2);
            dir = dir + @"\";
            dir2 = dir2 + @"\";

            Exec.Shell(exe + " get TestDSpaceGetBinaryFilePermissions*.txt " + dir, false);
            Exec.Shell(exe + " putbinary " + dir + "*.* TestDSpaceGetBinaryFilePermissions.blob", false);
            Exec.Shell(exe + " getbinary TestDSpaceGetBinaryFilePermissions.blob " + dir2);

            if (HasFullControl(useraccount, dir2))
            {
                Console.WriteLine("[PASSED] - " + string.Join(" ", args));
            }
            else
            {
                Console.WriteLine("[FAILED] - " + string.Join(" ", args));
            }

            System.IO.Directory.Delete(dir, true);
            System.IO.Directory.Delete(dir2, true);
            Exec.Shell(exe + " del TestDSpaceGetBinaryFilePermissions*", false);
        }

        private static bool HasFullControl(string useracct, string dir)
        {
            string[] files = System.IO.Directory.GetFiles(dir);
            foreach (string file in files)
            {
                System.Security.AccessControl.FileSecurity fs = System.IO.File.GetAccessControl(file);
                System.Security.AccessControl.AuthorizationRuleCollection rules = fs.GetAccessRules(true, false, typeof(System.Security.Principal.NTAccount));

                bool found = false;
                foreach (System.Security.AccessControl.FileSystemAccessRule rule in rules)
                {
                    System.Security.Principal.IdentityReference id = rule.IdentityReference;
                    string rights = rule.FileSystemRights.ToString();
                    string acct = id.Value;
                    if (acct.ToLower() == useracct && rights.ToLower() == "fullcontrol")
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    return false;
                }
            }
            return true;
        }

        public static void TestDSpaceHosts(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Error: QizmtHosts command needs argument: <dfsXmlPath>");
                return;
            }

            string dfspath = args[1];

            if (!dfspath.StartsWith(@"\\"))
            {
                Console.Error.WriteLine("Argument: <dfsXmlPath> must be a network path");
                return;
            }

            System.Xml.XmlDocument dfs = new System.Xml.XmlDocument();
            string slavelist = null;
            try
            {
                dfs.Load(dfspath);
                System.Xml.XmlNode node = dfs.SelectSingleNode("//SlaveList");
                if (node == null)
                {
                    Console.Error.WriteLine("SlaveList node is not found in dfs.xml");
                    return;
                }

                slavelist = node.InnerText.ToUpper();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error loading dfs.xml: {0}", e.Message);
                return;
            }

            string mr = @"<SourceCode>
  <Jobs>
    <Job Name=`Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regression_test_Qizmt_Hosts_Input.txt`);
            Shell(@`Qizmt del regression_test_Qizmt_Hosts_Output.txt`);            
            
            string hosts = ``;
            for(int i = 0; i < StaticGlobals.Qizmt_Hosts.Length; i++)
            {
                if(i != 0)
                {
                    hosts += `;`;
                }
                hosts += StaticGlobals.Qizmt_Hosts[i];
            }
            
            Qizmt_Log(`local:` + hosts + `:`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://regression_test_Qizmt_Hosts_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`1`);
                
                string hosts = ``;
                for(int i = 0; i < StaticGlobals.Qizmt_Hosts.Length; i++)
                {
                    if(i != 0)
                    {
                        hosts += `;`;
                    }
                    hosts += StaticGlobals.Qizmt_Hosts[i];
                }
                
                Qizmt_Log(`remote:` + hosts + `:`);
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`mr` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>1</KeyLength>
        <DFSInput>dfs://regression_test_Qizmt_Hosts_Input.txt</DFSInput>
        <DFSOutput>dfs://regression_test_Qizmt_Hosts_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                output.Add(line, line);
                
                string hosts = ``;
                for(int i = 0; i < StaticGlobals.Qizmt_Hosts.Length; i++)
                {
                    if(i != 0)
                    {
                        hosts += `;`;
                    }
                    hosts += StaticGlobals.Qizmt_Hosts[i];
                }
                
                Qizmt_Log(`map:` + hosts + `:`);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
                string hosts = ``;
                for(int i = 0; i < StaticGlobals.Qizmt_Hosts.Length; i++)
                {
                    if(i != 0)
                    {
                        hosts += `;`;
                    }
                    hosts += StaticGlobals.Qizmt_Hosts[i];
                }
                
                Qizmt_Log(`reduce:` + hosts + `:`);
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`Post-processing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regression_test_Qizmt_Hosts_Input.txt`);
            Shell(@`Qizmt del regression_test_Qizmt_Hosts_Output.txt`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>".Replace('`', '"');

            Console.WriteLine("-");
            Console.WriteLine("Testing Qizmt_Hosts...");

            string dir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\RegressionTest\DSpaceHostsTest\";
            if (System.IO.Directory.Exists(dir))
            {
                System.IO.Directory.Delete(dir, true);
            }
            System.IO.Directory.CreateDirectory(dir);

            System.IO.File.WriteAllText(dir + @"regressionTest_dspaceHosts.xml", mr);

            string exe = Exec.GetQizmtExe();
            Exec.Shell(exe + @" del regressionTest_dspaceHosts.xml");
            Exec.Shell(exe + @" importdir " + dir);
            string results = Exec.Shell(exe + @" exec regressionTest_dspaceHosts.xml");

            bool ok = true;
            string[] expected = new string[] { "local:" + slavelist + ":", "remote:" + slavelist + ":", "map:" + slavelist + ":", "reduce:" + slavelist + ":" };
            foreach (string exp in expected)
            {
                if (results.IndexOf(exp, StringComparison.OrdinalIgnoreCase) == -1)
                {
                    ok = false;
                    break;
                }
            }

            if (ok)
            {
                Console.WriteLine("[PASSED] - " + string.Join(" ", args));
            }
            else
            {
                Console.WriteLine("[FAILED] - " + string.Join(" ", args));
            }

            Exec.Shell(exe + @" del regressionTest_dspaceHosts.xml");
            System.IO.Directory.Delete(dir, true);
        }

        public static void TestFormatMetaOnlySwitch(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Error: FormatMetaOnlySwitch command needs argument: <dfsXmlPath>");
                return;
            }

            string dfspath = args[1];

            if (!dfspath.StartsWith(@"\\"))
            {
                Console.Error.WriteLine("Argument: <dfsXmlPath> must be a network path");
                return;
            }

            System.Xml.XmlDocument dfs = new System.Xml.XmlDocument();
            string slavelist = null;
            try
            {
                dfs.Load(dfspath);
                System.Xml.XmlNode node = dfs.SelectSingleNode("//SlaveList");
                if (node == null)
                {
                    Console.Error.WriteLine("SlaveList node is not found in dfs.xml");
                    return;
                }
                slavelist = node.InnerText.ToUpper();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error loading dfs.xml: {0}", e.Message);
                return;
            }

            string dspacedir = "";
            {
                int del = dfspath.LastIndexOf(@"\");
                dspacedir = dfspath.Substring(0, del + 1);
                del = dspacedir.IndexOf(@"\", 2);
                dspacedir = dspacedir.Substring(del + 1);
            }

            Console.WriteLine("-");
            Console.WriteLine("Testing: Qizmt format metaOnly switch...");

            string fpath = "";
            if (!GenerateDfsFile("regressionTest_formatMetaOnlySwitchTest.txt", dfspath, dspacedir, ref fpath))
            {
                return;
            }

            //format with metaonly=1
            string exe = Exec.GetQizmtExe();
            Exec.Shell(exe + " @format slaves=" + slavelist + " metaonly=1");

            bool ok = false;

            if (System.IO.File.Exists(fpath))
            {
                if (!GenerateDfsFile("regressionTest_formatMetaOnlySwitchTest.txt", dfspath, dspacedir, ref fpath))
                {
                    return;
                }

                //format with metaonly=0
                Exec.Shell(exe + " @format slaves=" + slavelist);

                if (!System.IO.File.Exists(fpath))
                {
                    ok = true;
                }
            }

            if (ok)
            {
                Console.WriteLine("[PASSED] - " + string.Join(" ", args));
            }
            else
            {
                Console.WriteLine("[FAILED] - " + string.Join(" ", args));
            }
        }

        private static bool GenerateDfsFile(string fname, string dfspath, string dspacedir, ref string fpath)
        {
            string exe = Exec.GetQizmtExe();
            Exec.Shell(exe + @" del " + fname);
            Exec.Shell(exe + @" gen " + fname + " 100B");

            System.Xml.XmlDocument dfs = new System.Xml.XmlDocument();
            try
            {
                dfs.Load(dfspath);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error loading dfs.xml: {0}", e.Message);
                return false;
            }

            System.Xml.XmlNode fnode = dfs.SelectSingleNode(@"//DfsFile[Name='" + fname + @"']/Nodes/FileNode[1]");
            if (fnode == null)
            {
                Console.Error.WriteLine("File node of generated file not found in dfs.xml");
                return false;
            }

            string name = fnode["Name"].InnerText;
            string host = fnode["Host"].InnerText.Split(';')[0];
            fpath = @"\\" + host + @"\" + dspacedir + name;

            if (!System.IO.File.Exists(fpath))
            {
                Console.Error.WriteLine("Cannot find the file that was just generated.");
                return false;
            }
            return true;
        }

        public static void TestAddRemoveMachineClearCache(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Error: AddRemoveMachineClearCache command needs argument: <dfsXmlPath>");
                return;
            }

            string dfspath = args[1];

            if (!dfspath.StartsWith(@"\\"))
            {
                Console.Error.WriteLine("Argument: <dfsXmlPath> must be a network path");
                return;
            }

            string slavelist = null;
            try
            {
                System.Xml.XmlDocument dfs = new System.Xml.XmlDocument();
                dfs.Load(dfspath);
                System.Xml.XmlNode node = dfs.SelectSingleNode("//SlaveList");
                if (node == null)
                {
                    Console.Error.WriteLine("SlaveList node is not found in dfs.xml");
                    return;
                }

                slavelist = node.InnerText.ToUpper();
                string[] parts = slavelist.Split(';');
                if (parts.Length < 2)
                {
                    Console.Error.WriteLine("Must have at least 2 machines in SlaveList in dfs.xml to test.");
                    return;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error loading dfs.xml: {0}", e.Message);
                return;
            }

            string surrogate = dfspath.Substring(2, dfspath.IndexOf(@"\", 2) - 2).ToUpper();
            int si = 2 + surrogate.Length + 1;
            string dspacedir = dfspath.Substring(si, dfspath.LastIndexOf(@"\") - si) + @"\";

            if (string.Compare(surrogate, System.Net.Dns.GetHostName(), true) != 0)
            {
                Console.Error.WriteLine("AddRemoveMachineClearCache test must be run from the surrogate.");
                return;
            }

            string jobdir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\RegressionTest\removeMachineClearCacheTest\";
            {
                string mr = @"<SourceCode>
  <Jobs>
    <Job Name=`local` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regressionTest_removeMachineClearCache_Input.txt`);
            Shell(@`Qizmt del regressionTest_removeMachineClearCache_Output.txt`);
            Shell(@`Qizmt del regressionTest_removeMachineClearCache_Cache`);
            Shell(@`Qizmt gen regressionTest_removeMachineClearCache_Input.txt 200B`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`mr` Custodian=`` Email=``>
       <Delta>
        <Name>regressionTest_removeMachineClearCache_Cache</Name>
        <DFSInput>dfs://regressionTest_removeMachineClearCache_Input*.txt</DFSInput>
      </Delta>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput></DFSInput>
        <DFSOutput>dfs://regressionTest_removeMachineClearCache_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                output.Add(line, ByteSlice.Prepare());
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Add(values[i].Value);
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>".Replace('`', '"');
                if (System.IO.Directory.Exists(jobdir))
                {
                    System.IO.Directory.Delete(jobdir, true);
                }
                System.IO.Directory.CreateDirectory(jobdir);
                System.IO.File.WriteAllText(jobdir + "regressionTest_removeMachineClearCache.xml", mr);
            }

            string nonsurrogate = null;
            {
                string[] hosts = slavelist.Split(';');
                foreach (string host in hosts)
                {
                    if (string.Compare(host, surrogate, true) != 0)
                    {
                        nonsurrogate = host;
                        break;
                    }
                }
                if (nonsurrogate == null)
                {
                    Console.Error.WriteLine("Non-surrogate is not found from SlaveList in dfs.xml");
                    return;
                }
            }

            //Remove a non-surrogate machine.
            {
                Console.WriteLine("-");
                Console.WriteLine("Testing removal of a non-surrogate machine...");

                if (!CreateCache(surrogate, jobdir))
                {
                    return;
                }

                Exec.Shell(@"Qizmt removemachine " + nonsurrogate);

                if (CacheExists(surrogate))
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
            }

            //Add the machine back.
            {
                Console.WriteLine("-");
                Console.WriteLine("Testing adding of a machine...");

                if (!CreateCache(surrogate, jobdir))
                {
                    return;
                }

                Exec.Shell(@"Qizmt addmachine " + nonsurrogate);

                if (CacheExists(surrogate))
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
            }

            //Removal of a non-surrogate and non-responsive machine.
            {
                Console.WriteLine("-");
                Console.WriteLine("Testing removal of a non-surrogate and non-responsive machine...");

                //Format dfs to its original state.
                Exec.Shell(@"Qizmt @format slaves=" + slavelist);

                if (!CreateCache(surrogate, jobdir))
                {
                    return;
                }

                string dummymachine = nonsurrogate + "dummy";
                string newslavelist = slavelist.Replace(nonsurrogate, dummymachine);
                DFSUtils.ChangeDFSXMLSlaveList(dfspath, newslavelist);

                Exec.Shell(@"Qizmt removemachine " + dummymachine, true);

                if (CacheExists(surrogate))
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
            }

            //Remove a participating surrogate machine.
            {
                Console.WriteLine("-");
                Console.WriteLine("Testing removal of a participating surrogate...");

                //Participating surrogate.
                string newslavelist = slavelist;
                if (slavelist.IndexOf(surrogate, StringComparison.OrdinalIgnoreCase) == -1)
                {
                    newslavelist += ";" + surrogate;
                }
                Exec.Shell(@"Qizmt @format slaves=" + newslavelist);

                if (!CreateCache(surrogate, jobdir))
                {
                    return;
                }

                Exec.Shell(@"Qizmt removemachine " + surrogate);

                //Who is the new surrogate?
                string newsurrogate = LocateSurrogate(dspacedir, nonsurrogate);

                if (newsurrogate == null)
                {
                    Console.Error.WriteLine("After the removal of surrogate, cannot locate the new surrogate.");
                    return;
                }

                if (CacheExists(newsurrogate))
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
            }

            //Removal of a non-participating surrogate.
            {
                Console.WriteLine("-");
                Console.WriteLine("Testing removal of a non-participating surrogate...");

                string newslavelist = "";
                if (slavelist.IndexOf(surrogate, StringComparison.OrdinalIgnoreCase) > -1)
                {
                    string[] hosts = slavelist.Split(';');
                    foreach (string host in hosts)
                    {
                        if (string.Compare(host, surrogate, true) != 0)
                        {
                            newslavelist += ";" + host;
                        }
                    }
                    newslavelist = newslavelist.Trim(new char[] { ';' });
                }
                else
                {
                    newslavelist = slavelist;
                }

                Exec.Shell(@"Qizmt @format slaves=" + newslavelist);

                if (!CreateCache(surrogate, jobdir))
                {
                    return;
                }

                Exec.Shell(@"Qizmt removemachine " + surrogate);

                string newsurrogate = LocateSurrogate(dspacedir, nonsurrogate);

                if (newsurrogate == null)
                {
                    Console.Error.WriteLine("Cannot locate new surrogate after the removal of surrogate machine.");
                    return;
                }

                if (CacheExists(newsurrogate))
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
            }

            Exec.Shell(@"Qizmt @format slaves=" + slavelist);
        }

        private static string LocateSurrogate(string dspacedir, string machine)
        {
            if (System.IO.File.Exists(@"\\" + machine + @"\" + dspacedir + @"dfs.xml"))
            {
                return machine;
            }
            else
            {
                string slavedat = @"\\" + machine + @"\" + dspacedir + @"slave.dat";
                if (System.IO.File.Exists(slavedat))
                {
                    string[] lines = System.IO.File.ReadAllLines(slavedat);
                    if (lines.Length > 0)
                    {
                        string firstline = lines[0];
                        int del = firstline.IndexOf("=");
                        if (del > -1)
                        {
                            return firstline.Substring(del + 1);
                        }
                    }
                }
            }
            return null;
        }

        private static bool CreateCache(string surrogate, string jobdir)
        {
            //Create mr job, run to create cache.
            Exec.Shell(@"Qizmt @=" + surrogate + " del regressionTest_removeMachineClearCache.xml");
            Exec.Shell(@"Qizmt @=" + surrogate + " importdir " + jobdir);
            Exec.Shell(@"Qizmt @=" + surrogate + " exec regressionTest_removeMachineClearCache.xml");

            //Check cache exists.
            if (!CacheExists(surrogate))
            {
                Console.Error.WriteLine("Cache is not found after job executed.");
                return false;
            }
            return true;
        }

        private static bool CacheExists(string surrogate)
        {
            string result = Exec.Shell(@"Qizmt @=" + surrogate + " info regressionTest_removeMachineClearCache_Cache", true);
            return (result.Length > 0);
        }

        private static void TestPerfmonAdminCommandLock(string[] args)
        {
            Console.WriteLine("-");
            Console.WriteLine("Testing perfmon under admin command lock...");

            string exe = Exec.GetQizmtExe();
            try
            {
                MySpace.DataMining.AELight.AELight.EnterAdminCmd();
                Console.WriteLine("Admin command lock obtained...");

                Console.WriteLine("Running perfmon...");
                string output = Exec.Shell(exe + " perfmon availablememory", false);

                string expected = "Max Available Bytes";
                if (output.IndexOf(expected, StringComparison.OrdinalIgnoreCase) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
            }
            finally
            {
                MySpace.DataMining.AELight.AELight.CleanThisExecQ();
                Console.WriteLine("Admin command lock released...");
            }
        }
        
        private static void TestPacketSniffAdminCommandLock(string[] args)
        {
            Console.WriteLine("-");
            Console.WriteLine("Testing packet sniff under admin command lock...");

            string exe = Exec.GetQizmtExe();
            try
            {                
                MySpace.DataMining.AELight.AELight.EnterAdminCmd();
                Console.WriteLine("Admin command lock obtained...");

                Console.WriteLine("Running packet sniff...");
                string output = Exec.Shell(exe + " packetsniff", false);
                string expected = "sniffing";
                if (output.IndexOf(expected, StringComparison.OrdinalIgnoreCase) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }
            }
            finally
            {
                MySpace.DataMining.AELight.AELight.CleanThisExecQ();
                Console.WriteLine("Admin command lock released...");
            }
        }

        public static void TestAdminCommandLock(string action, string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Error: Command {0} needs argument: <dfsXmlPath>", action);
                return;
            }

            string dfspath = args[1];
            {
                if (!dfspath.StartsWith(@"\\"))
                {
                    Console.Error.WriteLine("Argument: <dfsXmlPath> must be a network path");
                    return;
                }

                string surrogate = dfspath.Substring(2, dfspath.IndexOf(@"\", 2) - 2).ToUpper();

                if (string.Compare(surrogate, System.Net.Dns.GetHostName(), true) != 0)
                {
                    Console.Error.WriteLine("Error: {0} test must be run from the surrogate.", action);
                    return;
                }
            }

            string dspacedir = null;
            {
                int del = dfspath.IndexOf('$');
                if (del > 0)
                {
                    string temp = dfspath.Substring(del - 1).Replace('$', ':').Trim();
                    del = temp.LastIndexOf(@"\");
                    if (del > -1)
                    {
                        dspacedir = temp.Substring(0, del);
                    }
                }
            }
            if (dspacedir == null)
            {
                Console.Error.WriteLine("Error: Cannot parse Qizmt dir.");
                return;
            }

            MySpace.DataMining.AELight.AELight.AELight_Dir = dspacedir;
            
            if (action == "packetsniffadmincommandlock")
            {
                TestPacketSniffAdminCommandLock(args);
            }
            else if (action == "perfmonadmincommandlock")
            {
                TestPerfmonAdminCommandLock(args);
            }
        }

        public static void TestClearLogs(string[] args)
        {            
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Error: Command clearlogs needs argument: <dfsXmlPath>");
                return;
            }

            string dfspath = args[1];
            if (!dfspath.StartsWith(@"\\"))
            {
                Console.Error.WriteLine("Error: Argument <dfsXmlPath> must be a network path");
                return;
            }

            string dspacedir = null;
            {
                int del = dfspath.IndexOf('$');
                if (del > 0)
                {
                    string temp = dfspath.Substring(del - 1).Trim();
                    del = temp.LastIndexOf(@"\");
                    if (del > -1)
                    {
                        dspacedir = temp.Substring(0, del);
                    }
                }
            }
            if (dspacedir == null)
            {
                Console.Error.WriteLine("Error: Cannot parse Qizmt dir.");
                return;
            }

            string[] hosts = null;
            {
                try
                {
                    System.Xml.XmlDocument dc = new System.Xml.XmlDocument();
                    dc.Load(dfspath);
                    System.Xml.XmlNode node = dc.SelectSingleNode("//SlaveList");
                    if (node == null)
                    {
                        Console.Error.WriteLine("SlaveList node is not found in dfs.xml");
                        return;
                    }
                    hosts = node.InnerText.Split(';');
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("Error while loading dfs.xml: {0}", e.ToString());
                    return;
                }
            }

            Console.WriteLine("-");
            Console.WriteLine("Testing clearlogs...");

            string dir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\RegressionTest\ClearLogsTest\";
            if (System.IO.Directory.Exists(dir))
            {
                System.IO.Directory.Delete(dir, true);
            }
            System.IO.Directory.CreateDirectory(dir);

            System.IO.File.WriteAllText(dir + "regressionTest_clearlogs.xml", @"
<SourceCode>
  <Jobs>
    <Job Name=`local` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regressionTest_clearlogs_Output*.txt`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`remote` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://regressionTest_clearlogs_Output####.txt</DFSWriter>
          <Mode>ALL MACHINES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {        
                dfsoutput.WriteLine(`x`);                
                string s = null;
                int x = s.Length;
           }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));

            string exe = Exec.GetQizmtExe();
            Exec.Shell(exe + " del regressionTest_clearlogs.xml", false);
            Exec.Shell(exe + " importdir " + dir, false);

            try
            {
                Exec.Shell(exe + " exec regressionTest_clearlogs.xml");
            }
            catch
            {
            }

            //Make sure there exists slave-log.txt.
            bool found = false;
            for (int i = 0; i < hosts.Length; i++)
            {
                if (System.IO.File.Exists(@"\\" + hosts[i] + @"\" + dspacedir + @"\slave-log.txt"))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                Console.Error.WriteLine("Slave-log.txt is not located.");
                return;
            }

            Exec.Shell(exe + " del regressionTest_clearlogs.xml", false);
            Exec.Shell(exe + " del regressionTest_clearlogs_Output*.xml", false);
            System.IO.Directory.Delete(dir, true);

            string output = Exec.Shell(exe + " clearlogs");            
            if (output.IndexOf("Done", StringComparison.OrdinalIgnoreCase) > -1)
            {
                found = false;
                for (int i = 0; i < hosts.Length; i++)
                {
                    if (System.IO.File.Exists(@"\\" + hosts[i] + @"\" + dspacedir + @"\slave-log.txt"))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                    return;
                }
            }

            Console.WriteLine("[FAILED] - " + string.Join(" ", args));            
        }
    }
}
