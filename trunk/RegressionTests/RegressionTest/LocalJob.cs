using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RegressionTest
{
    public class LocalJob
    {
        public static void TestHost(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Error: LocalJobHost command needs argument: <dfsXmlPath>");
                return;
            }

            string dfspath = args[1];

            if (!dfspath.StartsWith(@"\\"))
            {
                Console.Error.WriteLine("Argument: <dfsXmlPath> must be a network path");
                return;
            }

            string surrogate = dfspath.Substring(2, dfspath.IndexOf(@"\", 2) - 2).ToUpper();

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
                string[] parts = slavelist.Split(new char[] { ',', ';' });
                if (parts.Length < 2)
                {
                    Console.Error.WriteLine("Must have at least 2 hosts in SlaveList tag in dfs.xml");
                    return;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error loading dfs.xml: {0}", e.Message);
                return;
            }

            string exe = Exec.GetQizmtExe();

            //Test Host tag, empty and non-empty.
            {
                string[] hosts = slavelist.Split(new char[] { ',', ';' });
                string nonsurrogate = null;
                string firsthost = hosts[0];
                foreach (string host in hosts)
                {
                    if (string.Compare(surrogate, host, true) != 0)
                    {
                        nonsurrogate = host;
                        break;
                    }
                }
                if (nonsurrogate == null)
                {
                    Console.Error.WriteLine("Non-surrogate is not found from dfs.xml");
                    return;
                }

                string mr = @"<SourceCode>
  <Jobs>
    <Job Name=`mash_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
        <Host>xxx</Host>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Qizmt_Log(`DNS=` + System.Net.Dns.GetHostName());
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>".Replace('`', '"').Replace("xxx", nonsurrogate);

                string dir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\RegressionTest\LocalJobHostTest\";
                if (System.IO.Directory.Exists(dir))
                {
                    System.IO.Directory.Delete(dir, true);
                }
                System.IO.Directory.CreateDirectory(dir);

                System.IO.File.WriteAllText(dir + "regressionTest_localJobHost_nonEmptyHost.xml", mr);

                mr = @"<SourceCode>
  <Jobs>
    <Job Name=`mash_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Qizmt_Log(`DNS=` + System.Net.Dns.GetHostName());
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>".Replace('`', '"');

                System.IO.File.WriteAllText(dir + "regressionTest_localJobHost_emptyHost.xml", mr);

                Exec.Shell(exe + " del regressionTest_localJobHost_nonEmptyHost.xml");
                Exec.Shell(exe + " del regressionTest_localJobHost_emptyHost.xml");
                Exec.Shell(exe + " importdir " + dir);

                Console.WriteLine("-");
                Console.WriteLine("Testing non-empty host in local job...");

                string output = Exec.Shell(exe + " exec regressionTest_localJobHost_nonEmptyHost.xml");
                string expected = "DNS=" + nonsurrogate;
                if (output.IndexOf(expected, StringComparison.OrdinalIgnoreCase) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }

                Console.WriteLine("-");
                Console.WriteLine("Testing empty host in local job...");

                output = Exec.Shell(exe + " exec regressionTest_localJobHost_emptyHost.xml");
                expected = "DNS=" + firsthost;
                if (output.IndexOf(expected, StringComparison.OrdinalIgnoreCase) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }

                //Clean up
                System.IO.Directory.Delete(dir, true);
                Exec.Shell(exe + " del regressionTest_localJobHost_nonEmptyHost.xml");
                Exec.Shell(exe + " del regressionTest_localJobHost_emptyHost.xml");
            }
        }

        public static void TestAddRefNonParticipatingCluster(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Error: LocalJobAddRefNonParticipatingCluster command needs argument: <dfsXmlPath>");
                return;
            }

            string dfspath = args[1];

            if (!dfspath.StartsWith(@"\\"))
            {
                Console.Error.WriteLine("Argument: <dfsXmlPath> must be a network path");
                return;
            }

            string surrogate = dfspath.Substring(2, dfspath.IndexOf(@"\", 2) - 2).ToUpper();

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
                string[] parts = slavelist.Split(new char[] { ',', ';' });
                if (parts.Length < 2)
                {
                    Console.Error.WriteLine("Must have at least 2 hosts in SlaveList tag in dfs.xml");
                    return;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error loading dfs.xml: {0}", e.Message);
                return;
            }

            string dfsback = null;
            bool ok = false;
            try
            {
                if (slavelist.IndexOf(surrogate, StringComparison.OrdinalIgnoreCase) > -1)
                {
                    if (!DFSUtils.MakeFileBackup(dfspath, ref dfsback))
                    {
                        Console.Error.WriteLine("Error while backing up dfs.xml.");
                        return;
                    }
                    string newslavelist = "";
                    string[] hosts = slavelist.Split(new char[] { ',', ';' });
                    foreach (string host in hosts)
                    {
                        if (string.Compare(surrogate, host, true) != 0)
                        {
                            newslavelist = ";" + host;
                        }
                    }
                    newslavelist = newslavelist.Trim(';');                    
                    DFSUtils.ChangeDFSXMLSlaveList(dfs, dfspath, newslavelist);
                }
                               
                string mr = @"<SourceCode>
  <Jobs>
    <Job>
      <Narrative>
        <Name>regression_test_put_dll_Preprocessing</Name>
        <Custodian></Custodian>
        <email></email>
      </Narrative>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regressionTestLocalJobAddRef4A30DE94_testdll.dll`);
        }
        ]]>
      </Local>
    </Job>
    <Job>
      <Narrative>
        <Name>regression_test_put_dll PUT DLL</Name>
        <Custodian></Custodian>
        <email></email>
      </Narrative>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            string localdir = IOUtils.GetTempDirectory();            
            string fn = `regressionTestLocalJobAddRef4A30DE94_testdll.dll`;
            string localfn = localdir + @`\` + Guid.NewGuid().ToString() + fn;            
            string testdlldatab64 = `TVqQAAMAAAAEAAAA//8AALgAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAA4fug4AtAnNIbgBTM0hVGhpcyBwcm9ncmFtIGNhbm5vdCBiZSBydW4gaW4gRE9TIG1vZGUuDQ0KJAAAAAAAAABQRQAATAEDAOMDJ0oAAAAAAAAAAOAAAiELAQgAAAgAAAAIAAAAAAAALicAAAAgAAAAQAAAAABAAAAgAAAAAgAABAAAAAAAAAAEAAAAAAAAAACAAAAAAgAAAAAAAAMAQIUAABAAABAAAAAAEAAAEAAAAAAAABAAAAAAAAAAAAAAAOAmAABLAAAAAEAAACgEAAAAAAAAAAAAAAAAAAAAAAAAAGAAAAwAAAAsJgAAHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAACAAAAAAAAAAAAAAACCAAAEgAAAAAAAAAAAAAAC50ZXh0AAAANAcAAAAgAAAACAAAAAIAAAAAAAAAAAAAAAAAACAAAGAucnNyYwAAACgEAAAAQAAAAAYAAAAKAAAAAAAAAAAAAAAAAABAAABALnJlbG9jAAAMAAAAAGAAAAACAAAAEAAAAAAAAAAAAAAAAAAAQAAAQgAAAAAAAAAAAAAAAAAAAAAQJwAAAAAAAEgAAAACAAUAcCAAALwFAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABMwAQALAAAAAQAAEQByAQAAcAorAAYqHgIoEAAACioAQlNKQgEAAQAAAAAADAAAAHYyLjAuNTA3MjcAAAAABQBsAAAAwAEAACN+AAAsAgAAiAIAACNTdHJpbmdzAAAAALQEAAAUAAAAI1VTAMgEAAAQAAAAI0dVSUQAAADYBAAA5AAAACNCbG9iAAAAAAAAAAIAAAFHFAIACQAAAAD6ATMAFgAAAQAAABEAAAACAAAAAgAAABAAAAANAAAAAQAAAAEAAAABAAAAAAAKAAEAAAAAAAYAggB7AAYApwCVAAYAvgCVAAYA2wCVAAYA+gCVAAYAEwGVAAYALAGVAAYARwGVAAYAYgGVAAYAmgF7AQYArgF7AQYAvAGVAAYA1QGVAAYABQLyATsAGQIAAAYASAIoAgYAaAIoAgAAAAABAAAAAAABAAEAAQAQADsARQAFAAEAAQBQIAAAAACWAIkACgABAGcgAAAAAIYYjwAOAAEAEQCPABIAGQCPABIAIQCPABIAKQCPABIAMQCPABIAOQCPABIAQQCPABIASQCPABIAUQCPABcAWQCPABIAYQCPABIAaQCPABIAcQCPABwAgQCPACIAiQCPAA4ACQCPAA4ALgALACsALgATAF0ALgAbAF0ALgAjAF0ALgArACsALgAzAGMALgA7AF0ALgBLAF0ALgBTAHsALgBjAKUALgBrALIALgBzALsALgB7AMQAJwAEgAAAAQAAAAAAAAAAAAAAAABFAAAAAgAAAAAAAAAAAAAAAQByAAAAAAAAAAA8TW9kdWxlPgByZWdyZXNzaW9uVGVzdExvY2FsSm9iQWRkUmVmNEEzMERFOTRfdGVzdGRsbC5kbGwAVGVzdENsYXNzAHJlZ3Jlc3Npb25UZXN0TG9jYWxKb2JBZGRSZWY0QTMwREU5NF90ZXN0ZGxsAG1zY29ybGliAFN5c3RlbQBPYmplY3QAU2F5SGkALmN0b3IAU3lzdGVtLlJlZmxlY3Rpb24AQXNzZW1ibHlUaXRsZUF0dHJpYnV0ZQBBc3NlbWJseURlc2NyaXB0aW9uQXR0cmlidXRlAEFzc2VtYmx5Q29uZmlndXJhdGlvbkF0dHJpYnV0ZQBBc3NlbWJseUNvbXBhbnlBdHRyaWJ1dGUAQXNzZW1ibHlQcm9kdWN0QXR0cmlidXRlAEFzc2VtYmx5Q29weXJpZ2h0QXR0cmlidXRlAEFzc2VtYmx5VHJhZGVtYXJrQXR0cmlidXRlAEFzc2VtYmx5Q3VsdHVyZUF0dHJpYnV0ZQBTeXN0ZW0uUnVudGltZS5JbnRlcm9wU2VydmljZXMAQ29tVmlzaWJsZUF0dHJpYnV0ZQBHdWlkQXR0cmlidXRlAEFzc2VtYmx5VmVyc2lvbkF0dHJpYnV0ZQBBc3NlbWJseUZpbGVWZXJzaW9uQXR0cmlidXRlAFN5c3RlbS5EaWFnbm9zdGljcwBEZWJ1Z2dhYmxlQXR0cmlidXRlAERlYnVnZ2luZ01vZGVzAFN5c3RlbS5SdW50aW1lLkNvbXBpbGVyU2VydmljZXMAQ29tcGlsYXRpb25SZWxheGF0aW9uc0F0dHJpYnV0ZQBSdW50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aQBvAG4AAAAxAC4AMAAuADAALgAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAMAAAAMDcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA`;
            byte[] testdlldata = System.Convert.FromBase64String(testdlldatab64);
            System.IO.File.WriteAllBytes(localfn, testdlldata);
            try
            {
                Shell(@`Qizmt dfs put ` + localfn + ` ` + fn);
            }
            finally
            {
                System.IO.File.Delete(localfn);
            }
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`testdll` Custodian=`` email=``>
      <Add Reference=`regressionTestLocalJobAddRef4A30DE94_testdll.dll` Type=`dfs` />
      <Using>regressionTestLocalJobAddRef4A30DE94_testdll</Using>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Qizmt_Log(TestClass.SayHi());
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`testdll` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regressionTestLocalJobAddRef4A30DE94_testdll.dll`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"');

                string dir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\RegressionTest\LocalJobAddRefTest\";
                if (System.IO.Directory.Exists(dir))
                {
                    System.IO.Directory.Delete(dir, true);
                }
                System.IO.Directory.CreateDirectory(dir);

                System.IO.File.WriteAllText(dir + "regressionTest_localJobAddRef.xml", mr);

                string exe = Exec.GetQizmtExe();
                Exec.Shell(exe + " del regressionTest_localJobAddRef.xml");
                Exec.Shell(exe + " importdir " + dir);

                Console.WriteLine("-");
                Console.WriteLine("Testing add reference in a local job on a non-participating cluster...");

                string output = Exec.Shell(exe + " exec regressionTest_localJobAddRef.xml");
                string expected = "hi there";
                if (output.IndexOf(expected, StringComparison.OrdinalIgnoreCase) > -1)
                {
                    Console.WriteLine("[PASSED] - " + string.Join(" ", args));
                }
                else
                {
                    Console.WriteLine("[FAILED] - " + string.Join(" ", args));
                }

                //Clean up
                Exec.Shell(exe + " del regressionTest_localJobAddRef.xml");
                System.IO.Directory.Delete(dir, true);
            }
            catch(Exception e)
            {
                Console.Error.WriteLine("Error during local job add reference testing: {0}", e.Message);
            }
            finally
            {
                if (dfsback != null)
                {
                    ok = DFSUtils.UndoFileChanges(dfspath, dfsback);
                }
            }

            if (dfsback != null && ok)
            {
                System.IO.File.Delete(dfsback);
            }
        }
    }
}
