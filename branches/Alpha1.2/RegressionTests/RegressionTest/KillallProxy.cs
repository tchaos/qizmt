using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace RegressionTest
{
    class KillallProxy
    {
        public static void TestKillallProxy(string[] args)
        {
            string job = @"<SourceCode>
  <Jobs>
    <Job Name=`m_Preprocessing`>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`m_Preprocessing`>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Qizmt_Log(`B40735E5-8B8D-4638-8908-CD2AB024C3A7`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>".Replace('`', '"');

            string dir = @"\\" + System.Net.Dns.GetHostName() + @"\" + Environment.CurrentDirectory.Replace(':', '$') + @"\RegressionTest\B40735E5-8B8D-4638-8908-CD2AB024C3A7\";
            if (System.IO.Directory.Exists(dir))
            {
                System.IO.Directory.Delete(dir, true);
            }
            System.IO.Directory.CreateDirectory(dir);

            System.IO.File.WriteAllText(dir + @"B40735E5-8B8D-4638-8908-CD2AB024C3A7.xml", job);

            string exe = Exec.GetQizmtExe();
            Exec.Shell(exe + @" del B40735E5-8B8D-4638-8908-CD2AB024C3A7.xml");
            Exec.Shell(exe + @" importdir " + dir);
            System.IO.Directory.Delete(dir, true);

            bool guidfound = false;
            bool jobdone = false;
            System.Threading.Thread th = new Thread(new ThreadStart(delegate()
                {
                    try
                    {
                        Console.WriteLine("Running job...");
                        string results = Exec.Shell(exe + @" exec B40735E5-8B8D-4638-8908-CD2AB024C3A7.xml");
                        Console.WriteLine("Job output: {0}", results);
                        if (results.IndexOf("B40735E5-8B8D-4638-8908-CD2AB024C3A7") > -1)
                        {
                            guidfound = true;
                        }
                    }
                    catch
                    {
                    }
                    jobdone = true;
                }));

            th.Start();
            System.Threading.Thread.Sleep(5000);

            if (guidfound)
            {
                throw new Exception("Job exited normally.  Job error.");
            }
            if (jobdone)
            {
                throw new Exception("Job finished too early, cannot run killall to kill it.");
            }

            Console.WriteLine("Running killall proxy...");
            Exec.Shell(exe + " killall -f proxy");

            System.Threading.Thread.Sleep(2000);

            if (guidfound)
            {
                throw new Exception("Job exited normally.  Job error.");
            }
            if (!jobdone)
            {
                throw new Exception("Job didn't return.  Killall didn't kill the job.");
            }
            
            Console.WriteLine("[PASSED] - " + string.Join(" ", args));

            Exec.Shell(exe + @" del B40735E5-8B8D-4638-8908-CD2AB024C3A7.xml");
        }
    }
}
