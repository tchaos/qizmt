using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RegressionTest
{
    public class CriticalSection
    {
        private static string exe = Exec.GetQizmtExe();
        private static System.Threading.ManualResetEvent evt = new System.Threading.ManualResetEvent(false);
        private static string dumpfn = null;

        public static void TestCriticalSection(string[] args)
        {
            const int threadcount = 2;

            string dir = Environment.CurrentDirectory + @"\TestCriticalSection\";
            if (System.IO.Directory.Exists(dir))
            {
                System.IO.Directory.Delete(dir, true);
            }
            System.IO.Directory.CreateDirectory(dir);

            dir = dir.Replace(":", "$");
            dir = @"\\" + System.Net.Dns.GetHostName() + @"\" + dir;
            dumpfn = dir + "dump.txt";

            Console.WriteLine("-");
            Console.WriteLine("Testing critical section...");

            for (int i = 0; i < threadcount; i++)
            {
                string mr = (@"<SourceCode>
  <Jobs>
    <Job Name=`regression_test_CriticalSection_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regression_test_CriticalSection_Input" + i.ToString() + @".txt`);
            Shell(@`Qizmt del regression_test_CriticalSection_Output" + i.ToString() + @".txt`);
            
            using(GlobalCriticalSection.GetLock())
            {
                Increment();
            }           
        }        
        private void Increment()
        {
            System.IO.StreamReader r = new System.IO.StreamReader(Qizmt_ExecArgs[0]);
           int count = Int32.Parse(r.ReadToEnd());
           r.Close();

           count++;
           System.IO.FileStream fs = new System.IO.FileStream(Qizmt_ExecArgs[0], System.IO.FileMode.Open, System.IO.FileAccess.Write, System.IO.FileShare.None);
           byte[] buf = System.Text.Encoding.UTF8.GetBytes(count.ToString());
           fs.Write(buf, 0, buf.Length);
           fs.Close();
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`regression_test_CriticalSection_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://regression_test_CriticalSection_Input" + i.ToString() + @".txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`1498`);
                dfsoutput.WriteLine(`1503`);
                dfsoutput.WriteLine(`1501`);
                dfsoutput.WriteLine(`1501`);    
                
                using(GlobalCriticalSection.GetLock())
                {
                    Increment();
                }              
           }           
            private void Increment()
            {
                System.IO.StreamReader r = new System.IO.StreamReader(Qizmt_ExecArgs[0]);
               int count = Int32.Parse(r.ReadToEnd());
               r.Close();

               count++;
               System.IO.FileStream fs = new System.IO.FileStream(Qizmt_ExecArgs[0], System.IO.FileMode.Open, System.IO.FileAccess.Write, System.IO.FileShare.None);
               byte[] buf = System.Text.Encoding.UTF8.GetBytes(count.ToString());
               fs.Write(buf, 0, buf.Length);
               fs.Close();
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`regression_test_CriticalSection` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>4</KeyLength>
        <DFSInput>dfs://regression_test_CriticalSection_Input" + i.ToString() + @".txt</DFSInput>
        <DFSOutput>dfs://regression_test_CriticalSection_Output" + i.ToString() + @".txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                output.Add(line, line);

                using(GlobalCriticalSection.GetLock())
                {
                    Increment();
                }           
          }          
          private void Increment()
        {
            System.IO.StreamReader r = new System.IO.StreamReader(Qizmt_ExecArgs[0]);
           int count = Int32.Parse(r.ReadToEnd());
           r.Close();

           count++;
           System.IO.FileStream fs = new System.IO.FileStream(Qizmt_ExecArgs[0], System.IO.FileMode.Open, System.IO.FileAccess.Write, System.IO.FileShare.None);
           byte[] buf = System.Text.Encoding.UTF8.GetBytes(count.ToString());
           fs.Write(buf, 0, buf.Length);
           fs.Close();
        }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {                    
              for(int i = 0; i < values.Length; i++)
              {
                    output.Add(key);    
                    
                    using(GlobalCriticalSection.GetLock())
                    {
                        Increment();
                    }          
              }
          }          
          private void Increment()
        {
            System.IO.StreamReader r = new System.IO.StreamReader(Qizmt_ExecArgs[0]);
           int count = Int32.Parse(r.ReadToEnd());
           r.Close();

           count++;
           System.IO.FileStream fs = new System.IO.FileStream(Qizmt_ExecArgs[0], System.IO.FileMode.Open, System.IO.FileAccess.Write, System.IO.FileShare.None);
           byte[] buf = System.Text.Encoding.UTF8.GetBytes(count.ToString());
           fs.Write(buf, 0, buf.Length);
           fs.Close();
        }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`regression_test_CriticalSection_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del regression_test_CriticalSection_Input" + i.ToString() + @".txt`);
            Shell(@`Qizmt del regression_test_CriticalSection_Output" + i.ToString() + @".txt`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"');

                string fn = "regressionTest_criticalSection" + i.ToString() + ".xml";
                System.IO.File.WriteAllText(dir + fn, mr);
            }
            
            Exec.Shell(exe + @" del regressionTest_criticalSection*.xml");
            Exec.Shell(exe + @" importdirmt " + dir);
                        
            System.IO.File.WriteAllText(dumpfn, "0");

            System.Threading.Thread[] ths = new System.Threading.Thread[threadcount];
            for (int i = 0; i < threadcount; i++)
            {
                System.Threading.Thread th = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ThreadProc));
                ths[i] = th;
                th.Start(i);                
            }

            evt.Set();

            for (int i = 0; i < threadcount; i++)
            {
                ths[i].Join();
            }

            int result = Int32.Parse(System.IO.File.ReadAllText(dumpfn).Trim());
            if (result == threadcount * 10)
            {
                Console.WriteLine("[PASSED] - " + string.Join(" ", args));
            }
            else
            {
                Console.WriteLine("[FAILED] - " + string.Join(" ", args));
            }

            Exec.Shell(exe + @" del regressionTest_criticalSection*.xml");
            System.IO.Directory.Delete(dir, true);
        }

        private static void ThreadProc(object num)
        {
            int inum = (int)num;
            evt.WaitOne();
            Exec.Shell(exe + @" exec regressionTest_criticalSection" + inum.ToString() + ".xml \"" + dumpfn + "\"");
        } 
    }
}
