using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RegressionTest
{
    public class IOCook
    {
        public static void TestIOCook(string[] args)
        {
            int writecnt = 2;
            int readcnt = 3;
            string curdir = Environment.CurrentDirectory;
            string writedir = curdir + @"\TestIOCook\write\";
            string readdir = curdir + @"\TestIOCook\read\";
            bool readexisting = false;
            bool writeexisting = false;

            for(int i = 1; i < args.Length; i++)
            {
                string arg = args[i];
                string optname = arg;
                string optval = "";
                int del = arg.IndexOf('=');
                if (del > -1)
                {
                    optname = arg.Substring(0, del);
                    optval = arg.Substring(del + 1).ToLower();
                }
                optname = optname.ToLower();

                switch (optname)
                {
                    case "writecount":
                        writecnt = Int32.Parse(optval);
                        break;
                    case "readcount":
                        readcnt = Int32.Parse(optval);
                        break;
                    case "writedir":
                        writedir = optval;
                        break;
                    case "readdir":
                        readdir = optval;
                        break;
                    case "readexisting":
                        if (optval == "1" || optval == "true")
                        {
                            readexisting = true;
                        }
                        else
                        {
                            readexisting = false;
                        }
                        break;
                    case "writeexisting":
                        if (optval == "1" || optval == "true")
                        {
                            writeexisting = true;
                        }
                        else
                        {
                            writeexisting = false;
                        }
                        break;
                    default:
                        Console.Error.WriteLine("Invalid argument");
                        return;
                }
            }

            if (writedir[writedir.Length - 1] != '\\')
            {
                writedir = writedir + @"\";
            }
            if (readdir[readdir.Length - 1] != '\\')
            {
                readdir = readdir + @"\";
            }
                        
            System.Threading.Thread[] readths = null;
            System.Threading.Thread[] writeths = null;
            Worker[] readworkers = null;
            Worker[] writeworkers = null;
            System.Threading.ManualResetEvent evt = new System.Threading.ManualResetEvent(false);

            //Read    
            if(readcnt > 0)
            {
                if (!System.IO.Directory.Exists(readdir))
                {
                    System.IO.Directory.CreateDirectory(readdir);
                }

                string[] readfns = null;
                if (readexisting)
                {
                    readfns = System.IO.Directory.GetFiles(readdir);
                    if (readfns.Length < readcnt)
                    {
                        Console.Error.WriteLine("Not enough files to read from: You have {0} files.  Need {1} files.", readfns.Length, readcnt);
                        return;
                    }
                }
                else
                {
                    Console.WriteLine("-");
                    Console.WriteLine("Preparing files for reading...");

                    int byteremain = 1024 * 1024 * 1; 
                    string seed = readdir + Guid.NewGuid().ToString() + ".txt";
                    byte[] buf = new byte[100];
                    System.IO.FileStream fseed = new System.IO.FileStream(seed, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read);
                    while (byteremain > 0)
                    {
                        fseed.Write(buf, 0, buf.Length);
                        byteremain -= buf.Length;
                    }
                    fseed.Close();     

                    readfns = new string[readcnt];
                    readfns[0] = seed;
                    for (int i = 1; i < readcnt; i++)
                    {
                        string fn = readdir + Guid.NewGuid().ToString() + ".txt";
                        readfns[i] = fn;
                        System.IO.File.Copy(seed, fn);
                    }

                    Console.WriteLine("-");
                    Console.WriteLine("Done preparing files for reading...");
                }

                readworkers = new Worker[readcnt];
                readths = new System.Threading.Thread[readcnt];
                Console.WriteLine("-");
                Console.WriteLine("Creating threads for reading...");
                for (int i = 0; i < readcnt; i++)
                {                    
                    Worker worker = new Worker();
                    worker.ID = i;
                    worker.FilePath = readfns[i];
                    worker.Evt = evt;
                    readworkers[i] = worker;
                    System.Threading.Thread th = new System.Threading.Thread(new System.Threading.ThreadStart(worker.ReadThreadProc));
                    readths[i] = th;
                    th.Start();
                }
                Console.WriteLine("-");
                Console.WriteLine("Done creating threads for reading...");
            }

            //Write
            if(writecnt > 0)
            {
                if (!System.IO.Directory.Exists(writedir))
                {
                    System.IO.Directory.CreateDirectory(writedir);
                }

                string[] writefns = null;
                if (writeexisting)
                {
                    writefns = System.IO.Directory.GetFiles(writedir);
                    if (writefns.Length < writecnt)
                    {
                        Console.Error.WriteLine("Not enough files to write to.  You have {0} files.  Need {1} files.", writefns.Length, writecnt);
                        return;
                    }
                }
                else
                {
                    Console.WriteLine("-");
                    Console.WriteLine("Preparing files for writing...");
                    byte[] buf = new byte[0];                   
                    writefns = new string[writecnt];
                    for (int i = 0; i < writecnt; i++)
                    {
                        string fn = writedir + Guid.NewGuid().ToString() + ".txt";
                        writefns[i] = fn;
                        System.IO.File.WriteAllBytes(fn, buf);                       
                    }
                    Console.WriteLine("-");
                    Console.WriteLine("Done preparing files for writing...");
                }                
               
                writeworkers = new Worker[writecnt];
                writeths = new System.Threading.Thread[writecnt];
                Console.WriteLine("-");
                Console.WriteLine("Creating threads for writing...");
                for (int i = 0; i < writecnt; i++)
                {                    
                    Worker worker = new Worker();
                    worker.ID = i;
                    worker.FilePath = writefns[i];
                    worker.Evt = evt;
                    writeworkers[i] = worker;
                    System.Threading.Thread th = new System.Threading.Thread(new System.Threading.ThreadStart(worker.WriteThreadProc));
                    writeths[i] = th;
                    th.Start();
                }
                Console.WriteLine("-");
                Console.WriteLine("Done creating threads for writing...");
            }
            
            evt.Set();
            Console.WriteLine("-");
            Console.WriteLine("Event set.  Waiting for all threads to complete...");
            if (readths != null)
            {
                for (int i = 0; i < readths.Length; i++)
                {
                    readths[i].Join();
                }
            }
            if (writeths != null)
            {
                for (int i = 0; i < writeths.Length; i++)
                {
                    writeths[i].Join();
                }
            }

            Console.WriteLine("-");
            Console.WriteLine("IOCook tests completed.");
                        
            if (readworkers != null)
            {
                Console.WriteLine("-");
                Console.WriteLine("Read results:");

                int readerr = 0;
                foreach (Worker worker in readworkers)
                {                    
                    readerr += worker.ErrMsg.Count;
                    foreach (string msg in worker.ErrMsg)
                    {
                        Console.WriteLine(msg);
                    }
                }

                Console.WriteLine("-");
                Console.WriteLine("Total read errors: {0}", readerr);
            } 
           
            if (writeworkers != null)
            {
                Console.WriteLine("-");
                Console.WriteLine("Write results:");

                int writeerr = 0;
                foreach (Worker worker in writeworkers)
                {
                    writeerr += worker.ErrMsg.Count;
                    foreach (string msg in worker.ErrMsg)
                    {
                        Console.WriteLine(msg);
                    }
                }

                Console.WriteLine("-");
                Console.WriteLine("Total write errors: {0}", writeerr);
            }            

            int xxxx = 0;
        }

        public class Worker
        {
            public int ID = -1;
            public string FilePath = null;
            public List<string> ErrMsg = new List<string>();
            public System.Threading.ManualResetEvent Evt = null;

            public void ReadThreadProc()
            {               
                System.IO.FileStream fs = null;
                Evt.WaitOne();
                try
                {
                    fs = new System.IO.FileStream(FilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
                    int ib = 0;
                    while ((ib = fs.ReadByte()) > -1)
                    {
                    }
                    Console.WriteLine(ID);
                }
                catch (Exception e)
                {
                    ErrMsg.Add(e.Message);
                }
                finally
                {
                    if (fs != null)
                    {
                        fs.Close();
                        fs = null;
                    }
                }
            }

            public void WriteThreadProc()
            {
                System.IO.FileStream fs = null;
                byte[] buf = new byte[100];
                int byteremain = 1024 * 1024 * 1;

                Evt.WaitOne();
                try
                {
                    fs = new System.IO.FileStream(FilePath, System.IO.FileMode.Open, System.IO.FileAccess.Write, System.IO.FileShare.Read);
                    while (byteremain > 0)
                    {
                        fs.Write(buf, 0, buf.Length);
                        byteremain -= buf.Length;
                    }
                    Console.WriteLine(ID);
                }
                catch (Exception e)
                {
                    ErrMsg.Add(e.Message);
                }
                finally
                {
                    if (fs != null)
                    {
                        fs.Close();
                        fs = null;
                    }
                }
            }
        }
    }
}
