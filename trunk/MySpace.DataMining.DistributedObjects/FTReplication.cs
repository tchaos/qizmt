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

//#define TESTFAULTTOLERANT

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects5
{
    public class FTReplication
    {
        public static int FILE_BUFFER_SIZE = MySpace.DataMining.DistributedObjects.MemoryUtils.DefaultFileBufferSize;
        public static int MAX_SIZE_PER_RECEIVE = 0x400 * 64;

        internal string[] allpullfiles = null;
        internal int start = 0;
        internal int stop = 0;
        internal int ftretries = 0;
        internal long jid = 0;
        internal Dictionary<string, int> tattledhosts = null;
        internal System.Threading.Thread thread = null;
        internal Exception exception = null;

#if TESTFAULTTOLERANT
        internal MySpace.DataMining.AELight.FTTest fttest = null;
#endif

        internal void ThreadProc()
        {
            try
            {
                Random rnd = new Random(unchecked(System.DateTime.Now.Millisecond + System.Threading.Thread.CurrentThread.ManagedThreadId));
                List<PullFileInfo> infos = new List<PullFileInfo>(stop - start);
                for (int i = start; i < stop; i++)
                {
                    PullFileInfo info;
                    info.PullFile = allpullfiles[i];
                    string destfn;
                    {
                        int ix;
                        ix = info.PullFile.LastIndexOf('\u0002');
                        if (-1 != ix)
                        {
                            destfn = info.PullFile.Substring(ix + 1);
                            info.PullFile = info.PullFile.Substring(0, ix);
                        }
                        else
                        {
                            ix = info.PullFile.LastIndexOf('\\');
                            if (-1 != ix)
                            {
                                destfn = info.PullFile.Substring(ix + 1);
                            }
                            else
                            {
                                destfn = info.PullFile;
                            }
                        }
                    }
                    info.DestFile = destfn;
                    info.DestFileStream = null;
                    info.Position = 0;
                    info.Retries = 0;
                    infos.Add(info);
                }

                byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];

                for (int i = 0; i < infos.Count; i++)
                {
                    PullFileInfo info = infos[i];
                    for (; ; ) //cooking
                    {
                        bool cooking_is_read = false;

                        try
                        {
                            if (info.DestFileStream == null)
                            {
                                if (info.Retries == 0)
                                {
                                    cooking_is_read = true;

#if TESTFAULTTOLERANT
                                    {                                       
                                        int dgdel = info.PullFile.IndexOf(@"\", 2);
                                        string dghost = info.PullFile.Substring(2, dgdel - 2).ToLower();
                                        fttest.BreakPoint(dghost, "replication", new Exception("FAULTTOLERANT replication test exception"));
                                    }                                    
#endif
                                    System.IO.File.Copy(info.PullFile, info.DestFile, true);
                                    cooking_is_read = false;
                                    break; // done with this pullfile
                                }
                                else
                                {
                                    info.DestFileStream = new System.IO.FileStream(info.DestFile, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                                }
                            }

                            if (info.DestFileStream != null)
                            {
                                cooking_is_read = true;
                                using (System.IO.FileStream pullfs = new System.IO.FileStream(info.PullFile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                                {
#if TESTFAULTTOLERANT
                                    {
                                        int dgdel = info.PullFile.IndexOf(@"\", 2);
                                        string dghost = info.PullFile.Substring(2, dgdel - 2).ToLower();
                                        fttest.BreakPoint(dghost, "replication", new Exception("FAULTTOLERANT replication test exception"));
                                    }
#endif
                                    if (info.Position != 0)
                                    {
                                        pullfs.Position = info.Position;
                                    }
                                    for (; ; )
                                    {
                                        //----------------------------COOKING--------------------------------
                                        cooking_is_read = true;
                                        //----------------------------COOKING--------------------------------
                                        int xread = pullfs.Read(fbuf, 0, MAX_SIZE_PER_RECEIVE);
                                        info.Position += xread;
                                        //----------------------------COOKING--------------------------------
                                        cooking_is_read = false;
                                        //----------------------------COOKING--------------------------------
                                        if (xread <= 0)
                                        {
                                            break;
                                        }
                                        info.DestFileStream.Write(fbuf, 0, xread);
                                    }
                                    break;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (!cooking_is_read)
                            {
                                if (info.DestFileStream != null)
                                {
                                    info.DestFileStream.Close();
                                }

                                try
                                {
                                    XLog.errorlog("Non-reading error; retries=" + info.Retries.ToString() +
                                        ";puller machine=" + System.Net.Dns.GetHostName() +
                                        ";pulling file=" + info.PullFile +
                                        ";error=" + e.ToString());
                                }
                                catch
                                {
                                }

                                throw;
                            }

                            if (++info.Retries > ftretries)
                            {
                                if (info.DestFileStream != null)
                                {
                                    info.DestFileStream.Close();
                                }

                                //Report this host with corrupt file
                                int del = info.PullFile.IndexOf(@"\", 2);
                                string badhost = info.PullFile.Substring(2, del - 2);
                                RogueHosts.Add(jid, badhost);
                                lock (tattledhosts)
                                {
                                    tattledhosts[badhost] = 1;
                                }

                                try
                                {
                                    XLog.errorlog("Retries exceeded ftretries; retries=" + info.Retries.ToString() +
                                      ";ftretries=" + ftretries.ToString() +
                                      ";puller machine=" + System.Net.Dns.GetHostName() +
                                      ";pulling file=" + info.PullFile +
                                      ";tattled host=" + badhost);
                                }
                                catch
                                {
                                }

                                goto next_pullfile;
                            }

                            //Random swap with the rest of the list of pullfiles
                            int iswapwith = rnd.Next(i, infos.Count);
                            infos[i] = infos[iswapwith];
                            infos[iswapwith] = info;
                            info = infos[i];
                            continue;
                        }
                    }
                next_pullfile:
                    int xxx = 10;
                }
            }
            catch(Exception ex)
            {
                exception = ex;
            }            
        }

        struct PullFileInfo
        {
            public string PullFile;
            public string DestFile;
            public System.IO.FileStream DestFileStream;
            public long Position;
            public int Retries;
        }
    }
}
