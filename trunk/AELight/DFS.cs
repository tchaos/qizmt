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
using System.Runtime.InteropServices;

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {

        internal static readonly string DFSXMLPATH = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) + @"\" + dfs.DFSXMLNAME;
        private static readonly string invalidatedCacheToken = "7985b994-5a01-41dd-a3ef-ebd508ecaa78";


        public static dfs LoadDfsConfig(IList<string> applyxpath)
        {
            using (LockDfsMutex())
            {
                return dfs.ReadDfsConfig_unlocked(DFSXMLPATH, applyxpath);
            }
        }

        public static dfs LoadDfsConfig()
        {
            using (LockDfsMutex())
            {
                return dfs.ReadDfsConfig_unlocked(DFSXMLPATH);
            }
        }

#if NOLONGERUSE
        public static string[] GetClusterMachines()
        {
            string master = Surrogate.MasterHost;
            string dfsxml = Surrogate.NetworkPathForHost(master) + @"\" + dfs.DFSXMLNAME;
            string[] slaves = null;

            using (LockDfsMutex())
            {
                dfs dc = dfs.ReadDfsConfig_unlocked(dfsxml);
                slaves = dc.Slaves.SlaveList.Split(';');
            }

            List<string> machines = new List<string>();
            bool masterInList = false;

            foreach (string slave in slaves)
            {
                if (string.Compare(master, slave, true) == 0)
                {
                    masterInList = true;
                }

                machines.Add(System.Net.Dns.GetHostEntry(slave).HostName);
            }

            if (!masterInList)
            {
                machines.Add(System.Net.Dns.GetHostEntry(master).HostName);
            }

            return machines.ToArray();
        }
#endif

        public static dfs.DfsFile DfsFindAny(dfs dc, string dfspath)
        {
            return dc.FindAny(dfspath);
        }

        public static dfs.DfsFile.FileNode DfsFindFileNode(dfs dc, string nodename, out string ownerfilename)
        {
            return dc.FindFileNode(nodename, out ownerfilename);
        }

        public static dfs.DfsFile DfsFind(dfs dc, string dfspath, string type)
        {
            return dc.Find(dfspath, type);
        }

        public static dfs.DfsFile DfsFind(dfs dc, string dfspath)
        {
            return dc.Find(dfspath);
        }


        static string DfsCreateNewBaseFileName(dfs dc, string dfspath)
        {
            if (null != DfsFindAny(dc, dfspath))
            {
                throw new Exception("File already exists in DFS: " + dfspath);
            }
            return GenerateZdFileDataNodeName(dfspath);
        }


        /*
        static void DfsAllSlaves(dfs dc)
        {
            string[] slaves = dc.Slaves.SlaveList.Split(',', ';');
            if (null == dc.Slaves.SlaveList || dc.Slaves.SlaveList.Length == 0 || slaves.Length < 1)
            {
                throw new Exception("SlaveList expected in configuration");
            }
            for (int si = 0; si < slaves.Length; si++)
            {
                //SlaveHost = slaves[si];
                //SlaveIP = IPAddressUtil.GetClosestAddress(SlaveHost);
                // ///
            }
        }
         */


        static void _UpdateDfsXml_retryexception(string action, Exception e)
        {
            {
                try
                {
                    System.IO.File.AppendAllText("DFS.XML.EXCEPTION",
                        "[" + DateTime.Now.ToString() + "] {" + action + "} "
                        + e.ToString()
                        + Environment.NewLine
                        + "-----------------------"
                        + Environment.NewLine);
                    System.Threading.Thread.Sleep(1000 * 2);
                }
                catch(Exception eludxe)
                {
                    throw new Exception("Exception '" + eludxe.Message + "' while logging update-dfs.xml {" + action + "} exception: " + e.Message, e);
                }
            }
        }

        protected static void UpdateDfsXml(dfs dc)
        {
            UpdateDfsXml(dc, DFSXMLPATH, dc.GetMetaBackupLocation());
        }

        protected static void UpdateDfsXml(dfs dc, string dfsxmlpath, string backupdir)
        {
            using (LockDfsMutex())
            {
                if (dc.UserModified)
                {
                    throw new NotSupportedException("Cannot save user-modified " + dfs.DFSXMLNAME);
                }

                string oldpath = dfsxmlpath + ".old";
                string newpath = dfsxmlpath + ".new";

                //+++metabackup+++
                string backupfile = null, oldbackupfile = null, newbackupfile = null;
                if (null != backupdir)
                {
                    backupfile = backupdir + @"\dfs-" + Surrogate.SafeTextPath(System.Net.Dns.GetHostName()) + @".xml";
                    oldbackupfile = backupfile + ".old";
                    newbackupfile = backupfile + ".new";
                }
                //---metabackup---

                // Write .new
                for (; ; )
                {
                    try
                    {
                        using (System.IO.FileStream fdc = new System.IO.FileStream(newpath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None))
                        {
                            System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(dfs));
                            //xs.Serialize(fdc, dc);
                            System.Xml.XmlTextWriter xw = new System.Xml.XmlTextWriter(fdc, null); // null=UTF-8
#if DEBUG
                            xw.Formatting = System.Xml.Formatting.Indented;
#else
                            xw.Formatting = System.Xml.Formatting.None;
#endif
                            xs.Serialize(xw, dc);
                        }
                        //+++metabackup+++
                        if (null != backupdir)
                        {
                            System.IO.File.Copy(newpath, newbackupfile, true);
                        }
                        //---metabackup---
                        break;
                    }
                    catch (System.IO.IOException e)
                    {
                        _UpdateDfsXml_retryexception("write-new", e);
                        continue;
                    }
                }

                // Move current to .old
                for (; ; )
                {
                    try
                    {
                        try
                        {
                            System.IO.File.Delete(oldpath);
                        }
                        catch
                        {
                        }
                        // Check if dfs.xml exists first, because this might be the first save on format.
                        if (System.IO.File.Exists(dfsxmlpath)) // Not using dfs.DfsConfigExists()!
                        {
                            System.IO.File.Move(dfsxmlpath, oldpath);
                        }
                        break;
                    }
                    catch (System.IO.IOException e)
                    {
                        _UpdateDfsXml_retryexception("move-old", e);
                        continue;
                    }
                }
                //+++metabackup+++
                if (null != backupdir)
                {
                    for (; ; )
                    {
                        try
                        {
                            try
                            {
                                System.IO.File.Delete(oldbackupfile);
                            }
                            catch
                            {
                            }
                            // See if backup dfs.xml exists first, because metabackup might not have been on.
                            if (System.IO.File.Exists(backupfile))
                            {
                                System.IO.File.Move(backupfile, oldbackupfile);
                            }
                            break;
                        }
                        catch (System.IO.IOException e)
                        {
                            _UpdateDfsXml_retryexception("metabackup-move-old", e);
                            continue;
                        }
                    }
                }
                //---metabackup---

                // Move .new to current.
                for (; ; )
                {
                    try
                    {
                        System.IO.File.Move(newpath, dfsxmlpath);
                        break;
                    }
                    catch (System.IO.IOException e)
                    {
                        _UpdateDfsXml_retryexception("move-new", e);
                        continue;
                    }
                }
                //+++metabackup+++
                if (null != backupdir)
                {
                    for (; ; )
                    {
                        try
                        {
                            System.IO.File.Move(newbackupfile, backupfile);
                            break;
                        }
                        catch (System.IO.IOException e)
                        {
                            _UpdateDfsXml_retryexception("metabackup-move-new", e);
                            continue;
                        }
                    }
                }
                //---metabackup---

                // Delete .old
                try
                {
                    System.IO.File.Delete(oldpath);
                }
                catch
                {
                }
                //+++metabackup+++
                if (null != backupdir)
                {
#if DEBUG
                    //System.Diagnostics.Debugger.Launch();
#endif
                    try
                    {
                        if (System.IO.File.Exists(oldbackupfile))
                        {
                            long lastprevid;
                            System.IO.FileInfo bfi = new System.IO.FileInfo(backupfile);
                            {
                                System.IO.FileInfo[] prevfiles = bfi.Directory.GetFiles(bfi.Name + ".*.prev");
                                Func<string, long> OrdFromXmlPrev = delegate(string fp)
                                    {
                                        int d = fp.LastIndexOf('.', fp.Length - 5 - 1) + 1;
                                        string snum = fp.Substring(d, (fp.Length - 5) - d);
                                        return long.Parse(snum);
                                    };
                                Array.Sort<System.IO.FileInfo>(prevfiles, new Comparison<System.IO.FileInfo>(
                                    delegate(System.IO.FileInfo fi1, System.IO.FileInfo fi2)
                                    {
                                        long ord1 = OrdFromXmlPrev(fi1.ToString());
                                        long ord2 = OrdFromXmlPrev(fi2.ToString());
                                        if (ord1 < ord2)
                                        {
                                            return -1;
                                        }
                                        if (ord1 > ord2)
                                        {
                                            return +1;
                                        }
                                        return 0;
                                    }));
                                const int DFSXMLPREVMAXCOUNT = 10;
                                for (int i = 0; i < prevfiles.Length - (DFSXMLPREVMAXCOUNT - 1); i++)
                                {
                                    try
                                    {
                                        System.IO.File.Delete(prevfiles[i].FullName);
                                    }
                                    catch
                                    {
                                    }
                                }
                                if (prevfiles.Length > 0)
                                {
                                    lastprevid = OrdFromXmlPrev(prevfiles[prevfiles.Length - 1].ToString());
                                }
                                else
                                {
                                    lastprevid = 0;
                                }
                            }

                            long newprevid;
                            if (lastprevid == long.MaxValue)
                            {
                                newprevid = 1;
                            }
                            else
                            {
                                newprevid = lastprevid + 1;
                            }
                            System.IO.File.Move(oldbackupfile, bfi.FullName + "." + newprevid + ".prev");

                        }
                    }
                    catch
                    {
                    }
                    try
                    {
                        System.IO.File.Delete(oldbackupfile);
                    }
                    catch
                    {
                    }
                }
                //---metabackup---

            }
        }


        static IDisposable LockDfsMutex()
        {
            return dfs.LockDfsMutex();
        }

        static int _DfsGetBinaryWildCard(string dfspath, string targetFolder)
        {
            int filesCount = 0;
            string srex = Surrogate.WildcardRegexString(dfspath);
            System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            {
                dfs dc = LoadDfsConfig();
                foreach (dfs.DfsFile df in dc.Files)
                {
                    if (rex.IsMatch(df.Name))
                    {
                        filesCount += _DfsGetBinaryNoWildCard(df.Name, targetFolder);
                    }
                }
            }
            return filesCount;
        }

        static int _DfsGetBinaryNoWildCard(string dfspath, string targetFolder)
        {
            if (!targetFolder.EndsWith(@"\"))
            {
                targetFolder += @"\";
            }

            if (!dfs.DfsConfigExists(DFSXMLPATH))
            {
                Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                SetFailure();
                return 0;
            }

            if (dfspath.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                dfspath = dfspath.Substring(6);
            }

            if (dfspath.Length == 0)
            {
                Console.Error.WriteLine("Invalid dfspath");
                SetFailure();
                return 0;
            }

            EnsureNetworkPath(targetFolder);
            if (!System.IO.Directory.Exists(targetFolder))
            {
                Console.Error.WriteLine("Target folder is not found: {0}", targetFolder);
                return 0;
            }

            dfs dc = LoadDfsConfig();
            dfs.DfsFile dfsf = DfsFind(dc, dfspath);
            if (null == dfsf)
            {
                Console.Error.WriteLine("Error:  The specified file '{0}' does not exist in DFS", dfspath);
                SetFailure();
                return 0;
            }

            int blobPadSize = MySpace.DataMining.DistributedObjects.Blob.padSize;
            const int MAX_SIZE_PER_RECEIVE = 0x400 * 16 * 4;
            int filesCount = 0;
            byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];

            System.Security.Principal.NTAccount nt = new System.Security.Principal.NTAccount(userdomain, dousername);
            System.Security.Principal.SecurityIdentifier secID = (System.Security.Principal.SecurityIdentifier)nt.Translate(typeof(System.Security.Principal.SecurityIdentifier));
            System.Security.AccessControl.FileSystemAccessRule rule = new System.Security.AccessControl.FileSystemAccessRule(secID,
            System.Security.AccessControl.FileSystemRights.FullControl,
            System.Security.AccessControl.AccessControlType.Allow);

            List<string> perrors = new List<string>();

            foreach (dfs.DfsFile.FileNode node in dfsf.Nodes)
            {
                bool eof = false;
                bool newline = false;
                System.IO.Stream fs = null;
                System.IO.FileStream ft = null;

                try
                {
                    fs = new DfsFileNodeStream(node, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                    {
                        int xhead = StreamReadLoop(fs, fbuf, 4);
                        if (4 == xhead)
                        {
                            int hlen = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(fbuf);
                            StreamReadExact(fs, fbuf, hlen - 4);
                        }
                    }

                    while (!eof)
                    {
                        if (newline)
                        {
                            if (ft != null)
                            {
                                ft.Close();
                                ft = null;
                            }
                        }

                        int xread = 0;
                        while (xread < MAX_SIZE_PER_RECEIVE)
                        {
                            newline = false;
                            int ib = fs.ReadByte();
                            if (ib == -1)
                            {
                                eof = true;
                                break;
                            }

                            if ('\n' == ib)
                            {
                                newline = true;
                                break;
                            }

                            if (ib != '\r')
                            {
                                fbuf[xread++] = (byte)ib;
                            }
                        }

                        string s = System.Text.Encoding.UTF8.GetString(fbuf, 0, xread);

                        if (s.Length > 0)
                        {
                            if (ft == null)
                            {
                                string targetPath = targetFolder + s.Substring(0, blobPadSize).Trim();
                                s = s.Substring(260);
                                if (System.IO.File.Exists(targetPath))
                                {
                                    Console.Error.WriteLine("Warning: Local file overwritten: {0}", targetPath);
                                }
                                ft = new System.IO.FileStream(targetPath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                                filesCount++;
                                try
                                {
                                    System.Security.AccessControl.FileSecurity fsec = new System.Security.AccessControl.FileSecurity();
                                    fsec.AddAccessRule(rule);
                                    System.IO.File.SetAccessControl(targetPath, fsec);
                                }
                                catch (Exception e)
                                {
                                    perrors.Add(e.ToString());
                                }
                            }

                            byte[] data = Convert.FromBase64String(s);
                            ft.Write(data, 0, data.Length);
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("Error: {0}", e.Message);
                }
                finally
                {
                    if (ft != null)
                    {
                        ft.Close();
                        ft = null;
                    }

                    if (fs != null)
                    {
                        fs.Close();
                        fs = null;
                    }
                }
            }

            if (perrors.Count > 0)
            {
                Console.Error.WriteLine("Error while assigning file permission to: {0}\\{1}", userdomain, dousername);
                foreach (string perr in perrors)
                {
                    Console.Error.WriteLine(perr);
                }
            }
            return filesCount;
        }

        static void DfsGetBinary(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("getbinary error:  {0} getbinary <dfsname> <localdir>", appname);
                SetFailure();
                return;
            }
                        
            string dfspath = args[0];
            string targetFolder = args[1];
            int filesCount = 0;

            if (dfspath.IndexOfAny(wcchars) > -1)
            {
                filesCount = _DfsGetBinaryWildCard(dfspath, targetFolder);
            }
            else
            {
                filesCount = _DfsGetBinaryNoWildCard(dfspath, targetFolder);
            }

            Console.WriteLine();
            Console.WriteLine("{0} files written to: {1}", filesCount, targetFolder);
        }
      
        static void DfsPutBinary(string[] args)
        {
            if (!dfs.DfsConfigExists(DFSXMLPATH))
            {
                Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                SetFailure();
                return;
            }            

            if (args.Length < 2)
            {
                Console.Error.WriteLine("putbinary error:  {0} putbinary <wildcard> <dfsname>", appname);
                SetFailure();
                return;
            }

            string localpath = args[0];            
            int del = localpath.LastIndexOf(@"\");
            string dir = localpath.Substring(0, del);

            if (!System.IO.Directory.Exists(dir))
            {
                Console.Error.WriteLine("Directory not found: {0}", dir);
                SetFailure();
                return;
            }

            string wildcard = localpath.Substring(del + 1);
            System.IO.DirectoryInfo di = new System.IO.DirectoryInfo(dir);
            System.IO.FileInfo[] files = di.GetFiles(wildcard);

            if (files.Length == 0)
            {
                Console.Error.WriteLine("No files found in directory {0} matching wildcard {1}", dir, wildcard);
                SetFailure();
                return;
            }

            string dfspath = args[1];
            if (dfspath.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                dfspath = dfspath.Substring(6);
            }
            string reason = "";            
            if (dfs.IsBadFilename(dfspath, out reason))
            {
                Console.Error.WriteLine("Invalid dfspath: {0}", reason);
                SetFailure();
                return;
            }

            dfs dc = LoadDfsConfig();

            for (int i = 0; i < dc.Files.Count; i++)
            {
                if (0 == string.Compare(dc.Files[i].Name, dfspath, true))
                {
                    Console.Error.WriteLine("Error:  The specified file already exists in DFS: {0}", dfspath);
                    SetFailure();
                    return;
                }
            }           

            string[] slaves = dc.Slaves.SlaveList.Split(',', ';');
            if (null == dc.Slaves.SlaveList || dc.Slaves.SlaveList.Length == 0 || slaves.Length < 1)
            {
                Console.Error.WriteLine("SlaveList expected in configuration (no machines)");
                SetFailure();
                return;
            }
            if (dc.Replication > 1)
            {
                slaves = ExcludeUnhealthySlaveMachines(slaves, true).ToArray();
            }
            if (0 == slaves.Length)
            {
                Console.Error.WriteLine("No healthy machines for DFS putbinary");
                SetFailure();
                return;
            }

            int blobPadSize = MySpace.DataMining.DistributedObjects.Blob.padSize;
            string nl = Environment.NewLine;
            byte[] nbuf = System.Text.Encoding.UTF8.GetBytes(nl);
            long MAX_IMAGE_SIZE = dc.DataNodeBaseSize - blobPadSize * 4 - nbuf.Length;
            string sMAX_IMAGE_SIZE = AELight.GetFriendlyByteSize(MAX_IMAGE_SIZE);
            const int MAX_SIZE_PER_RECEIVE = 0x400 * 21 * 3;            
            long sampledist = dc.DataNodeBaseSize / dc.DataNodeSamples;            

            Random rnd = new Random((DateTime.Now.Millisecond / 2) + (System.Diagnostics.Process.GetCurrentProcess().Id / 2));
            List<dfs.DfsFile.FileNode> ninfos = new List<dfs.DfsFile.FileNode>(64);
            int nextslave = rnd.Next() % slaves.Length;
            long curbytepos = 0;       
            long nextsamplepos = 0;

            byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];
            byte[] sbuf = new byte[(MAX_SIZE_PER_RECEIVE / 3) * 4];            

            int fi = 0;            
            while(fi < files.Length)
            {
                string SlaveHost = slaves[nextslave];
                if (++nextslave >= slaves.Length)
                {
                    nextslave = 0;
                }
                string netdir = NetworkPathForHost(SlaveHost);
                string chunkname = GenerateZdFileDataNodeName(dfspath);
                string chunkpath = netdir + @"\" + chunkname;
                string samplepath = netdir + @"\" + chunkname + ".zsa";
                long chunkremain = dc.DataNodeBaseSize;
                long chunkpos = curbytepos;

                System.IO.FileStream fc = new System.IO.FileStream(chunkpath, System.IO.FileMode.CreateNew, System.IO.FileAccess.Write, System.IO.FileShare.None, FILE_BUFFER_SIZE);
                System.IO.FileStream fsa = new System.IO.FileStream(samplepath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None);

                MySpace.DataMining.DistributedObjects.Entry.ToBytes(4 + 8, fbuf, 0); // Size of header.
                MySpace.DataMining.DistributedObjects.Entry.LongToBytes(chunkpos, fbuf, 4);
                fc.Write(fbuf, 0, 4 + 8);

                while (fi < files.Length)
                {
                    System.IO.FileInfo file = files[fi];

                    long b64Len = file.Length / 3L;
                    if (b64Len * 3L < file.Length)
                    {
                        b64Len++;
                    }
                    b64Len *= 4;

                    if (b64Len >= MAX_IMAGE_SIZE)
                    {
                        Console.Error.WriteLine("Cannot put file.  File exceeds size limit of {0}: {1}", sMAX_IMAGE_SIZE, file.Name);
                        fi++;
                        continue;
                    }

                    if (b64Len > chunkremain)
                    {
                        break;
                    }

                    System.IO.FileStream fs = new System.IO.FileStream(file.FullName, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);

                    {
                        string fname = file.Name;
                        if (fname.Length < blobPadSize)
                        {
                            fname = fname.PadRight(blobPadSize);
                        }
                        else if (fname.Length > blobPadSize)
                        {
                            fname = fname.Substring(0, blobPadSize);
                        }
                        int bc = System.Text.Encoding.UTF8.GetBytes(fname, 0, fname.Length, fbuf, 0);
                        fc.Write(fbuf, 0, bc);
                        chunkremain -= bc;
                        curbytepos += bc;
                    }

                    int xread = 0;
                    for (; ; )
                    {
                        xread = fs.Read(fbuf, 0, MAX_SIZE_PER_RECEIVE);

                        if (xread <= 0)
                        {                            
                            break;
                        }

                        string s = Convert.ToBase64String(fbuf, 0, xread);
                        int bc = System.Text.Encoding.UTF8.GetBytes(s, 0, s.Length, sbuf, 0);
                        fc.Write(sbuf, 0, bc);
                        chunkremain -= bc;
                        curbytepos += bc;

                        if (chunkpos >= nextsamplepos)
                        {
                            fsa.Write(sbuf, 0, bc);
                            nextsamplepos += sampledist;
                        }
                    }                    

                    {
                        fc.Write(nbuf, 0, nbuf.Length);
                        chunkremain -= nbuf.Length;
                        curbytepos += nbuf.Length;
                    }

                    fs.Close();
                    fi++;
                }

                fc.Close();
                fsa.Close();

                {
                    dfs.DfsFile.FileNode fnode = new dfs.DfsFile.FileNode();
                    fnode.Host = SlaveHost;
                    fnode.Position = chunkpos;
                    fnode.Length = curbytepos - chunkpos;
                    fnode.Name = chunkname;
                    ninfos.Add(fnode);
                }
            }

            string dfspathreplicating = ".$" + dfspath + ".$replicating-" + Guid.NewGuid().ToString();
            using (LockDfsMutex()) // Needed: change between load & save should be atomic.
            {
                dc = LoadDfsConfig(); // Reload in case of changes during put.
                if (null != dc.FindAny(dfspathreplicating))
                {
                    Console.Error.WriteLine("Error: file exists: file put into DFS from another location during put: " + dfspathreplicating);
                    SetFailure();
                    return;
                }

                dfs.DfsFile dfsfile = new dfs.DfsFile();
                dfsfile.Nodes = ninfos;
                dfsfile.Name = dfspathreplicating;
                dfsfile.Size = curbytepos;
                dc.Files.Add(dfsfile);
                UpdateDfsXml(dc);
            }
            ReplicationPhase(dfspathreplicating, true, 0, slaves);
            using (LockDfsMutex()) // Needed: change between load & save should be atomic.
            {
                dc = LoadDfsConfig(); // Reload in case of changes during put.
                dfs.DfsFile dfu = dc.FindAny(dfspathreplicating);
                if (null != dfu)
                {
                    if (null != DfsFindAny(dc, dfspath))
                    {
                        Console.Error.WriteLine("Error: file exists: file put into DFS from another location during put");
                        SetFailure();
                        return;
                    }
                    dfu.Name = dfspath;
                    UpdateDfsXml(dc);
                }
            }

            Console.WriteLine("Sent {0} bytes to file dfs://{1}", curbytepos, dfspath);
        }

        static void DfsPut(string[] args)
        {
            DfsPut(args, 0x400 * 64);
        }

        static void DfsCopyTo(string[] args)
        {
            if (args.Length <= 1)
            {
                Console.Error.WriteLine("copyto usage: <dfs-file> <target-host> [<target-dfs-filename>]");
                SetFailure();
                return;
            }
            string srcdfsname = args[0];
            if (-1 != srcdfsname.IndexOf('@'))
            {
                Console.Error.WriteLine("Do not specify record length of source DFS file name: " + srcdfsname);
                SetFailure();
                return;
            }
            if (srcdfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                srcdfsname = srcdfsname.Substring(6);
            }
            string targethost = args[1];
            string targetdfsname = srcdfsname;
            if (args.Length > 2)
            {
                targetdfsname = args[2];
                if (-1 != targetdfsname.IndexOf('@'))
                {
                    Console.Error.WriteLine("Do not specify record length of target DFS file name: " + targetdfsname);
                    SetFailure();
                    return;
                }
            }
            if (targetdfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                targetdfsname = targetdfsname.Substring(6);
            }
            dfs dc = LoadDfsConfig();
            IList<dfs.DfsFile.FileNode> fputfilenodes;
            int RecordLength;
            {
                dfs.DfsFile df = dc.FindAny(srcdfsname);
                if (null == df)
                {
                    Console.Error.WriteLine("DFS file '" + srcdfsname + "' not found");
                    SetFailure();
                    return;
                }
                if (0 != string.Compare(DfsFileTypes.NORMAL, df.Type, StringComparison.OrdinalIgnoreCase)
                    && 0 != string.Compare(DfsFileTypes.BINARY_RECT, df.Type, StringComparison.OrdinalIgnoreCase))
                {
                    Console.Error.WriteLine("Cannot copyto DFS file '" + srcdfsname + "' of type " + df.Type);
                    SetFailure();
                    return;
                }
                fputfilenodes = df.Nodes;
                RecordLength = df.RecordLength;
            }
            string fputfilesfilename = MySpace.DataMining.DistributedObjects.IOUtils.GetTempDirectory()
                + @"\copyto_" + Guid.NewGuid().ToString() + ".txt";
            try
            {
                List<string> unhealthysrcmachines = new List<string>();
                Dictionary<string, int> uniqueSrcHosts = new Dictionary<string, int>();
                using (System.IO.StreamWriter sw = new System.IO.StreamWriter(fputfilesfilename))
                {
                    foreach (dfs.DfsFile.FileNode fn in fputfilenodes)
                    {
                        bool foundhost = false;
                        foreach (string host in fn.Host.Split(';'))
                        {
                            string thissrchost = host.ToUpper();
                            if (!uniqueSrcHosts.ContainsKey(thissrchost))
                            {
                                if (unhealthysrcmachines.Contains(thissrchost))
                                {
                                    continue;
                                }
                                if (!Surrogate.IsHealthySlaveMachine(thissrchost))
                                {
                                    unhealthysrcmachines.Add(thissrchost);
                                    continue;
                                }
                                uniqueSrcHosts.Add(thissrchost, 0);
                            }
                            foundhost = true;
                            sw.WriteLine(@"{0}\{1}", NetworkPathForHost(thissrchost), fn.Name);
                            break;
                        }
                        if (!foundhost)
                        {
                            Console.Error.WriteLine("Too many unhealthy machines");
                            SetFailure();
                            return;
                        }
                    }
                }
                Shell((appname + " `@=" + targethost + "` fput `files=@" + fputfilesfilename
                    + "` mode=dfschunks `dfsfilename=" + targetdfsname + "`").Replace('`', '"'));
                {
                    string[] sout = Shell((appname + " `@=" + targethost
                        + "` ls `" + targetdfsname + "`").Replace('`', '"')).Split(new char[] { '\n', '\r' },
                        StringSplitOptions.RemoveEmptyEntries);
                    if (sout.Length < 1 || -1 == sout[0].IndexOf(targetdfsname, StringComparison.OrdinalIgnoreCase))
                    {
                        throw new Exception("File not found in remote cluster after put request");
                    }
                    Console.WriteLine("File copied to remote cluster:");
                    Console.WriteLine(sout[0]);
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error: {0}", e.Message);
            }
            finally
            {
                try
                {
                    System.IO.File.Delete(fputfilesfilename);
                }
                catch
                {
                }
            }
        }

        static void DfsFPut(string[] args)
        {
            List<string> files = null;
            List<string> dirs = null;
            string pttn = "*.txt";
            string dfsfilename = "";
            string mode = "";
            const string contimode = "continuous";
            const string dfschunksmode = "dfschunks";

            for (int i = 0; i < args.Length; i++)
            {
                string skey = args[i].ToLower();
                string sval = "";
                int del = skey.IndexOf('=');
                if (del > -1)
                {
                    sval = skey.Substring(del + 1);
                    skey = skey.Substring(0, del);
                }
                if (sval.Length == 0)
                {
                    Console.Error.WriteLine("fput error:  Argument {0} expects a value.", skey);
                    SetFailure();
                    return;
                }
                switch (skey)
                {
                    case "files":
                        {
                            files = new List<string>();
                            if (sval[0] == '@')
                            {
                                GetItemsFromFileAppend(sval.Substring(1), files);
                            }
                            else
                            {
                                files.AddRange(sval.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries));
                            }
                        }
                        break;

                    case "dirs":
                        {
                            dirs = new List<string>();
                            if (sval[0] == '@')
                            {
                                GetItemsFromFileAppend(sval.Substring(1), dirs);
                            }
                            else
                            {
                                dirs.AddRange(sval.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries));
                            }
                        }
                        break;

                    case "pattern":
                        pttn = sval;
                        break;

                    case "dfsfilename":
                        dfsfilename = sval;
                        if (dfsfilename.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                        {
                            dfsfilename = dfsfilename.Substring(6);
                        }
                        break;

                    case "mode":
                        mode = sval;
                        break;

                    default:
                        Console.Error.WriteLine("fput error:  Invalid argument: {0}", skey);
                        SetFailure();
                        return;
                }
            }

            if ((mode == contimode || mode == dfschunksmode) && dfsfilename.Length == 0)
            {
                Console.Error.WriteLine("fput error:  Must provide dfsfilename when mode is {0}.", contimode);
                SetFailure();
                return;
            }

            List<string> fileList = new List<string>();
            List<string> nonExist = new List<string>();
            Dictionary<string, int> uniqueSrcHosts = new Dictionary<string, int>();

            if (files == null && dirs == null)
            {
                Console.Error.WriteLine("fput error:  {0} fput files|dirs=<item[,item,item]> [pattern=<pattern>] ", appname);
                SetFailure();
                return;
            }
            if (files != null)
            {
                for (int i = 0; i < files.Count; i++)
                {
                    if (System.IO.File.Exists(files[i]))
                    {
                        fileList.Add(files[i]);
                        string thissrchost = GetHostName(files[i]).ToUpper();
                        if (!uniqueSrcHosts.ContainsKey(thissrchost))
                        {
                            uniqueSrcHosts.Add(thissrchost, 0);
                        }
                    }
                    else
                    {
                        nonExist.Add(files[i]);
                    }
                }
            }

            if (dirs != null)
            {
                for (int i = 0; i < dirs.Count; i++)
                {
                    if (System.IO.Directory.Exists(dirs[i]))
                    {
                        string[] matched = System.IO.Directory.GetFiles(dirs[i], pttn);
                        if (matched.Length > 0)
                        {
                            string thissrchost = GetHostName(dirs[i]).ToUpper();
                            if (!uniqueSrcHosts.ContainsKey(thissrchost))
                            {
                                uniqueSrcHosts.Add(thissrchost, 0);
                            }
                        }
                        for (int j = 0; j < matched.Length; j++)
                        {
                            fileList.Add(matched[j]);
                        }
                    }
                    else
                    {
                        nonExist.Add(dirs[i]);
                    }
                }
            }

            for (int i = 0; i < nonExist.Count; i++)
            {
                Console.Error.WriteLine(nonExist[i] + " does not exist");
            }

            if (fileList.Count == 0)
            {
                Console.WriteLine("There is no file to put.");
                return;
            }

            string[] hosts = null;
            int cooktimeout = -1;
            int cookretries = -1;
            {
                dfs dc = LoadDfsConfig();               
                hosts = dc.Slaves.SlaveList.Split(';');
                cooktimeout = dc.slave.CookTimeout;
                cookretries = dc.slave.CookRetries;
            }

            Random rand = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
            for (int i = 0; i < hosts.Length; i++)
            {
                int r = rand.Next() % hosts.Length;
                string str = hosts[r];
                hosts[r] = hosts[i];
                hosts[i] = str;
            }

            if (mode != contimode && mode != dfschunksmode)
            {
                for (int i = 0; i < fileList.Count; i++)
                {
                    int r = rand.Next() % fileList.Count;
                    string str = fileList[r];
                    fileList[r] = fileList[i];
                    fileList[i] = str;
                }

                int batchsize = Math.Min(uniqueSrcHosts.Count, hosts.Length) * 8;
                int curfile = 0;
                int curhost = 0;
                for (; ; )
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(@"<SourceCode>
                <Jobs>
                <Job Name=`` Custodian=`` Email=`` Description=``>
                   <IOSettings>        
                          <JobType>remote</JobType>");

                    int batched = 0;
                    while (batched < batchsize && curfile < fileList.Count)
                    {
                        if (curhost >= hosts.Length)
                        {
                            curhost = 0;
                        }

                        System.IO.FileInfo fInfo = new System.IO.FileInfo(fileList[curfile]);
                        sb.Append(@"<DFS_IO>
                        <DFSReader></DFSReader>          
                        <DFSWriter>");
                        sb.Append(fInfo.Name);
                        sb.Append(@"</DFSWriter>
                        <Host>");
                        sb.Append(hosts[curhost]);
                        sb.Append(@"</Host><Meta>");
                        sb.Append(fileList[curfile]);
                        sb.Append(@"</Meta>
                        </DFS_IO>");

                        batched++;
                        curfile++;
                        curhost++;
                    }

                    sb.Append(@"      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                string inFile = Qizmt_Meta;
                bool isgz = inFile.EndsWith(`.gz`, StringComparison.OrdinalIgnoreCase);

                //----------------------------COOKING--------------------------------
                int cooktimeout = ");
                    sb.Append(cooktimeout);
                    sb.Append(@";
                int cookretries = ");
                    sb.Append(cookretries);
                    sb.Append(@";
                bool cooking_is_cooked = false;
                int cooking_cooksremain = cookretries;
                bool cooking_is_read = false;
                long cooking_pos = 0;
                //----------------------------COOKING--------------------------------

                System.IO.Stream fs = null;
                const int MAXREAD = 0x400 * 64;
                byte[] fbuf = new byte[MAXREAD];
                bool bomdone = false;

                while (true)
                {
                    try
                    {
                        //----------------------------COOKING--------------------------------
                        cooking_is_read = true;

                        if (cooking_is_cooked)
                        {
                            cooking_is_cooked = false;
                            System.Threading.Thread.Sleep(MySpace.DataMining.DistributedObjects.IOUtils.RealRetryTimeout(cooktimeout));
                            if (fs != null)
                            {
                                fs.Close();
                            }                        
                            fs = new System.IO.FileStream(inFile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);                        
                            if (isgz)
                            {
                                fs = new System.IO.Compression.GZipStream(fs, System.IO.Compression.CompressionMode.Decompress);
                            }                        
                            fs.Seek(cooking_pos, System.IO.SeekOrigin.Begin);
                        }
                        //----------------------------COOKING--------------------------------

                        if (fs == null)
                        {
                            fs = new System.IO.FileStream(inFile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
                            if (isgz)
                            {
                                fs = new System.IO.Compression.GZipStream(fs, System.IO.Compression.CompressionMode.Decompress);
                            }
                        }

                        if (!bomdone)
                        {                        
                            int bomread = fs.Read(fbuf, 0, 3);
                            bomdone = true;
                            //----------------------------COOKING--------------------------------
                            cooking_pos = bomread;
                            cooking_is_read = false;
                            //----------------------------COOKING--------------------------------
                            if (!(fbuf[0] == 0xEF && fbuf[1] == 0xBB && fbuf[2] == 0xBF))
                            {
                                for (int i = 0; i < bomread; i++)
                                {
                                    dfsoutput.WriteByte(fbuf[i]);
                                }
                            }                  
                        }

                        cooking_is_read = true;
                        int read = fs.Read(fbuf, 0, MAXREAD);
                        if (read < 1)
                        {
                            break;
                        }
                        //----------------------------COOKING--------------------------------
                        cooking_pos += read;
                        cooking_is_read = false;
                        //----------------------------COOKING--------------------------------
                        dfsoutput.Write(fbuf, 0, read);   
                    }
                    catch (Exception e)
                    {
                        //----------------------------COOKING--------------------------------
                        if (!cooking_is_read)
                        {
                            Qizmt_Log(`Error occurred when putting :` + inFile + `. Error: ` + e.ToString());
                            break;
                        }
                        if (cooking_cooksremain-- <= 0)
                        {
                            Qizmt_Log(`Error occurred when putting :` + inFile + `. Cooked too many times: ` + e.ToString());
                            break;
                        }
                        cooking_is_cooked = true;
                        continue;
                        //----------------------------COOKING--------------------------------
                    }
                }
                if (fs != null)
                {
                    fs.Close();
                }           
                Qizmt_Log(`Uploaded: ` + inFile);
            }
        $^$>
              </Remote>
    </Job>
    </Jobs>
</SourceCode>").Replace("$^$", "]]").Replace('`', '"');

                    string tmpJobFile = @"fput_" + Guid.NewGuid().ToString();
                    using (System.IO.StreamWriter writer = new System.IO.StreamWriter(tmpJobFile))
                    {
                        writer.Write(sb.ToString());
                        writer.Close();
                    }
                    Console.WriteLine("Putting files in batch...");
                    Exec("", LoadConfig(null, tmpJobFile), new string[] { }, false, false);
                    System.IO.File.Delete(tmpJobFile);

                    if (curfile >= fileList.Count)
                    {
                        break;
                    }
                }
                Console.WriteLine("All done");
            }
            else
            {
                //continuous mode.
                Dictionary<string, List<string>> srchostfiles = new Dictionary<string, List<string>>(fileList.Count);
                Dictionary<string, List<string>> workerfiles = new Dictionary<string, List<string>>(fileList.Count);

                foreach (string srcfile in fileList)
                {
                    string srchost = GetHostName(srcfile).ToLower();
                    if (!srchostfiles.ContainsKey(srchost))
                    {
                        srchostfiles.Add(srchost, new List<string>());
                    }
                    srchostfiles[srchost].Add(srcfile);
                }

                {
                    int iw = 0;
                    foreach (string srchost in srchostfiles.Keys)
                    {
                        List<string> srcfiles = srchostfiles[srchost];
                        for (int i = 0; i < srcfiles.Count; i++)
                        {
                            int r = rand.Next() % srcfiles.Count;
                            string str = srcfiles[r];
                            srcfiles[r] = srcfiles[i];
                            srcfiles[i] = str;
                        }

                        string worker = hosts[iw];
                        if (!workerfiles.ContainsKey(worker))
                        {
                            workerfiles.Add(worker, new List<string>());
                        }
                        workerfiles[worker].AddRange(srcfiles);

                        if (++iw >= hosts.Length)
                        {
                            iw = 0;
                        }
                    }
                }

                StringBuilder sb = new StringBuilder();
                sb.Append(@"<SourceCode>
                <Jobs>
                <Job Name=`` Custodian=`` Email=`` Description=``>
                   <IOSettings>        
                          <JobType>remote</JobType>");

                StringBuilder sbList = new StringBuilder();
                StringBuilder sbCase = new StringBuilder();
                string tempoutfilename = Guid.NewGuid().ToString();

                {
                    int wi = 0;                    
                    int tempoutfileindex = 0;
                    foreach (string worker in workerfiles.Keys)
                    {
                        for (int io = 0; io < 8; io++)
                        {
                            sb.Append(@"<DFS_IO>
<DFSReader></DFSReader>");
                            sb.Append("<DFSWriter>");
                            sb.Append(tempoutfilename);
                            sb.Append("_");
                            sb.Append(tempoutfileindex++);
                            sb.Append("</DFSWriter>");
                            sb.Append("<Host>");
                            sb.Append(worker);
                            sb.Append(@"</Host>
<Meta>");
                            sb.Append(wi);
                            sb.Append("|");
                            sb.Append(io);
                            sb.Append(@"</Meta>
</DFS_IO>");
                        }                        

                        List<string> srcfiles = workerfiles[worker];
                        string arrayname = "srcfiles_" + wi.ToString();                        
                        sbList.Append("string[] ");
                        sbList.Append(arrayname);
                        sbList.Append(" = new string[");
                        sbList.Append(srcfiles.Count);
                        sbList.Append("];");
                        sbList.Append(Environment.NewLine);
                        for (int fi = 0; fi < srcfiles.Count; fi++)
                        {                            
                            sbList.Append(arrayname);
                            sbList.Append("[");
                            sbList.Append(fi);
                            sbList.Append("] = @`");
                            sbList.Append(srcfiles[fi]);
                            sbList.Append("`;");
                            sbList.Append(Environment.NewLine);
                        }
                                                
                        sbCase.Append("case ");
                        sbCase.Append(wi);
                        sbCase.Append(@":
srcFiles = ");
                        sbCase.Append(arrayname);
                        sbCase.Append(@";
break;");
                        sbCase.Append(Environment.NewLine);

                        wi++;
                    }
                }

                sb.Append(@"</IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                int workerindex = 0;
                int processid = 0;
                {
                    int del = Qizmt_Meta.IndexOf(`|`);
                    workerindex = Int32.Parse(Qizmt_Meta.Substring(0, del));
                    processid = Int32.Parse(Qizmt_Meta.Substring(del + 1));
                }
                string[] srcFiles = null;");

                sb.Append(sbList.ToString());

                sb.Append(@"
switch(workerindex)
{");
                sb.Append(sbCase.ToString());
                sb.Append(@"
}
               

                for(int fi = 0; fi < srcFiles.Length; fi++)
                {
                    if(fi % 8 != processid)
                    {
                        continue;
                    }

                    string inFile = srcFiles[fi];
                    bool isgz = inFile.EndsWith(`.gz`, StringComparison.OrdinalIgnoreCase);

                    const bool dfschunks = ");
                    sb.Append(mode == "dfschunks" ? "true" : "false");
                    sb.Append(@";
                    bool dfschunksdone = false;

                    //----------------------------COOKING--------------------------------
                    int cooktimeout = ");
                    sb.Append(cooktimeout);
                    sb.Append(@";
                    int cookretries = ");
                    sb.Append(cookretries);
                    sb.Append(@";
                    bool cooking_is_cooked = false;
                    int cooking_cooksremain = cookretries;
                    bool cooking_is_read = false;
                    long cooking_pos = 0;
                    //----------------------------COOKING--------------------------------

                    System.IO.Stream fs = null;
                    const int MAXREAD = 0x400 * 64;
                    byte[] fbuf = new byte[MAXREAD];
                    bool bomdone = false;

                    while (true)
                    {
                        try
                        {
                            //----------------------------COOKING--------------------------------
                            cooking_is_read = true;

                            if (cooking_is_cooked)
                            {
                                cooking_is_cooked = false;
                                System.Threading.Thread.Sleep(MySpace.DataMining.DistributedObjects.IOUtils.RealRetryTimeout(cooktimeout));
                                if (fs != null)
                                {
                                    fs.Close();
                                }                        
                                fs = new System.IO.FileStream(inFile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);                        
                                if (isgz)
                                {
                                    fs = new System.IO.Compression.GZipStream(fs, System.IO.Compression.CompressionMode.Decompress);
                                }                        
                                fs.Seek(cooking_pos, System.IO.SeekOrigin.Begin);
                            }
                            //----------------------------COOKING--------------------------------

                            if (fs == null)
                            {
                                fs = new System.IO.FileStream(inFile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
                                if (isgz)
                                {
                                    fs = new System.IO.Compression.GZipStream(fs, System.IO.Compression.CompressionMode.Decompress);
                                }
                            }

                            if (dfschunks)
                            {
                                if(!dfschunksdone)
                                {
                                    dfschunksdone = true;
                                    fs.Read(fbuf, 0, 4);
                                    int headerlen = Entry.BytesToInt(fbuf);
                                    if(headerlen < 4 || headerlen > 0x400 * 2)
                                    {
                                        throw new Exception(`Invalid header length for DFS file chunk: ` + inFile);
                                    }
                                    for(int hremain = headerlen - 4; hremain > 0;)
                                    {
                                        int hrx = fs.Read(fbuf, 0, hremain);
                                        if(hrx < 0)
                                        {
                                            throw new Exception(`Unable to read entire header from DFS file chunk: ` + inFile);
                                        }
                                        hremain -= hrx;
                                    }
                                }
                            }
                            else if (!bomdone)
                            {                        
                                int bomread = fs.Read(fbuf, 0, 3);
                                bomdone = true;
                                //----------------------------COOKING--------------------------------
                                cooking_pos = bomread;
                                cooking_is_read = false;
                                //----------------------------COOKING--------------------------------
                                if (!(fbuf[0] == 0xEF && fbuf[1] == 0xBB && fbuf[2] == 0xBF))
                                {
                                    for (int i = 0; i < bomread; i++)
                                    {
                                        dfsoutput.WriteByte(fbuf[i]);
                                    }
                                }                  
                            }

                            cooking_is_read = true;
                            int read = fs.Read(fbuf, 0, MAXREAD);
                            if (read < 1)
                            {
                                break;
                            }
                            //----------------------------COOKING--------------------------------
                            cooking_pos += read;
                            cooking_is_read = false;
                            //----------------------------COOKING--------------------------------
                            dfsoutput.Write(fbuf, 0, read);   
                        }
                        catch (Exception e)
                        {
                            //----------------------------COOKING--------------------------------
                            if (!cooking_is_read)
                            {
                                Qizmt_Log(`Error occurred when putting :` + inFile + `. Error: ` + e.ToString());
                                break;
                            }
                            if (cooking_cooksremain-- <= 0)
                            {
                                Qizmt_Log(`Error occurred when putting :` + inFile + `. Cooked too many times: ` + e.ToString());
                                break;
                            }
                            cooking_is_cooked = true;
                            continue;
                            //----------------------------COOKING--------------------------------
                        }
                    }
                    if (fs != null)
                    {
                        fs.Close();
                    }           
                    Qizmt_Log(`Uploaded: ` + inFile);
                }
            }
        $^$>
              </Remote>
    </Job>
    <Job Name=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt combine ");
                sb.Append(tempoutfilename);
                sb.Append("_* +");
                sb.Append(dfsfilename);                
                sb.Append(@"`);
            }
        ]]>
      </Local>
    </Job>
    </Jobs>
</SourceCode>").Replace("$^$", "]]").Replace('`', '"');

                    string tmpJobFile = @"fput_" + Guid.NewGuid().ToString();
                    using (System.IO.StreamWriter writer = new System.IO.StreamWriter(tmpJobFile))
                    {
                        writer.Write(sb.ToString());
                        writer.Close();
                    }
                    Console.WriteLine("Putting files ...");
                    Exec("", LoadConfig(null, tmpJobFile), new string[] { }, false, false);
                    System.IO.File.Delete(tmpJobFile);
                    Console.Write("All done");
            }
        }

        static string GetHostName(string networkpath)
        {
            string host = "";
            if (networkpath.StartsWith(@"\\"))
            {
                host = networkpath.Substring(2, networkpath.IndexOf('\\', 2));
            }
            return host;
        }

        static void DfsFGet(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("fget error:  {0} fget <dfspath> <targetFolder>[ <targetFolder> <targetFolder>] [-gz] [-md5]", appname);
                SetFailure();
                return;
            }

            string dfsfile = args[0];
            if (dfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                dfsfile = dfsfile.Substring(6);
            }

            string dfsfilename = "";
            string dfsfileext = "";
            {
                int del = dfsfile.LastIndexOf('.');
                if (del > -1)
                {
                    dfsfileext = dfsfile.Substring(del);
                    dfsfilename = dfsfile.Substring(0, del);
                }
                else
                {
                    dfsfilename = dfsfile;
                }
            }

            Dictionary<string, string> netpaths = new Dictionary<string, string>();
            string[] hosts = null;
            string[] parts = null;
            int cooktimeout = -1;
            int cookretries = -1;
            {
                dfs.DfsFile df;
                using (LockDfsMutex())
                {
                    dfs dc = LoadDfsConfig();
                    df = dc.FindAny(dfsfile);
                    if (null == df)
                    {
                        Console.Error.WriteLine("File not found in DFS: {0}", dfsfile);
                        SetFailure();
                        return;
                    }
                    hosts = dc.Slaves.SlaveList.Split(';');
                    cooktimeout = dc.slave.CookTimeout;
                    cookretries = dc.slave.CookRetries;
                }

                foreach (string host in hosts)
                {
                    if (Surrogate.IsHealthySlaveMachine(host))
                    {
                        netpaths.Add(host.ToLower(), Surrogate.NetworkPathForHost(host));
                    }
                }

                parts = new string[df.Nodes.Count];
                for (int i = 0; i < df.Nodes.Count; i++)
                {
                    dfs.DfsFile.FileNode fn = df.Nodes[i];
                    string[] fnhosts = fn.Host.ToLower().Split(';');
                    string fnnetpath = null;
                    foreach (string fnhost in fnhosts)
                    {
                        if (netpaths.ContainsKey(fnhost))
                        {
                            fnnetpath = netpaths[fnhost];
                            break;
                        }
                    }
                    if (fnnetpath == null)
                    {
                        Console.Error.WriteLine("File node is not on any healthy machine: {0}: {1}", fn.Host, fn.Name);
                        SetFailure();
                        return;
                    }
                    parts[i] = fnnetpath + @"\" + fn.Name;
                }
            }

            List<string> targetdirs = null;
            Dictionary<string, int> uniqueTargetHosts = new Dictionary<string, int>();
            {
                if (args[1][0] == '@')
                {
                    targetdirs = new List<string>();
                    GetItemsFromFileAppend(args[1].Substring(1), targetdirs);
                }
                else
                {
                    targetdirs = new List<string>(args.Length - 1);
                    for (int i = 1; i < args.Length; i++)
                    {
                        if (args[i][0] != '-')
                        {
                            targetdirs.Add(args[i]);
                        }
                    }
                }

                for (int i = 0; i < targetdirs.Count; i++)
                {
                    string _dir = targetdirs[i];
                    if (_dir.EndsWith(@"\"))
                    {
                        _dir = _dir.Substring(0, _dir.Length - 1);
                        targetdirs[i] = _dir;
                    }

                    string thistargethost = GetHostName(targetdirs[i]).ToUpper();
                    if (!uniqueTargetHosts.ContainsKey(thistargethost))
                    {
                        uniqueTargetHosts.Add(thistargethost, 0);
                    }
                }
            }

            bool isgz = false;
            bool ismd5 = false;
            for (int i = 2; i < args.Length; i++)
            {
                string thisarg = args[i].ToLower();
                switch(thisarg)
                {
                    case "-gz":
                        isgz = true;
                        break;

                    case "-md5":
                        ismd5 = true;
                        break;
                }               
            }

            Random rand = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
            for (int i = 0; i < hosts.Length; i++)
            {
                int r = rand.Next() % hosts.Length;
                string str = hosts[r];
                hosts[r] = hosts[i];
                hosts[i] = str;
            }

            int[] orders = new int[parts.Length];
            for (int i = 0; i < parts.Length; i++)
            {
                orders[i] = i;
            }
            for (int i = 0; i < orders.Length; i++)
            {
                int r = rand.Next() % orders.Length;
                int num = orders[r];
                orders[r] = orders[i];
                orders[i] = num;
            }

            int batchsize = Math.Min(uniqueTargetHosts.Count, hosts.Length) * 8;
            {
                int curorder = 0;
                int curhost = 0;
                for (; ; )
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(@"<SourceCode>
                <Jobs>
                <Job Name="""">
                   <IOSettings>        
                          <JobType>remote</JobType>");

                    int batched = 0;
                    while (batched < batchsize && curorder < orders.Length)
                    {
                        if (curhost >= hosts.Length)
                        {
                            curhost = 0;
                        }
                      
                        int index = orders[curorder];
                        string partnetpath = parts[index];
                        string targetdir = targetdirs[index % targetdirs.Count];
                        string meta = partnetpath + '|' + targetdir + '|' + index;

                        sb.Append(@"<DFS_IO>
                    <DFSReader></DFSReader>          
                    <DFSWriter></DFSWriter>
                    <Host>");
                        sb.Append(hosts[curhost]);
                        sb.Append(@"</Host>
                    <Meta>");
                        sb.Append(meta);
                        sb.Append(@"</Meta>
                    </DFS_IO>");

                        batched++;
                        curorder++;
                        curhost++;
                    }

                    sb.Append(@"      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                string dfsfn = `" + dfsfilename + @"`;
                string dfsext = `" + dfsfileext + @"`;
                bool isgz = " + (isgz ? "true" : "false") + @";
                bool ismd5 = " + (ismd5 ? "true" : "false") + @";
                string[] meta = Qizmt_Meta.Split('|');
                string srcfilepath = meta[0];
                string targetdir = meta[1];
                string index = meta[2];
                if (!System.IO.Directory.Exists(targetdir))
                {
                    System.IO.Directory.CreateDirectory(targetdir);
                }
                string tarfilepath = targetdir + @`\` + dfsfn + `_` + index + dfsext;
                string tarfilepathmd5 = tarfilepath + `.hd`;
                if (isgz)
                {
                    tarfilepath += `.gz`;
                }
               
                //----------------------------COOKING--------------------------------
                int cooktimeout = ");
                    sb.Append(cooktimeout);
                    sb.Append(@";
                int cookretries = ");
                    sb.Append(cookretries);
                    sb.Append(@";
                bool cooking_is_cooked = false;
                int cooking_cooksremain = cookretries;
                bool cooking_is_read = false;
                long cooking_pos = 0;
                //----------------------------COOKING--------------------------------

                const int FILE_BUFFER_SIZE = 0x1000;
                const int MAX_SIZE_PER_RECEIVE = 0x400 * 64;
                byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];
                System.IO.Stream fsrc = null;
                bool headdone = false;
                int headerlen = 0;
                System.IO.Stream ftar = null;
                {
                    System.IO.FileStream _ftar = new System.IO.FileStream(tarfilepath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                    if (isgz)
                    {
                        ftar = new System.IO.Compression.GZipStream(_ftar, System.IO.Compression.CompressionMode.Compress);
                    }
                    else
                    {
                        ftar = _ftar;
                    }
                }           

                while (true)
                {
                    try
                    {
                        //----------------------------COOKING--------------------------------
                        cooking_is_read = true;

                        if (cooking_is_cooked)
                        {
                            cooking_is_cooked = false;
                            System.Threading.Thread.Sleep(MySpace.DataMining.DistributedObjects.IOUtils.RealRetryTimeout(cooktimeout));
                            if (fsrc != null)
                            {
                                fsrc.Close();
                            }
                            fsrc = new System.IO.FileStream(srcfilepath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
                            fsrc.Seek(cooking_pos, System.IO.SeekOrigin.Begin);
                        }
                        //----------------------------COOKING--------------------------------

                        if (fsrc == null)
                        {
                            fsrc = new System.IO.FileStream(srcfilepath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE);
                        }

                        if (!headdone)
                        {
                            int xread = StreamReadLoop(fsrc, fbuf, 4);
                            if (4 == xread)
                            {
                                int hlen = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(fbuf);
                                StreamReadExact(fsrc, fbuf, hlen - 4);
                                xread = hlen;
                            }
                            headdone = true;
                            headerlen = xread;
                            //----------------------------COOKING--------------------------------
                            cooking_pos = xread;
                            cooking_is_read = false;
                            //----------------------------COOKING--------------------------------
                        }

                        {
                            cooking_is_read = true;
                            int xread = fsrc.Read(fbuf, 0, MAX_SIZE_PER_RECEIVE);
                            if (xread <= 0)
                            {
                                break;
                            }
                            //----------------------------COOKING--------------------------------
                            cooking_pos += xread;
                            cooking_is_read = false;
                            //----------------------------COOKING--------------------------------
                            ftar.Write(fbuf, 0, xread);
                        }
                    }
                    catch (Exception e)
                    {
                        //----------------------------COOKING--------------------------------
                        if (!cooking_is_read)
                        {
                            throw;
                        }
                        if (cooking_cooksremain-- <= 0)
                        {
                            throw new System.IO.IOException(`Cooked too many times: ` + e.ToString());
                        }
                        cooking_is_cooked = true;
                        continue;
                        //----------------------------COOKING--------------------------------
                    }
                }

                if(ismd5)
                {
                    System.Security.Cryptography.MD5 md5 = new System.Security.Cryptography.MD5CryptoServiceProvider();
                    fsrc.Seek(headerlen, System.IO.SeekOrigin.Begin);
                    byte[] hashresult = md5.ComputeHash(fsrc);
                    StringBuilder sbresult = new StringBuilder(32);   
                    foreach(byte hb in hashresult)
                    {
                        sbresult.Append(hb.ToString(`x2`));
                    }
                    System.IO.File.WriteAllText(tarfilepathmd5, sbresult.ToString());
                }
                
                fsrc.Close();
                ftar.Close();
            }
            
            static int StreamReadLoop(System.IO.Stream stm, byte[] buf, int len)
            {
                int sofar = 0;
                try
                {
                    while (sofar < len)
                    {
                        int xread = stm.Read(buf, sofar, len - sofar);
                        if (xread <= 0)
                        {
                            break;
                        }
                        sofar += xread;
                    }
                    return sofar;
                }
                catch (ArgumentException e)
                {
                    throw new ArgumentException(`StreamRead* Requested ` + len.ToString() + ` bytes; ` + e.Message, e);
                }
            }

            static void StreamReadExact(System.IO.Stream stm, byte[] buf, int len)
            {
                if (len != StreamReadLoop(stm, buf, len))
                {
                    throw new System.IO.IOException(`Unable to read from stream`);
                }
            }
        $^$>
              </Remote>
    </Job>
    </Jobs>
</SourceCode>").Replace("$^$", "]]").Replace('`', '"');

                    string jobname = "fget_" + Guid.NewGuid().ToString();
                    using (System.IO.StreamWriter sw = System.IO.File.CreateText(jobname))
                    {
                        sw.Write(sb.ToString());
                        sw.Close();
                    }
                    Console.WriteLine("Getting files in batch...");
                    Exec("", LoadConfig(null, jobname), new string[] { }, false, false);
                    System.IO.File.Delete(jobname);

                    if (curorder >= orders.Length)
                    {
                        break;
                    }
                }
            }            
            Console.WriteLine("All done");
        }

        static void GetItemsFromFileAppend(string file, List<string> append)
        {
            using (System.IO.StreamReader sr = System.IO.File.OpenText(file))
            {
                string s;
                for (; ; )
                {
                    s = sr.ReadLine();
                    if (null == s)
                    {
                        break;
                    }
                    s = s.Trim();
                    if (s.Length < 1 || '#' == s[0])
                    {
                        continue;
                    }
                    append.AddRange(s.Split(';', ','));
                }
            }
        }

        static void DfsPut(string[] args, long maxLineSize)
        {
            if (args.Length > 0 && "-rv" == args[0])
            {
                args = SubArray(args, 1);
                ReplicationDebugVerbose = true;
            }

            if (!dfs.DfsConfigExists(DFSXMLPATH))
            {
                Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                SetFailure();
                return;
            }
            if (args.Length < 1)
            {
                Console.Error.WriteLine("dfs put error:  {0} dfs put <localpath> [<dfspath>]", appname);
                SetFailure();
                return;
            }
            {
                string localpath = args[0];
                if (!System.IO.File.Exists(localpath))
                {
                    Console.Error.WriteLine("File not found: {0}", localpath);
                    SetFailure();
                    return;
                }
                string dfspath;
                if (args.Length > 1)
                {
                    dfspath = args[1];
                    if (dfspath.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        dfspath = dfspath.Substring(6);
                    }
                }
                else
                {
                    dfspath = (new System.IO.FileInfo(localpath)).Name;
                }

                int RecordLength = -1;
                {
                    int ic = dfspath.IndexOf('@');
                    if (-1 != ic)
                    {
                        try
                        {
                            RecordLength = Surrogate.GetRecordSize(dfspath.Substring(ic + 1));
                            dfspath = dfspath.Substring(0, ic);
                        }
                        catch (FormatException e)
                        {
                            Console.Error.WriteLine("Invalid Record Length or DFS path: {0}", e.Message);
                            SetFailure();
                            return;
                        }
                        catch (OverflowException e)
                        {
                            Console.Error.WriteLine("Invalid Record Length or DFS path: {0}", e.Message);
                            SetFailure();
                            return;
                        }
                    }
                }

                {
                    string reason = "";
                    if (dfs.IsBadFilename(dfspath, out reason))
                    {
                        Console.Error.WriteLine("Invalid DFS path: {0}", reason);
                        SetFailure();
                        return;
                    }
                }

                EnsureNetworkPath(localpath);

                {
                    dfs dc = LoadDfsConfig();

                    if (localpath.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
                    {
                        if (-1 != dfspath.IndexOf(@".\") || -1 != dfspath.IndexOf(@"./"))
                        {
                            // Prevent navigating directories.
                            Console.Error.WriteLine("Invalid DFS name for DLL");
                            SetFailure();
                            return;
                        }
                        System.IO.FileInfo dllfi = new System.IO.FileInfo(localpath);
                        dc.Find(dfspath, DfsFileTypes.DLL); // Error if not dll, otherwise fine to replace.
                        string[] slaves = dc.Slaves.SlaveList.Split(';');
                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                            new Action<string>(
                            delegate(string slave)
                            {
                                string netpath = Surrogate.NetworkPathForHost(slave);
                                string cacpath = netpath + @"\" + dfs.DLL_DIR_NAME;
                                try
                                {
                                    System.IO.Directory.CreateDirectory(cacpath);
                                }
                                catch
                                {
                                }
                                System.IO.File.Copy(localpath, cacpath + @"\" + dfspath, true);
                            }), slaves);
                        using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                        {
                            dc = LoadDfsConfig(); // Reload in case of changes during put.
                            dfs.DfsFile dfsfile = dc.Find(dfspath, DfsFileTypes.DLL); // Error if not dll, otherwise fine to replace.
                            if (null == dfsfile)
                            {
                                dfsfile = new dfs.DfsFile();
                                dc.Files.Add(dfsfile);
                            }
                            dfsfile.Type = DfsFileTypes.DLL;
                            dfsfile.Name = dfspath;
                            dfsfile.Size = dllfi.Length;

                            UpdateDfsXml(dc);
                        }
                        Console.WriteLine("dfs://{0} successfully written", dfspath);
                    }
                    else
                    {
                        for (int i = 0; i < dc.Files.Count; i++)
                        {
                            if (0 == string.Compare(dc.Files[i].Name, dfspath, true))
                            {
                                Console.Error.WriteLine("Error:  The specified file already exists in DFS: {0}", dfspath);
                                SetFailure();
                                return;
                            }
                        }

                        long sampledist = dc.DataNodeBaseSize / dc.DataNodeSamples;

                        using (System.IO.FileStream _fs = new System.IO.FileStream(localpath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                        {
                            //const int MAX_SIZE_PER_RECEIVE = 0x400 * 64;
                            //byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];
                            byte[] fbuf = new byte[maxLineSize];
                            int[] lbuf = new int[3];
                            short lbufCount = 0;

                            System.IO.Stream fs = _fs;

                            if (localpath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
                            {
                                fs = new System.IO.Compression.GZipStream(_fs, System.IO.Compression.CompressionMode.Decompress);

                                if (RecordLength < 1)
                                {
                                    lbuf[2] = fs.ReadByte();
                                    lbuf[1] = fs.ReadByte();
                                    lbuf[0] = fs.ReadByte();

                                    if (!(lbuf[2] == 0xEF && lbuf[1] == 0xBB && lbuf[0] == 0xBF))
                                    {
                                        lbufCount = 3;
                                    }
                                }
                            }
                            else
                            {
                                if (RecordLength < 1)
                                {
                                    //remove BOM						
                                    fs.Read(fbuf, 0, 3);

                                    if (!(fbuf[0] == 0xEF && fbuf[1] == 0xBB && fbuf[2] == 0xBF))
                                    {
                                        fs.Position = 0;
                                    }
                                }
                            }

                            string[] slaves = dc.Slaves.SlaveList.Split(',', ';');
                            if (null == dc.Slaves.SlaveList || dc.Slaves.SlaveList.Length == 0 || slaves.Length < 1)
                            {
                                Console.Error.WriteLine("SlaveList expected in configuration (no machines)");
                                SetFailure();
                                return;
                            }
                            if (dc.Replication > 1)
                            {
                                slaves = ExcludeUnhealthySlaveMachines(slaves, true).ToArray();
                            }
                            if (0 == slaves.Length)
                            {
                                Console.Error.WriteLine("No healthy machines for DFS put");
                                SetFailure();
                                return;
                            }

                            Random rnd = new Random((DateTime.Now.Millisecond / 2) + (System.Diagnostics.Process.GetCurrentProcess().Id / 2));

                            List<dfs.DfsFile.FileNode> ninfos = new List<dfs.DfsFile.FileNode>(64);
                            int nextslave = rnd.Next() % slaves.Length;
                            long curbytepos = 0;
                            for (; ; )
                            {

#if DEBUG
                                if (RecordLength > 0)
                                {
                                    if (lbufCount != 0)
                                    {
                                        // lbufCount should be zero here because BOM isn't used with rectangular records.
                                        throw new Exception("Internal error: (RecordLength > 0) && (lbufCount != 0)");
                                    }
                                }
#endif

                                string SlaveHost = slaves[nextslave];
                                string SlaveIP = IPAddressUtil.GetIPv4Address(SlaveHost);
                                if (++nextslave >= slaves.Length)
                                {
                                    nextslave = 0;
                                }
                                string netdir = NetworkPathForHost(SlaveHost);
                                string chunkname = GenerateZdFileDataNodeName(dfspath);
                                string chunkpath = netdir + @"\" + chunkname;
                                string samplepath = netdir + @"\" + chunkname + ".zsa";
                                using (System.IO.FileStream _fc = new System.IO.FileStream(chunkpath, System.IO.FileMode.CreateNew, System.IO.FileAccess.Write, System.IO.FileShare.None, FILE_BUFFER_SIZE))
                                {
                                    System.IO.FileStream samps = null;
                                    if (RecordLength < 1)
                                    {
                                        samps = new System.IO.FileStream(samplepath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None);
                                    }
                                    try
                                    {
                                        long chunkpos = 0;
                                        long nextsamplepos = 0;

                                        long Position = curbytepos;

                                        System.IO.Stream fc = _fc;
                                        if (1 == dc.slave.CompressDfsChunks)
                                        {
                                            fc = new System.IO.Compression.GZipStream(_fc, System.IO.Compression.CompressionMode.Compress);
                                        }

                                        MySpace.DataMining.DistributedObjects.Entry.ToBytes(4 + 8, fbuf, 0); // Size of header.
                                        MySpace.DataMining.DistributedObjects.Entry.LongToBytes(Position, fbuf, 4);
                                        fc.Write(fbuf, 0, 4 + 8);

                                        {
                                            long chunkremain = dc.DataNodeBaseSize;
                                            long Length = 0;
                                            bool eof = false;
                                            while (chunkremain > 0 && !eof)
                                            {
                                                if (RecordLength > 0)
                                                {
                                                    int recordremain = RecordLength;
                                                    while (recordremain > 0)
                                                    {
                                                        int xread = recordremain;
                                                        if (xread > fbuf.Length)
                                                        {
                                                            xread = fbuf.Length;
                                                        }
                                                        xread = fs.Read(fbuf, 0, xread);
                                                        if (xread < 1)
                                                        {
                                                            eof = true;
                                                            if (recordremain != RecordLength)
                                                            {
                                                                Console.Error.WriteLine("Warning: incomplete record at end of input file");
                                                            }
                                                            break;
                                                        }
                                                        fc.Write(fbuf, 0, xread);
                                                        chunkremain -= xread;
                                                        recordremain -= xread;
                                                        curbytepos += xread;
#if DEBUG
                                                        if (recordremain < 0)
                                                        {
                                                            throw new Exception("DEBUG: (recordremain < 0)");
                                                        }
#endif
                                                    }
                                                }
                                                else
                                                {

                                                    int xread = 0;

                                                    for (; ; )
                                                    {
                                                        int ib;
                                                        if (lbufCount == 0)
                                                        {
                                                            ib = fs.ReadByte();
                                                        }
                                                        else
                                                        {
                                                            ib = lbuf[--lbufCount];
                                                        }

                                                        if (-1 == ib)
                                                        {
                                                            eof = true;
                                                            break;
                                                        }
                                                        if (xread < fbuf.Length)
                                                        {
                                                            fbuf[xread++] = (byte)ib;
                                                        }
                                                        if ('\n' == ib)
                                                        {
                                                            break;
                                                        }
                                                    }
                                                    //Length += xread;
                                                    chunkremain -= xread;
                                                    curbytepos += xread;
                                                    fc.Write(fbuf, 0, xread);

                                                    chunkpos += xread;

                                                    if (chunkpos >= nextsamplepos)
                                                    {
                                                        samps.Write(fbuf, 0, xread);
                                                        nextsamplepos += sampledist;
                                                    }
                                                }

                                            }

                                            Length = curbytepos - Position;
                                            if (0 == Length)
                                            {
                                                break;
                                            }
                                            Length = curbytepos - Position;

                                            {
                                                dfs.DfsFile.FileNode fnode = new dfs.DfsFile.FileNode();
                                                fnode.Host = SlaveHost;
                                                fnode.Position = Position;
                                                fnode.Length = Length;
                                                fnode.Name = chunkname;
                                                ninfos.Add(fnode);
                                            }

                                        }

                                        fc.Close();
                                    }
                                    finally
                                    {
                                        if (null != samps)
                                        {
                                            samps.Dispose();
                                        }
                                    }
                                }
                            }

                            string dfspathreplicating = ".$" + dfspath + ".$replicating-" + Guid.NewGuid().ToString();
                            using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                            {
                                dc = LoadDfsConfig(); // Reload in case of changes during put.
                                if (null != dc.FindAny(dfspathreplicating))
                                {
                                    Console.Error.WriteLine("Error: file exists: file put into DFS from another location during put: " + dfspathreplicating);
                                    SetFailure();
                                    return;
                                }

                                dfs.DfsFile dfsfile = new dfs.DfsFile();
                                //dfsfile.Nodes = new List<dfs.DfsFile.FileNode>(ninfos);
                                if (RecordLength > 0)
                                {
                                    dfsfile.XFileType = DfsFileTypes.BINARY_RECT + "@" + RecordLength.ToString();
                                }
                                dfsfile.Nodes = ninfos;
                                dfsfile.Name = dfspathreplicating;
                                dfsfile.Size = curbytepos;

                                dc.Files.Add(dfsfile);

                                UpdateDfsXml(dc);
                            }
                            fs.Close();
                            ReplicationPhase(dfspathreplicating, true, 0, slaves);
                            using (LockDfsMutex()) // Needed: change between load & save should be atomic.
                            {
                                dc = LoadDfsConfig(); // Reload in case of changes during put.
                                dfs.DfsFile dfu = dc.FindAny(dfspathreplicating);
                                if (null != dfu)
                                {
                                    if (null != DfsFindAny(dc, dfspath))
                                    {
                                        Console.Error.WriteLine("Error: file exists: file put into DFS from another location during put");
                                        SetFailure();
                                        return;
                                    }
                                    dfu.Name = dfspath;
                                    UpdateDfsXml(dc);
                                }
                            }

                            Console.WriteLine("Sent {0} bytes to file dfs://{1}", curbytepos, dfspath);

                        }
                    }
                }

            }
        }


        class GettingParts
        {
            internal List<dfs.DfsFile.FileNode> parts;
            internal int nextpart = 0;
            internal string localstartname, localendname;
            internal dfs dc;
            internal System.Security.AccessControl.FileSystemAccessRule rule;

            internal void threadproc()
            {
                const int MAX_SIZE_PER_RECEIVE = 0x400 * 64;
                byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];

                for (; ; )
                {
                    dfs.DfsFile.FileNode node;
                    int partnum;
                    lock (this)
                    {
                        if (nextpart >= parts.Count)
                        {
                            break;
                        }
                        partnum = nextpart;
                        node = parts[nextpart++];
                    }
                    string localfullname = localstartname + partnum.ToString() + localendname;
                    using (System.IO.FileStream _fs = new System.IO.FileStream(localfullname, System.IO.FileMode.CreateNew, System.IO.FileAccess.Write, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                    {
                        System.IO.Stream fs = _fs;
                        //if (localendname.EndsWith(".gz"))
                        {
                            fs = new System.IO.Compression.GZipStream(_fs, System.IO.Compression.CompressionMode.Compress);
                        }

                        //string netpath = NetworkPathForHost(node.Host.Split(';')[0]) + @"\" + node.Name;
                        using (System.IO.Stream _fc = new DfsFileNodeStream(node, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                        {
                            System.IO.Stream fc = _fc;
                            if (1 == dc.slave.CompressDfsChunks)
                            {
                                fc = new System.IO.Compression.GZipStream(_fc, System.IO.Compression.CompressionMode.Decompress);
                            }

                            {
                                int xread = StreamReadLoop(fc, fbuf, 4);
                                if (4 == xread)
                                {
                                    int hlen = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(fbuf);
                                    StreamReadExact(fc, fbuf, hlen - 4);
                                }
                            }

                            for (; ; )
                            {
                                int xread = fc.Read(fbuf, 0, MAX_SIZE_PER_RECEIVE);
                                if (xread <= 0)
                                {
                                    break;
                                }
                                fs.Write(fbuf, 0, xread);
                            }

                            fc.Close();
                        }

                        fs.Close();
                        try
                        {
                            System.Security.AccessControl.FileSecurity fsec = new System.Security.AccessControl.FileSecurity();
                            fsec.AddAccessRule(rule);
                            System.IO.File.SetAccessControl(localfullname, fsec);
                        }
                        catch(Exception e)
                        {
                            Console.Error.WriteLine("Error while assigning file permission to: {0}\\{1}", userdomain, dousername);
                            Console.Error.WriteLine(e.ToString());
                        }                        
                    }
                    //if (verbose)
                    {
                        Console.Write('*');
                        ConsoleFlush();
                    }

                }

            }

        }


        static void _DfsGetPartsNoWildcard(string[] args, int startparts, int lastparts)
        {
            if (!dfs.DfsConfigExists(DFSXMLPATH))
            {
                Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                SetFailure();
                return;
            }
            if (args.Length < 2)
            {
                Console.Error.WriteLine("dfs get error:  {0} dfs get <dfspath> <localpath>", appname);
                SetFailure();
                return;
            }

            string dfspath = args[0];
            if (dfspath.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                dfspath = dfspath.Substring(6);
            }
            if (dfspath.Length == 0)
            {
                Console.Error.WriteLine("Invalid dfspath");
                SetFailure();
                return;
            }

            string localpath = args[1];

            //System.Threading.Thread.Sleep(8000);
            string localstartname = localpath;
            string localendname = "";
            { // Remove ext...
                int ildot = localstartname.LastIndexOf('.');
                if (-1 != ildot)
                {
                    bool anotherdotcheck = (localstartname.Length - ildot <= 4);
                    localstartname = localstartname.Substring(0, ildot);
                    if (anotherdotcheck)
                    {
                        ildot = localstartname.LastIndexOf('.');
                        if (-1 != ildot)
                        {
                            if (localstartname.Length - ildot <= 4)
                            {
                                localstartname = localstartname.Substring(0, ildot);
                            }
                        }
                    }
                }
            }
            localendname = localpath.Substring(localstartname.Length); // e.g. ".txt.gz"
            if (!localendname.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
            {
                localendname += ".gz";
            }
            localstartname += ".part";

            {
                string edir = ".";
                string efile = localstartname + "*" + localendname;
                {
                    int ilslash = efile.LastIndexOfAny(new char[] { '/', '\\' });
                    if (-1 != ilslash)
                    {
                        edir = efile.Substring(0, ilslash);
                        efile = efile.Substring(ilslash + 1);
                    }
                }
                string[] existparts = System.IO.Directory.GetFiles(edir, efile);
                if (existparts.Length != 0)
                {
                    /*
                    Console.Error.WriteLine("Abort: output files already exist");
                    SetFailure();
                    return;
                     * */
                    if (QuietMode || !InteractiveMode)
                    {
                        Console.Error.WriteLine("The specified file(s) already exists locally: {0}", localstartname + "*" + localendname);
                        SetFailure();
                        return;
                    }
                    Console.Write("The specified file(s) already exists locally; overwrite? ");
                    ConsoleFlush();
                    for (; ; )
                    {
                        string s = Console.ReadLine();
                        char ch = '\0';
                        if (0 != s.Length)
                        {
                            ch = char.ToUpper(s[0]);
                        }
                        if ('N' == ch)
                        {
                            Console.WriteLine("    Aborted by user");
                            return;
                        }
                        else if ('Y' == ch)
                        {
                            break; // !
                        }
                        else
                        {
                            Console.Write("Overwrite, yes or no? ");
                            ConsoleFlush();
                        }
                    }

                    // Delete them before writing; new output might have less part count.
                    for (int i = 0; i < existparts.Length; i++)
                    {
                        System.IO.File.Delete(existparts[i]);
                    }
                }
            }

            int ncpus = Surrogate.NumberOfProcessors;

            dfs dc = LoadDfsConfig();
            dfs.DfsFile df = DfsFind(dc, dfspath);
            if (null == df)
            {
                Console.Error.WriteLine("Error: File not found in DFS: {0}", dfspath);
                SetFailure();
                return;
            }

            Console.WriteLine("Getting GZip parts of DFS file '{1}'...", df.Nodes.Count, df.Name);

            List<System.Threading.Thread> threads = new List<System.Threading.Thread>(ncpus);
            GettingParts gp = new GettingParts();
            gp.dc = dc;
            //gp.parts = df.Nodes;
            gp.parts = new List<dfs.DfsFile.FileNode>();
            bool hitlimit = false;
            for (int dn = 0; dn < df.Nodes.Count; dn++)
            {
                if (dn > lastparts)
                {
                    hitlimit = true;
                    break;
                }
                gp.parts.Add(df.Nodes[dn]);
            }
            gp.nextpart = startparts;
            gp.localendname = localendname;
            gp.localstartname = localstartname;
            {
                System.Security.Principal.NTAccount nt = new System.Security.Principal.NTAccount(userdomain, dousername);
                System.Security.Principal.SecurityIdentifier secID = (System.Security.Principal.SecurityIdentifier)nt.Translate(typeof(System.Security.Principal.SecurityIdentifier));
                System.Security.AccessControl.FileSystemAccessRule rule = new System.Security.AccessControl.FileSystemAccessRule(secID,
                System.Security.AccessControl.FileSystemRights.FullControl,
                System.Security.AccessControl.AccessControlType.Allow);
                gp.rule = rule;
            }

            for (int i = 0; i < ncpus; i++)
            {
                System.Threading.Thread thd = new System.Threading.Thread(new System.Threading.ThreadStart(gp.threadproc));
                thd.Start();
                threads.Add(thd);
            }
            for (int i = 0; i < threads.Count; i++)
            {
                threads[i].Join();
            }
            //if (verbose)
            {
                if (hitlimit)
                {
                    Console.WriteLine("Done with partial get {0}-{1}", startparts, lastparts);
                }
                else
                {
                    Console.WriteLine("Done");
                }
            }
            Console.WriteLine("Files written to '{0}N{1}'", localstartname, localendname);

        }


        static char[] wcchars = new char[] { '*', '?' };

        public static void DfsGet(string[] args)
        {
            int startparts = 0;
            int lastparts = int.MaxValue;

            if (args.Length == 3
                && args[0].StartsWith("parts=", StringComparison.OrdinalIgnoreCase))
            {
                string sp = args[0].Substring(6);
                int idash = sp.IndexOf('-');
                if (-1 == idash)
                {
                    startparts = int.Parse(sp);
                    lastparts = startparts;
                }
                else
                {
                    startparts = int.Parse(sp.Substring(0, idash));
                    sp = sp.Substring(idash + 1);
                    if (sp.Length > 0) // otherwise "N-" is N-<all-the-rest>
                    {
                        lastparts = int.Parse(sp);
                    }
                    if (lastparts < startparts || startparts < 0)
                    {
                        Console.Error.WriteLine("dfs get error: parts={0}-{1} range error", startparts, lastparts);
                        SetFailure();
                        return;
                    }
                }
                args = SubArray(args, 1);
            }

            if (args.Length < 2)
            {
                Console.Error.WriteLine("dfs get error:  {0} dfs get <dfspath> <localpath>", appname);  
                SetFailure();
                return;
            }

            if (-1 != args[0].IndexOfAny(wcchars))
            {
                _DfsGetWildcard(args, startparts, lastparts);
            }
            else
            {
                _DfsGetNoWildcard(args, startparts, lastparts);
            }
        }


        static void _DfsGetWildcard(string[] args, int startparts, int lastparts)
        {
            string localpath = args[1];
            EnsureNetworkPath(localpath);
            if (!System.IO.Directory.Exists(localpath))
            {
                Console.Error.WriteLine("dfs get error:  local directory is not found: {0}", localpath);
                SetFailure();
                return;
            }
            if (!localpath.EndsWith(@"\"))
            {
                localpath = localpath + @"\";
            }            

            string[] newargs = new string[2];
            string srex = Surrogate.WildcardRegexString(args[0]);
            System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            {
                dfs dc = LoadDfsConfig();
                foreach (dfs.DfsFile df in dc.Files)
                {
                    if (rex.IsMatch(df.Name))
                    {
                        newargs[0] = "dfs://" + df.Name;
                        newargs[1] = localpath + df.Name;
                        _DfsGetNoWildcard(newargs, startparts, lastparts);
                    }
                }
            }
        }


        // <dfspath> <localpath>
        static void _DfsGetNoWildcard(string[] args, int startparts, int lastparts)
        {
            //System.Threading.Thread.Sleep(8000);
            if (!dfs.DfsConfigExists(DFSXMLPATH))
            {
                Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                SetFailure();
                return;
            }
            if (args.Length < 2)
            {
                Console.Error.WriteLine("dfs get error:  {0} dfs get <dfspath> <localpath>", appname);
                SetFailure();
                return;
            }
            {
                string dfspath = args[0];
                if (dfspath.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    dfspath = dfspath.Substring(6);
                }
                if (dfspath.Length == 0)
                {
                    Console.Error.WriteLine("Invalid dfspath");
                    SetFailure();
                    return;
                }

                string localpath = args[1];

                EnsureNetworkPath(localpath);

                if (localpath.EndsWith(".gz"))
                {
                    //Console.Error.WriteLine("Error: dfs get not supported for GZip (.gz) files; use dfs getgzip");
                    //SetFailure();
                    _DfsGetPartsNoWildcard(args, startparts, lastparts);
                    return;
                }

                if (System.IO.File.Exists(localpath))
                {
                    if (QuietMode || !InteractiveMode)
                    {
                        Console.Error.WriteLine("The specified file already exists locally: {0}", localpath);
                        SetFailure();
                        return;
                    }
                    Console.Write("The specified file already exists locally; overwrite? ");
                    ConsoleFlush();
                    for (; ; )
                    {
                        string s = Console.ReadLine();
                        char ch = '\0';
                        if (0 != s.Length)
                        {
                            ch = char.ToUpper(s[0]);
                        }
                        if ('N' == ch)
                        {
                            Console.WriteLine("    Aborted by user");
                            return;
                        }
                        else if ('Y' == ch)
                        {
                            break; // !
                        }
                        else
                        {
                            Console.Write("Overwrite, yes or no? ");
                            ConsoleFlush();
                        }
                    }
                }

                const int MAX_SIZE_PER_RECEIVE = 0x400 * 64;
                byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];

                dfs dc = LoadDfsConfig();

                dfs.DfsFile dfsf = DfsFindAny(dc, dfspath);
                if (null == dfsf)
                {
                    Console.Error.WriteLine("Error:  The specified file '{0}' does not exist in DFS", dfspath);
                    SetFailure();
                    return;
                }
                if (0 != string.Compare(dfsf.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase)
                    && 0 != string.Compare(dfsf.Type, DfsFileTypes.BINARY_RECT, StringComparison.OrdinalIgnoreCase))
                {
                    Console.Error.WriteLine("DFS file '{0}' is not of expected type", dfsf.Name);
                    SetFailure();
                    return;
                }
                bool warned = false;
                long curfilepos = 0;
                using (System.IO.FileStream _fs = new System.IO.FileStream(localpath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                {
                    System.IO.Stream fs = _fs;
                    /*if (localpath.EndsWith(".gz"))
                    {
                        //if (verbose)
                        {
                            Console.WriteLine("Warning:  GZip (.gz) does not suport files over 4 GB in size; use 'dfs getgzip' if large file support is ever needed");
                        }

                        fs = new System.IO.Compression.GZipStream(_fs, System.IO.Compression.CompressionMode.Compress);
                    }*/

                    bool hitlimit = false;
                    for (int dn = startparts; dn < dfsf.Nodes.Count; dn++)
                    {
                        if (dn > lastparts)
                        {
                            //if (verbose)
                            {
                                Console.WriteLine("    Done with partial get {0}-{1}", startparts, lastparts);
                            }
                            hitlimit = true;
                            break;
                        }
                        dfs.DfsFile.FileNode node = dfsf.Nodes[dn];
                        if (curfilepos != node.Position)
                        {
                            if (-1 != node.Position)
                            {
                                if (!warned)
                                {
                                    warned = true;
                                    if (startparts != 0)
                                    {
                                        LogOutput("1 or more parts of the file are not received.  Index of parts for this file starts from 0 to " + (dfsf.Nodes.Count - 1).ToString());
                                    }
                                    else
                                    {
                                        LogOutput("Warning: file positions do not line up; " + dfs.DFSXMLNAME + " or data-node chunks may be corrupted");
                                    }                                    
                                    //return;
                                }
                            }
                        }
                        using (System.IO.Stream _fc = new DfsFileNodeStream(node, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                        {
                            System.IO.Stream fc = _fc;
                            if (1 == dc.slave.CompressDfsChunks)
                            {
                                fc = new System.IO.Compression.GZipStream(_fc, System.IO.Compression.CompressionMode.Decompress);
                            }

                            {
                                int xread = StreamReadLoop(fc, fbuf, 4);
                                if (4 == xread)
                                {
                                    int hlen = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(fbuf);
                                    StreamReadExact(fc, fbuf, hlen - 4);
                                }
                            }

                            for (; ; )
                            {
                                int xread = fc.Read(fbuf, 0, MAX_SIZE_PER_RECEIVE);
                                if (xread <= 0)
                                {
                                    break;
                                }
                                fs.Write(fbuf, 0, xread);
                                curfilepos += xread;
                            }

                            fc.Close();
                        }
                    }
                    if (!hitlimit)
                    {
                        if (curfilepos != dfsf.Size)
                        {
                            if (-1 != dfsf.Size)
                            {
                                if (!warned)
                                {
                                    warned = true;
                                    if (startparts != 0)
                                    {
                                        LogOutput("1 or more parts of the file are not received.  Index of parts for this file starts from 0 to " + (dfsf.Nodes.Count - 1).ToString());
                                    }
                                    else
                                    {
                                        LogOutput("Warning: file data received is not correct resulting size; " + dfs.DFSXMLNAME + " or data-node chunks may be corrupted");
                                    }
                                    //return;
                                }
                            }
                        }
                    }
                    fs.Close();

                    try
                    {
                        System.Security.Principal.NTAccount nt = new System.Security.Principal.NTAccount(userdomain, dousername);
                        System.Security.Principal.SecurityIdentifier secID = (System.Security.Principal.SecurityIdentifier)nt.Translate(typeof(System.Security.Principal.SecurityIdentifier));
                        System.Security.AccessControl.FileSystemAccessRule rule = new System.Security.AccessControl.FileSystemAccessRule(secID,
                            System.Security.AccessControl.FileSystemRights.FullControl,
                            System.Security.AccessControl.AccessControlType.Allow);
                        System.Security.AccessControl.FileSecurity fsec = new System.Security.AccessControl.FileSecurity();
                        fsec.AddAccessRule(rule);
                        System.IO.File.SetAccessControl(localpath, fsec);
                    }
                    catch(Exception e)
                    {
                        Console.Error.WriteLine("Error while assigning file permission to: {0}\\{1}", userdomain, dousername);
                        Console.Error.WriteLine(e.ToString());
                    }                    
                }
                Console.WriteLine("Received {0} bytes to file {1}", curfilepos, localpath);
            }
        }


        public static void DfsBulkGet(string[] args)
        {
            string outputfilepath = args[0]; // Lines of "<host>[;<...>] <chunkname> <size>" but <size> must exclude the size of the header.
            string dfspath = args[1];

            dfs.DfsFile df;
            using (LockDfsMutex())
            {
                dfs dc = LoadDfsConfig();
                df = dc.FindAny(dfspath);
                if (null == df)
                {
                    Console.Error.WriteLine("File not found in DFS: {0}", dfspath);
                    SetFailure();
                    return;
                }
            }

            using (System.IO.StreamWriter sw = System.IO.File.CreateText(outputfilepath))
            {
                foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                {
                    sw.WriteLine("{0} {1} {2}", fn.Host, fn.Name, fn.Length);
                }
            }

            Console.WriteLine("dfs://{0}", df.Name);
            Console.WriteLine("{0} {1} {2}", df.Size, df.Type, df.RecordLength);
        }

        public static void DfsShuffle(string[] args)
        {
            if (!dfs.DfsConfigExists(DFSXMLPATH))
            {
                Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                SetFailure();
                return;
            }

            if (args.Length < 2)
            {
                Console.Error.WriteLine("dfs shuffle error:  {0} dfs get <source dfspath> <target dfspath>", appname);
                SetFailure();
                return;
            }

            string sourcefn = args[0];
            if (sourcefn.StartsWith(@"dfs:\\", StringComparison.OrdinalIgnoreCase))
            {
                sourcefn = sourcefn.Substring(6);
            }

            dfs dc = LoadDfsConfig();
            dfs.DfsFile dfssf = DfsFindAny(dc, sourcefn);           
            if (null == dfssf)
            {
                Console.Error.WriteLine("Error:  The specified file '{0}' does not exist in DFS", sourcefn);
                SetFailure();
                return;
            }
            if (0 != string.Compare(dfssf.Type, DfsFileTypes.BINARY_RECT, StringComparison.OrdinalIgnoreCase))
            {
                Console.Error.WriteLine("Shuffle only supports binary dfs file", dfssf.Name);
                SetFailure();
                return;
            }

            string targetfn = args[1];
            if (targetfn.StartsWith(@"dfs:\\", StringComparison.OrdinalIgnoreCase))
            {
                targetfn = targetfn.Substring(6);
            }
            {
                dfs.DfsFile dfstf = DfsFindAny(dc, targetfn);
                if (dfstf != null)
                {
                    Console.Error.WriteLine("Error:  The specified target file '{0}' already exists in DFS", targetfn);
                    SetFailure();
                    return;
                }
            }

            string[] chunkhosts = new string[dfssf.Nodes.Count];
            for (int ni = 0; ni < dfssf.Nodes.Count; ni++)
            {
                dfs.DfsFile.FileNode fn = dfssf.Nodes[ni];
                string[] thishosts = fn.Host.Split(';');
                chunkhosts[ni] = thishosts[0];
            }

            Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
            for (int hi = 0; hi < chunkhosts.Length; hi++)
            {
                int rindex = rnd.Next(0, Int32.MaxValue) % chunkhosts.Length;
                string oldhost = chunkhosts[hi];
                chunkhosts[hi] = chunkhosts[rindex];
                chunkhosts[rindex] = oldhost;
            }

            StringBuilder sbchunkhosts = new StringBuilder();
            foreach (string ch in chunkhosts)
            {
                if (sbchunkhosts.Length > 0)
                {
                    sbchunkhosts.Append(",");
                }
                sbchunkhosts.Append("@\"");
                sbchunkhosts.Append(ch);
                sbchunkhosts.Append("\"");
            }

            string guid = Guid.NewGuid().ToString();
            string root = AELight.AELight_Dir.Replace(':', '$');
            string outputfn = "shuffle_output_" + guid;
            string tempfnpost = ".$" + guid + ".$" + System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
            string jobsfn = "shuffle_job.xml" + tempfnpost;
            try
            {
                using (System.IO.StreamWriter sw = System.IO.File.CreateText(jobsfn))
                {
                    sw.Write((@"
<SourceCode>
  <Jobs>
     <Job Name=`Shuffle` Custodian=`` Email=`` Description=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO_Multi>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://" + outputfn + @"_####.txt</DFSWriter>
          <Mode>ALL MACHINES</Mode>
        </DFS_IO_Multi>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {      
                string[] hosts = new string[]{" + sbchunkhosts.ToString() + @"};
                string root = @`" + root + @"`;
                string tempfile = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
                Shell(@`qizmt bulkget ` + tempfile + ` " + sourcefn + @"`);
                
                using(System.IO.StreamReader reader = new System.IO.StreamReader(tempfile))
                {
                    int linepos = 0;                   
                    string line = reader.ReadLine();                    
                    while(line != null)
                    {
                        if(linepos % StaticGlobals.Qizmt_BlocksTotalCount == StaticGlobals.Qizmt_BlockID)
                        {
                            string[] parts = line.Split(' ');
                            string[] chunkhosts = parts[0].Split(';');
                            string chunkname = parts[1];
                            string size = parts[2];                            
                            string newhost = hosts[linepos];
                            string newchunkname = GenerateDataNodeName(`shuffle`, `zd.`, `.zd`);
                            
                            string copyfrom = @`\\` + chunkhosts[0] + @`\` + root + @`\` + chunkname;
                            string copyto = @`\\` + newhost + @`\` + root + @`\` + newchunkname;
                            int CookTimeout = 1000 * 60;
                            int CookRetries = 64;
                            int cooking_cooksremain = CookRetries;
                            for (; ; ) // Cooking loop.
                            {
                                try
                                {
                                    System.IO.File.Copy(copyfrom, copyto, true); // overwrite=true
                                }
                                catch (Exception e)
                                {                                    
                                    if (cooking_cooksremain-- <= 0)
                                    {
                                        throw new System.IO.IOException(`cooked too many times (retries=`
                                            + CookRetries.ToString()
                                            + `; timeout=` + CookTimeout.ToString()
                                            + `) on ` + copyfrom, e);
                                    }
                                    System.Threading.Thread.Sleep(MySpace.DataMining.DistributedObjects.IOUtils.RealRetryTimeout(CookTimeout));                                                    
                                    continue; // !
                                }
                                break;
                            }                            
                            dfsoutput.WriteLine(newhost + ` ` + newchunkname + ` ` + size);                            
                        }
                        line = reader.ReadLine();
                        linepos++;
                    }    
                }                
                System.IO.File.Delete(tempfile);              
            }
            
            public static string GenerateDataNodeName(string dfspath, string prefix, string suffix)
            {
                StringBuilder sbfnn = new StringBuilder(32);
                sbfnn.Append(prefix);
                {
                    int j = 0;
                    for (int i = 0; i < dfspath.Length; i++)
                    {
                        if (char.IsLetterOrDigit(dfspath[i]))
                        {
                            sbfnn.Append(dfspath[i]);
                            if (++j >= 10)
                            {
                                break;
                            }
                        }
                    }
                }
                sbfnn.Append('.');
                sbfnn.Append(Guid.NewGuid().ToString());
                sbfnn.Append(suffix);
                return sbfnn.ToString();
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`bulkput` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                string outfn = `" + outputfn + @"`;
                string[] lines = Shell(@`Qizmt ls ` + outfn + `*`).Split('\n');
              
                List<string> outputfiles = new List<string>();
                foreach(string line in lines)
                {
                    string _line = line.Trim();
                    if(_line.StartsWith(outfn))
                    {
                        string[] parts = _line.Split(' ');
                        outputfiles.Add(parts[0].Trim());    
                    }
                }
                
                outputfiles.Sort();         
                
                List<string[]> newchunks = new List<string[]>();
                foreach(string outputfile in outputfiles)
                {
                    string tempfile = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
                    Shell(`Qizmt get ` + outputfile + ` ` + tempfile);
                    newchunks.Add(System.IO.File.ReadAllLines(tempfile));
                    System.IO.File.Delete(tempfile);                    
                }             

                Shell(@`Qizmt del ` + outfn + `*`);
                
                {
                    string tempfile = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
                    int curlinepos = 0;
                    int curfilepos = 0;
                    using(System.IO.StreamWriter writer = new System.IO.StreamWriter(tempfile))
                    {
                        for(;;)
                        {
                            if(curlinepos >= newchunks[curfilepos].Length)
                            {
                                break;
                            }
                            writer.WriteLine(newchunks[curfilepos][curlinepos]);
                            
                            curfilepos++;
                            if(curfilepos >= newchunks.Count)
                            {
                                curfilepos = 0;
                                curlinepos++;
                            }
                        }     
                        writer.Close();
                    }  
                    
                    Shell(`Qizmt bulkput ` + tempfile + ` ` + ` " + targetfn + @" rbin@" + dfssf.RecordLength.ToString() + @"`);
                    System.IO.File.Delete(tempfile);
                }                 
            }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
").Replace("`", "\""));
                }
                Console.WriteLine("Shuffling {0}...", sourcefn);
                Exec("", LoadConfig(jobsfn), new string[] { }, false, false);
                Console.WriteLine("Done");
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
            }

            {
                

            }
        }

        public static void DfsBulkPut(string[] args)
        {
            string inputfilepath = args[0]; // Lines of "<host> <chunkname> <size>" but <size> must exclude the size of the header.
            string newprettyfilename = args[1];
            string filetype = args[2];

            // Can probably only support file types that don't have samples.
            if (!filetype.StartsWith(DfsFileTypes.BINARY_RECT, true, null))
            {
                throw new Exception("DfsBulkPut: must be of type " + DfsFileTypes.BINARY_RECT);
            }

            List<string> partsinfo = new List<string>();
            using (System.IO.StreamReader sr = System.IO.File.OpenText(inputfilepath))
            {
                for (; ; )
                {
                    string pn = sr.ReadLine();
                    if (null == pn)
                    {
                        break;
                    }
                    if (0 != pn.Length)
                    {
                        partsinfo.Add(pn);
#if DEBUG
                        {
                            string[] x = pn.Split(' ');
                            string xhost = x[0];
                            string xchunkname = x[1];
                            long xchunksize = long.Parse(x[2]);
                            if (-1 != xhost.IndexOf(';'))
                            {
                                throw new Exception("DEBUG:  cannot specify multiple hosts: " + xhost);
                            }
                            if(!System.IO.File.Exists(Surrogate.NetworkPathForHost(xhost) + @"\" + xchunkname))
                            {
                                throw new Exception("DEBUG:  (!System.IO.File.Exists(\"" + Surrogate.NetworkPathForHost(xhost) + @"\" + xchunkname + "\"))");
                            }
                            if (xchunksize < 0)
                            {
                                throw new Exception("DEBUG:  chunk size is negative: " + xchunksize.ToString());
                            }
                        }
#endif
                    }
                }
            }
            string[] slaves;
            {
                dfs.DfsFile df = new dfs.DfsFile();
                df.Nodes = new List<dfs.DfsFile.FileNode>(partsinfo.Count);
                df.Name = ".$" + newprettyfilename + ".$replicating-" + Guid.NewGuid().ToString();
                df.Type = filetype;
                using (LockDfsMutex())
                {
                    dfs dc = LoadDfsConfig();
                    if (null != dc.FindAny(newprettyfilename))
                    {
                        Console.Error.WriteLine("Output file already exists: " + newprettyfilename);
                        SetFailure();
                        return;
                    }
                    {
                        slaves = dc.Slaves.SlaveList.Split(',', ';');
                        if (null == dc.Slaves.SlaveList || dc.Slaves.SlaveList.Length == 0 || slaves.Length < 1)
                        {
                            Console.Error.WriteLine("SlaveList expected in configuration (no machines)");
                            SetFailure();
                            return;
                        }
                        if (dc.Replication > 1)
                        {
                            slaves = ExcludeUnhealthySlaveMachines(slaves, true).ToArray();
                        }
                        if (0 == slaves.Length)
                        {
                            Console.Error.WriteLine("No healthy machines for DFS put");
                            SetFailure();
                            return;
                        }
                    }
                    checked
                    {
                        long TotalSize = 0;
                        for (int ipi = 0; ipi < partsinfo.Count; ipi++)
                        {
                            string[] x = partsinfo[ipi].Split(' ');
                            string xhost = x[0];
                            string xchunkname = x[1];
                            long xchunksize = long.Parse(x[2]);
                            dfs.DfsFile.FileNode fn = new dfs.DfsFile.FileNode();
                            fn.Position = ipi;
                            fn.Host = xhost;
                            fn.Name = xchunkname;
                            fn.Length = xchunksize;
                            TotalSize += xchunksize;
                            df.Nodes.Add(fn);
                        }
                        df.Size = TotalSize;
                    }
                    dc.Files.Add(df);
                    UpdateDfsXml(dc);
                }
                ReplicationPhase(df.Name, false, 0, slaves);
                using (LockDfsMutex())
                {
                    dfs dc = LoadDfsConfig(); // Reload in case of intermediate change.
                    dfs.DfsFile dfu = dc.FindAny(df.Name);
                    if (null != dfu)
                    {
                        if (null != DfsFindAny(dc, newprettyfilename))
                        {
                            Console.Error.WriteLine("Output file already exists: " + newprettyfilename);
                            SetFailure();
                            return;
                        }
                        dfu.Name = newprettyfilename;
                        UpdateDfsXml(dc);
                    }
                }
            }
        }


        public static void DfsDeleteMt(string dfspath, bool verbose)
        {
            _DfsDelete_mt(dfspath, verbose);
        }

        public static void DfsMetaDelete(string dfspath)
        {
            string srex = Surrogate.WildcardRegexString(dfspath);
            System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            List<dfs.DfsFile> delfiles = new List<dfs.DfsFile>();
            using (LockDfsMutex())
            {
                dfs dc = LoadDfsConfig();
                List<dfs.DfsFile> newfiles = new List<dfs.DfsFile>(dc.Files.Count);
                // Note: also loop for non-wildcards to fix any anomalies.
                for (int i = 0; i < dc.Files.Count; i++)
                {
                    dfs.DfsFile df = dc.Files[i];
                    if (rex.IsMatch(df.Name))
                    {
                        delfiles.Add(df);
                    }
                    else
                    {
                        newfiles.Add(df);
                    }
                }
                if (delfiles.Count == 0)
                {
                    // Nothing to do, so just bail out.
                    return;
                }
                dc.Files = newfiles;
                UpdateDfsXml(dc);
            }

            foreach (dfs.DfsFile df in delfiles)
            {
                Console.WriteLine("Meta information deleted for file '{0}' ({1} parts)", df.Name, df.Nodes.Count);
            }
        }


        public static void DfsDelete(string dfspath)
        {
            DfsDelete(dfspath, true);
        }

        public static void DfsDelete(string dfspath, bool verbose)
        {
            _DfsDelete(dfspath, verbose);
        }

        public static void DfsDelete(List<dfs.DfsFile> files, bool verbose)
        {
            _DfsDelete_mt(files, verbose);
        }

        public static void _KillDllFileChunks_unlocked(dfs.DfsFile dfsdll, bool verbose)
        {
            dfs dc = LoadDfsConfig(); // Just to get the slave list.
            string[] slaves = dc.Slaves.SlaveList.Split(',', ';');
            for (int si = 0; si < slaves.Length; si++)
            {
                try
                {
                    string slave = slaves[si];
                    string netpath = Surrogate.NetworkPathForHost(slave);
                    string cacpath = netpath + @"\" + dfs.DLL_DIR_NAME;
                    System.IO.File.Delete(cacpath + dfsdll.Name);
                }
                catch
                {
                }
            }
        }

        public static void _KillDllFileChunks_unlocked_mt(IList<dfs.DfsFile> dfsdlls, bool verbose)
        {
            dfs dc = LoadDfsConfig(); // Just to get the slave list.
            string[] slaves = dc.Slaves.SlaveList.Split(',', ';');

            MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                new Action<string>(
                delegate(string slave)
                {
                    string netpath = Surrogate.NetworkPathForHost(slave);
                    string cacpath = netpath + @"\" + dfs.DLL_DIR_NAME;
                    for (int idll = 0; idll < dfsdlls.Count; idll++)
                    {
                        try
                        {
                            System.IO.File.Delete(cacpath + @"\" + dfsdlls[idll].Name);
                        }
                        catch
                        {
                        }
                    }
                }), slaves, slaves.Length);



            if (verbose)
            {
                for (int di = 0; di < dfsdlls.Count; di++)
                {
                    Console.WriteLine("Successfully deleted DLL '{0}'", dfsdlls[di].Name);
                }
            }
        }


        // Note: metabackupdir can be null if no meta backup.
        static void _DfsKillFileChunks_unlocked(dfs.DfsFile dfsf, string metabackupdir, bool verbose)
        {
            if (0 == string.Compare(DfsFileTypes.DELTA, dfsf.Type, true))
            {
                _KillSnowballFileChunks_unlocked(dfsf, verbose);
            }
            else if (0 == string.Compare(DfsFileTypes.DLL, dfsf.Type, true))
            {
                _KillDllFileChunks_unlocked(dfsf, verbose);
            }
            else if (0 == string.Compare(DfsFileTypes.JOB, dfsf.Type, true))
            {
                for (int dn = 0; dn < dfsf.Nodes.Count; dn++)
                {
                    foreach (string chost in dfsf.Nodes[dn].Host.Split(';'))
                    {
                        try
                        {
                            System.IO.File.Delete(NetworkPathForHost(chost) + @"\" + dfsf.Nodes[dn].Name);
                            if (null != metabackupdir)
                            {
                                System.IO.File.Delete(metabackupdir + @"\" + dfsf.Nodes[dn].Name);
                            }
                        }
                        catch
                        {
                        }
                    }
                }

                if (verbose)
                {
                    Console.WriteLine("Successfully deleted file '{0}' ({1} parts)", dfsf.Name, dfsf.Nodes.Count);
                }

            }
            else
            {
                for (int dn = 0; dn < dfsf.Nodes.Count; dn++)
                {
                    foreach (string chost in dfsf.Nodes[dn].Host.Split(';'))
                    {
                        try
                        {
                            System.IO.File.Delete(NetworkPathForHost(chost) + @"\" + dfsf.Nodes[dn].Name);
                            System.IO.File.Delete(NetworkPathForHost(chost) + @"\" + dfsf.Nodes[dn].Name + ".zsa");
                        }
                        catch
                        {
                        }
                    }
                }

                if (verbose)
                {
                    Console.WriteLine("Successfully deleted file '{0}' ({1} parts)", dfsf.Name, dfsf.Nodes.Count);
                }

            }
        }

        // Note: metabackupdir can be null if no meta backups.
        static void _KillJobFileChunks_unlocked_mt(List<dfs.DfsFile> delfiles, string metabackupdir, bool verbose)
        {
            if (delfiles.Count == 0)
            {
                return;
            }

            List<string> fnodes = new List<string>();
            for (int di = 0; di < delfiles.Count; di++)
            {
                dfs.DfsFile dfsf = delfiles[di];

                //Collect file node paths.
                for (int dn = 0; dn < dfsf.Nodes.Count; dn++)
                {
                    foreach (string chost in dfsf.Nodes[dn].Host.Split(';'))
                    {
                        fnodes.Add(NetworkPathForHost(chost) + @"\" + dfsf.Nodes[dn].Name);
                        if (null != metabackupdir)
                        {
                            fnodes.Add(metabackupdir + @"\" + dfsf.Nodes[dn].Name);
                        }
                    }
                }
            }

            int dist = 15;
            int maxThread = 15;

            if (fnodes.Count > 0)
            {
                int nThreads = fnodes.Count / dist;

                if (nThreads * dist < fnodes.Count)
                {
                    nThreads++;
                }

                if (nThreads > maxThread)
                {
                    nThreads = maxThread;
                }

                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                new Action<string>(
                delegate(string fnode)
                {
                    try
                    {
                        System.IO.File.Delete(fnode);
                    }
                    catch
                    {
                    }
                }
                ), fnodes, nThreads);
            }

            if (verbose)
            {
                for (int di = 0; di < delfiles.Count; di++)
                {
                    Console.WriteLine("Successfully deleted file '{0}' ({1} parts)", delfiles[di].Name, delfiles[di].Nodes.Count);
                }
            }
        }

        static void _KillDataFileChunks_unlocked_mt(List<dfs.DfsFile> delfiles, bool verbose)
        {
            if (delfiles.Count == 0)
            {
                return;
            }

            List<string> fnodes = new List<string>();
            for (int di = 0; di < delfiles.Count; di++)
            {
                dfs.DfsFile dfsf = delfiles[di];

                //Collect file node paths.
                for (int dn = 0; dn < dfsf.Nodes.Count; dn++)
                {
                    foreach (string chost in dfsf.Nodes[dn].Host.Split(';'))
                    {
                        fnodes.Add(NetworkPathForHost(chost) + @"\" + dfsf.Nodes[dn].Name);
                    }
                }
            }

            int dist = 15;
            int maxThread = 15;

            if (fnodes.Count > 0)
            {
                int nThreads = fnodes.Count / dist;

                if (nThreads * dist < fnodes.Count)
                {
                    nThreads++;
                }

                if (nThreads > maxThread)
                {
                    nThreads = maxThread;
                }

                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                new Action<string>(
                delegate(string fnode)
                {
                    try
                    {
                        System.IO.File.Delete(fnode);
                        System.IO.File.Delete(fnode + ".zsa");
                    }
                    catch
                    {
                    }
                }
                ), fnodes, nThreads);
            }

            if (verbose)
            {
                for (int di = 0; di < delfiles.Count; di++)
                {
                    Console.WriteLine("Successfully deleted file '{0}' ({1} parts)", delfiles[di].Name, delfiles[di].Nodes.Count);
                }
            }
        }

        static void _DfsDelete(string dfspath, bool verbose)
        {
            if (dfspath.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                dfspath = dfspath.Substring(6);
            }

            string srex = Surrogate.WildcardRegexString(dfspath);
            System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            List<dfs.DfsFile> delfiles = new List<dfs.DfsFile>();
            string metabackupdir = null;
            using (LockDfsMutex())
            {
                dfs dc = LoadDfsConfig();
                metabackupdir = dc.GetMetaBackupLocation();
                List<dfs.DfsFile> newfiles = new List<dfs.DfsFile>(dc.Files.Count);
                // Note: also loop for non-wildcards to fix any anomalies.
                for (int i = 0; i < dc.Files.Count; i++)
                {
                    dfs.DfsFile df = dc.Files[i];
                    if (rex.IsMatch(df.Name))
                    {
                        delfiles.Add(df);
                    }
                    else
                    {
                        newfiles.Add(df);
                    }
                }
                if (delfiles.Count == 0)
                {
                    // Nothing to do, so just bail out.
                    return;
                }
                dc.Files = newfiles;
                UpdateDfsXml(dc);
            }
            for (int di = 0; di < delfiles.Count; di++)
            {
                try
                {
                    _DfsKillFileChunks_unlocked(delfiles[di], metabackupdir, verbose);
                }
                catch
                {
                }
            }
        }

        static void _DfsDelete_mt(string dfspath, bool verbose)
        {
            if (dfspath.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                dfspath = dfspath.Substring(6);
            }

            string srex = Surrogate.WildcardRegexString(dfspath);
            System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            List<dfs.DfsFile> delfiles = new List<dfs.DfsFile>();
            List<dfs.DfsFile> delfilesSB = new List<dfs.DfsFile>();
            List<dfs.DfsFile> delfilesDLL = new List<dfs.DfsFile>();
            List<dfs.DfsFile> delfilesJOB = new List<dfs.DfsFile>();
            string metabackupdir = null;
            using (LockDfsMutex())
            {
                dfs dc = LoadDfsConfig();
                if (dc.Files.Count < 1)
                {
                    // Nothing to do, so just bail out.
                    return;
                }
                metabackupdir = dc.GetMetaBackupLocation();
                List<dfs.DfsFile> newfiles = new List<dfs.DfsFile>(dc.Files.Count);
                // Note: also loop for non-wildcards to fix any anomalies.
                for (int i = 0; i < dc.Files.Count; i++)
                {
                    dfs.DfsFile df = dc.Files[i];
                    if (rex.IsMatch(df.Name))
                    {
                        if (0 == string.Compare(DfsFileTypes.DELTA, df.Type, true))
                        {
                            delfilesSB.Add(df);
                        }
                        else if (0 == string.Compare(DfsFileTypes.DLL, df.Type, true))
                        {
                            delfilesDLL.Add(df);
                        }
                        else if (0 == string.Compare(DfsFileTypes.JOB, df.Type, true))
                        {
                            delfilesJOB.Add(df);
                        }
                        else
                        {
                            delfiles.Add(df);
                        }
                    }
                    else
                    {
                        newfiles.Add(df);
                    }
                }
                dc.Files = newfiles;
                UpdateDfsXml(dc);
            }

            _KillSnowballFileChunks_unlocked_mt(delfilesSB, verbose);
            _KillDllFileChunks_unlocked_mt(delfilesDLL, verbose);
            _KillDataFileChunks_unlocked_mt(delfiles, verbose);
            _KillJobFileChunks_unlocked_mt(delfilesJOB, metabackupdir, verbose);
        }

        private static void _DfsDelete_mt(List<dfs.DfsFile> files, bool verbose)
        {
            List<dfs.DfsFile> delfiles = new List<dfs.DfsFile>();
            List<dfs.DfsFile> delfilesSB = new List<dfs.DfsFile>();
            List<dfs.DfsFile> delfilesDLL = new List<dfs.DfsFile>();
            List<dfs.DfsFile> delfilesJOB = new List<dfs.DfsFile>();
            string metabackupdir = null;

            using (LockDfsMutex())
            {
                dfs dc = LoadDfsConfig();
                if (dc.Files.Count < 1)
                {
                    // Nothing to do, so just bail out.
                    return;
                }
                metabackupdir = dc.GetMetaBackupLocation();
                List<dfs.DfsFile> newfiles = new List<dfs.DfsFile>(dc.Files.Count);

                for (int i = 0; i < dc.Files.Count; i++)
                {
                    dfs.DfsFile df = dc.Files[i];

                    foreach (dfs.DfsFile file in files)
                    {
                        if (string.Compare(df.Name, file.Name, true) == 0)
                        {
                            if (0 == string.Compare(DfsFileTypes.DELTA, df.Type, true))
                            {
                                delfilesSB.Add(df);
                            }
                            else if (0 == string.Compare(DfsFileTypes.DLL, df.Type, true))
                            {
                                delfilesDLL.Add(df);
                            }
                            else if (0 == string.Compare(DfsFileTypes.JOB, df.Type, true))
                            {
                                delfilesJOB.Add(df);
                            }
                            else
                            {
                                delfiles.Add(df);
                            }
                        }
                        else
                        {
                            newfiles.Add(df);
                        }
                    }
                }
                dc.Files = newfiles;
                UpdateDfsXml(dc);
            }

            _KillSnowballFileChunks_unlocked_mt(delfilesSB, verbose);
            _KillDllFileChunks_unlocked_mt(delfilesDLL, verbose);
            _KillDataFileChunks_unlocked_mt(delfiles, verbose);
            _KillJobFileChunks_unlocked_mt(delfilesJOB, metabackupdir, verbose);
        }

        static bool atype = true;

        public static void DfsRename(string[] args)
        {
            using (LockDfsMutex())
            {
                dfs dc = LoadDfsConfig();

                //dfs.DfsFile df = DfsFind(dc, args[0]);
                dfs.DfsFile df = DfsFindAny(dc, args[0]);
                if (null == df)
                {
                    Console.Error.WriteLine("Error:  File to rename not found: {0}", args[0]);
                    SetFailure();
                    return;
                }

                string newdfsname = args[1];
                if (newdfsname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    newdfsname = newdfsname.Substring(6);
                }
                string reason = "";
                if (dfs.IsBadFilename(newdfsname, out reason))
                {
                    Console.Error.WriteLine("Error:  Invalid target file: {0}", reason);
                    SetFailure();
                    return;
                }

                if (null != DfsFindAny(dc, args[1]))
                {
                    Console.Error.WriteLine("Error:  Rename target file already exists: {0}", args[1]);
                    SetFailure();
                    return;
                }               

                df.Name = newdfsname;
                UpdateDfsXml(dc);

                Console.WriteLine("DFS file renamed to: {0}", newdfsname);
            }
        }

        public static void DfsSwap(string[] args)
        {
            using (LockDfsMutex())
            {
                dfs dc = LoadDfsConfig();

                string dfsname1 = args[0];
                string dfsname2 = args[1];

                dfs.DfsFile df1 = DfsFindAny(dc, dfsname1);
                if (null == df1)
                {
                    Console.Error.WriteLine("Error:  File to swap not found: {0}", dfsname1);
                    SetFailure();
                    return;
                }

                dfs.DfsFile df2 = DfsFindAny(dc, dfsname2);
                if (null == df2)
                {
                    Console.Error.WriteLine("Error:  File to swap not found: {0}", dfsname2);
                    SetFailure();
                    return;
                }

                /*if (0 != string.Compare(df1.XFileType, df2.XFileType, StringComparison.OrdinalIgnoreCase))
                {
                    Console.Error.WriteLine("Error:  the files to swap are incompatible");
                    SetFailure();
                    return;
                }*/

                string tempname = df1.Name;
                df1.Name = df2.Name;
                df2.Name = tempname;

                UpdateDfsXml(dc);

                Console.WriteLine("DFS file names swapped: {0} <-> {1}", dfsname1, dfsname2);
            }
        }

        public static void DfsInvalidateCachedFileNode(string[] args)
        {
            using (LockDfsMutex())
            {
                dfs dc = LoadDfsConfig();

                dfs.DfsFile dfCache = DfsFindAny(dc, args[0]);

                if (null == dfCache)
                {
                    Console.Error.WriteLine("Error:  Cache file not found: {0}", args[0]);
                    SetFailure();
                    return;
                }

                string fileNodeName = args[1];

                if (fileNodeName.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                {
                    fileNodeName = fileNodeName.Substring(6);
                }

                dfs.DfsFile.FileNode node = dfCache.FindNode(fileNodeName);

                if (node != null)
                {
                    node.Name = string.Format("{0}_invalidated_{1}_{2}", invalidatedCacheToken, DateTime.UtcNow.ToString("yyyyMMdd-HHmmss"), node.Name);
                    UpdateDfsXml(dc);
                    Console.WriteLine("The file node {0} in cache {1} has been invalidated.", args[1], args[0]);
                }
                else
                {
                    Console.Error.WriteLine("Error:  File node to invalidate not found in cache: {0}", args[1]);
                    SetFailure();
                    return;
                }
            }
        }

        static void Dfs(string cmd, string[] args)
        {
            //using (LockDfsMutex())
            {
                switch (cmd)
                {
                    case "delete":
                    case "del":
                    case "rm":
                    case "delmt":
                    case "delst":
                        if (!dfs.DfsConfigExists(DFSXMLPATH))
                        {
                            Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                            SetFailure();
                            return;
                        }
                        if (args.Length < 1)
                        {
                            Console.Error.WriteLine("Error: {0} command needs argument: <path|wildcard>", cmd);
                            SetFailure();
                            return;
                        }

                        if (string.Compare(cmd, "delst", true) == 0)
                        {
                            DfsDelete(args[0]);  //single threaded.
                        }
                        else
                        {
                            DfsDeleteMt(args[0], true);
                        }

                        break;

                    case "head":
                        {
                            if (!dfs.DfsConfigExists(DFSXMLPATH))
                            {
                                Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                                SetFailure();
                                return;
                            }
                            if (args.Length < 1 || '-' == args[0][0])
                            {
                                Console.Error.WriteLine("Error: dfs head command needs argument: <dfsfile>");
                                SetFailure();
                                return;
                            }
                            string[] specs;
                            if (args[0].StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                            {
                                specs = args[0].Substring(6).Split(':');
                            }
                            else
                            {
                                specs = args[0].Split(':');
                            }
                            dfs dc = LoadDfsConfig();

                            dfs.DfsFile df = null;
                            try
                            {
                                df = DfsFind(dc, specs[0]);
                            }
                            catch (System.IO.IOException e)
                            {
                                SetFailure();
                                return;
                            }

                            if (null == df)
                            {
                                Console.Error.WriteLine("File not found in DFS: {0}", specs[0]);
                                SetFailure();
                                return;
                            }
                            if (df.Nodes.Count > 0)
                            {

                                string shost = "";
                                if (specs.Length >= 2 && specs[1].Length > 0)
                                {
                                    shost = IPAddressUtil.GetName(specs[1]);
                                }

                                string partspec = "";
                                bool foundpart = true;
                                if (specs.Length >= 3)
                                {
                                    partspec = specs[2];
                                    foundpart = false;
                                }

                                int lc = 10;
                                if (args.Length >= 2)
                                {
                                    lc = int.Parse(args[1]);
                                    if (lc <= 0)
                                    {
                                        throw new FormatException("Line count makes no sense");
                                    }
                                }

                                const int MAX_SIZE_PER_RECEIVE = 0x400 * 64;
                                byte[] fbuf = new byte[MAX_SIZE_PER_RECEIVE];

                                foreach (dfs.DfsFile.FileNode node in df.Nodes)
                                {
                                    if (partspec.Length > 0)
                                    {
                                        if (0 == string.Compare(node.Name, partspec, StringComparison.OrdinalIgnoreCase))
                                        {
                                            if (ListContainsHost(node.Host, shost))
                                            {
                                                // Good!..
                                                foundpart = true;
                                            }
                                            else
                                            {
                                                ConsoleFlush();
                                                Console.Error.WriteLine("    Specified data-node chunk does not reside on specified host");
                                                SetFailure();
                                                return;
                                            }
                                        }
                                        else
                                        {
                                            continue;
                                        }
                                    }

                                    //string netpath = NetworkPathForHost(node.Host.Split(';')[0]) + @"\" + node.Name;
                                    using (System.IO.Stream _fc = new DfsFileNodeStream(node, true, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read, FILE_BUFFER_SIZE))
                                    {
                                        System.IO.Stream fc = _fc;
                                        if (1 == dc.slave.CompressDfsChunks)
                                        {
                                            fc = new System.IO.Compression.GZipStream(_fc, System.IO.Compression.CompressionMode.Decompress);
                                        }

                                        {
                                            int xread = StreamReadLoop(fc, fbuf, 4);
                                            if (4 == xread)
                                            {
                                                int hlen = MySpace.DataMining.DistributedObjects.Entry.BytesToInt(fbuf);
                                                StreamReadExact(fc, fbuf, hlen - 4);
                                            }
                                        }

                                        using (System.IO.StreamReader sr = new System.IO.StreamReader(fc))
                                        {
                                            for (; lc > 0; lc--)
                                            {
                                                string s = sr.ReadLine();
                                                if (null == s)
                                                {
                                                    break;
                                                }
                                                Console.WriteLine(s);
                                            }
                                        }
                                    }
                                    if (partspec.Length > 0)
                                    {
                                        break;
                                    }
                                    if (lc <= 0)
                                    {
                                        break;
                                    }
                                }
                                if (lc > 0)
                                {
                                    ConsoleFlush();
                                    if (!foundpart)
                                    {
                                        Console.Error.WriteLine("    Specified data-node chunk not found");
                                    }
                                    else
                                    {
                                        if (partspec.Length > 0)
                                        {
                                            Console.Error.WriteLine("    Hit end of specified data-node chunk");
                                        }
                                        else
                                        {
                                            //Console.Error.WriteLine("    Hit end of file");
                                        }
                                    }
                                }
                            }
                            else
                            {
                                Console.Error.WriteLine("    No data-node chunks");
                            }
                        }
                        break;

                    case "rename":
                    case "ren":
                    case "move":
                    case "mv":
                        if (!dfs.DfsConfigExists(DFSXMLPATH))
                        {
                            Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                            SetFailure();
                            return;
                        }
                        if (args.Length < 2)
                        {
                            Console.Error.WriteLine("Error: dfs rename command needs arguments: <dfspath> <dfspath>");
                            SetFailure();
                            return;
                        }
                        DfsRename(args);
                        break;

                    case "swap":
                        if (!dfs.DfsConfigExists(DFSXMLPATH))
                        {
                            Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                            SetFailure();
                            return;
                        }
                        if (args.Length < 2)
                        {
                            Console.Error.WriteLine("Error: dfs swap command needs arguments: <dfspath> <dfspath>");
                            SetFailure();
                            return;
                        }
                        DfsSwap(args);
                        break;

                    case "countparts":
                        {
                            if (args.Length == 0)
                            {
                                Console.Error.WriteLine("Error: countparts command needs argument: <dfspath>");
                                SetFailure();
                                return;
                            }
                            if (!dfs.DfsConfigExists(DFSXMLPATH))
                            {
                                Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                                SetFailure();
                                return;
                            }
                            string dfsfilename = args[0];
                            dfs dc = LoadDfsConfig();
                            dfs.DfsFile df = DfsFindAny(dc, dfsfilename);
                            if (null == df)
                            {
                                Console.Error.WriteLine("No such file: {0}", dfsfilename);
                                return;
                            }
                            if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase)
                                || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, StringComparison.OrdinalIgnoreCase))
                            {
                                Console.WriteLine(df.Nodes.Count);
                            }
                            else
                            {
                                Console.Error.WriteLine("countparts not supported for file of type '{0}'", df.Type);
                            }
                        }
                        break;

                    case "filesize":
                        {
                            if (args.Length == 0)
                            {
                                Console.Error.WriteLine("Error: filesize command needs argument: <dfspath>");
                                SetFailure();
                                return;
                            }
                            if (!dfs.DfsConfigExists(DFSXMLPATH))
                            {
                                Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                                SetFailure();
                                return;
                            }
                            string dfsfilename = args[0];
                            dfs dc = LoadDfsConfig();
                            dfs.DfsFile df = DfsFindAny(dc, dfsfilename);
                            if (null == df)
                            {
                                Console.Error.WriteLine("No such file: {0}", dfsfilename);
                                SetFailure();
                                return;
                            }
                            if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase)
                                || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, StringComparison.OrdinalIgnoreCase))
                            {
                                Console.WriteLine(df.Size); // Byte count.
                                Console.WriteLine(Surrogate.GetFriendlyByteSize(df.Size)); // Friendly size.
                            }
                            else
                            {
                                Console.Error.WriteLine("filesize not supported for file of type '{0}'", df.Type);
                                SetFailure();
                                return;
                            }
                        }
                        break;

                    case "ls":
                    case "dir":
                        if (!dfs.DfsConfigExists(DFSXMLPATH))
                        {
                            Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                            SetFailure();
                            return;
                        }
                        {
                            int iarg = 0;
                            bool showhidden = false;

                            if (args.Length > iarg)
                            {
                                if ("-h" == args[iarg])
                                {
                                    iarg++;
                                    showhidden = true;
                                }
                            }

                            bool filterspecified = args.Length > iarg;
                            string filter = filterspecified ? args[iarg++] : "*";
                            if (filter.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                            {
                                filter = filter.Substring(6);
                            }
                            string srex = Surrogate.WildcardRegexString(filter);
                            System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                            
                            if (!filterspecified) // Only show [metadata] info if no specific filter.
                            {

                                long dcsize = dfs.GetDfsConfigSize(DFSXMLPATH);
                                string colorcode = "", endcolorcode = "";
                                ConsoleColor oldcolor = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.Cyan;
                                if (isdspace)
                                {
                                    colorcode = "\u00012";
                                    endcolorcode = "\u00010";
                                }
                                Console.Write("  {0}{1,-40}{2}  ", colorcode, "[metadata]", endcolorcode);
                                Console.WriteLine("{0,10}", GetFriendlyByteSize(dcsize));
                                Console.ForegroundColor = oldcolor;
                            }

                            long totalbytes = 0;
                            dfs dc = LoadDfsConfig();
                            string about = "";
                            int matchedCount = 0;
                            for (int i = 0; i < dc.Files.Count; i++)
                            {
                                if (rex.IsMatch(dc.Files[i].Name))
                                {
                                    bool isnormalfile = 0 == string.Compare(dc.Files[i].Type, DfsFileTypes.NORMAL);
                                    bool iszballfile = 0 == string.Compare(dc.Files[i].Type, DfsFileTypes.DELTA);
                                    bool isjobsfile = 0 == string.Compare(dc.Files[i].Type, DfsFileTypes.JOB);
                                    bool isdllfile = 0 == string.Compare(dc.Files[i].Type, DfsFileTypes.DLL);
                                    bool istbl = false;
                                    int RecordLength = dc.Files[i].RecordLength;
                                    if (RecordLength > 0)
                                    {
                                        isnormalfile = true; // For most purposes here it's the same.
                                    }
                                    /*if (isnormalfile && dc.Files[i].Name.EndsWith(".tbl", StringComparison.OrdinalIgnoreCase))
                                    {
                                        istbl = true;
                                    }*/

                                    string ssize = " ";
                                    if (isnormalfile || isdllfile) // jobs file doesn't update the file size yet!
                                    {
                                        ssize = GetFriendlyByteSize(dc.Files[i].Size);
                                        if (dc.Files[i].Size < 0)
                                        {
                                            ssize = "?";
                                        }
                                    }
                                    if (dc.Files[i].Size >= 0)
                                    {
                                        totalbytes += dc.Files[i].Size;
                                    }
                                    else
                                    {
                                        about = "~";
                                    }
                                    ConsoleColor oldcolor = ConsoleColor.Gray; // ...
                                    string colorcode = "", endcolorcode = "";
                                    if (iszballfile)
                                    {
                                        oldcolor = Console.ForegroundColor;
                                        Console.ForegroundColor = ConsoleColor.Cyan;
                                        if (isdspace)
                                        {
                                            colorcode = "\u00012";
                                            endcolorcode = "\u00010";
                                        }
                                    }
                                    else if (isjobsfile)
                                    {
                                        if (isdspace)
                                        {
                                            colorcode = "\u00013";
                                            endcolorcode = "\u00010";
                                        }
                                    }
                                    else if (isdllfile || istbl)
                                    {
                                        oldcolor = Console.ForegroundColor;
                                        Console.ForegroundColor = ConsoleColor.Cyan;
                                        if (isdspace)
                                        {
                                            colorcode = "\u00012[\u00010";
                                            endcolorcode = "\u00012]\u00010";
                                        }
                                        Console.ForegroundColor = oldcolor;
                                    }
                                    if (RecordLength > 0)
                                    {
                                        endcolorcode += "\u00012@" + RecordLength.ToString() + "\u00010";
                                    }
                                    {
                                        int iddx = dc.Files[i].Name.IndexOf(".$");
                                        if (-1 != iddx)
                                        {
                                            iddx = dc.Files[i].Name.IndexOf(".$", iddx + 2);
                                            if (-1 != iddx)
                                            {
                                                if (showhidden)
                                                {
                                                    Console.ForegroundColor = ConsoleColor.Red;
                                                    colorcode = "\u00014";
                                                    endcolorcode = "\u00010";
                                                }
                                                else
                                                {
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                    if (isdllfile || istbl || RecordLength > 0)
                                    {
                                        Console.Write("  {0}{1}{2}  ", colorcode, dc.Files[i].Name, endcolorcode);
                                        int spacelen = 40 - 2 - dc.Files[i].Name.Length;
                                        if (spacelen > 0)
                                        {
                                            Console.Write(new string(' ', 40 - 2 - dc.Files[i].Name.Length));
                                        }
                                    }
                                    else
                                    {
                                        Console.Write("  {0}{1,-40}{2}  ", colorcode, dc.Files[i].Name, endcolorcode);
                                    }
                                    if (iszballfile || isjobsfile)
                                    {
                                        Console.ForegroundColor = oldcolor;
                                    }
                                    Console.Write("{0,10}", ssize);
                                    if (isnormalfile)
                                    {
                                        Console.Write("  ({0} parts)", dc.Files[i].Nodes.Count);
                                    }
                                    Console.WriteLine();
                                    matchedCount++;
                                }
                            }
                            Console.WriteLine("        {0} Distributed Files", matchedCount);
                            Console.WriteLine("        {0}{1} Used (data files)", about, GetFriendlyByteSize(totalbytes));
                            {
                                long freespace = 0;
                                long freemin = long.MaxValue;
                                int replicationFactor = dc.Replication;
                                string[] fslaves = dc.Slaves.SlaveList.Split(';');
                                //for (int fsi = 0; fsi < fslaves.Length; fsi++)
                                MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                    new Action<string>(
                                    delegate(string fslave)
                                    {
                                        //string fslave = fslaves[fsi];
                                        try
                                        {
                                            long x = (long)GetDiskFreeBytes(Surrogate.NetworkPathForHost(fslave));
                                            if (replicationFactor > 1)
                                            {
                                                x = x / replicationFactor;
                                            }
                                            lock (fslaves)
                                            {
                                                if (x < freemin)
                                                {
                                                    freemin = x;
                                                }
                                                freespace += x;
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            LogOutputToFile("Error while calculating DFS disk spage usage: " + e.ToString());
                                            /*if (!dc.IsFailoverEnabled)
                                            {
                                                throw;
                                            }*/
                                        }
                                    }
                                ), fslaves, fslaves.Length);
                                Console.WriteLine("        {0} Free ({1} node avg; {2} node min)",
                                    GetFriendlyByteSize(freespace),
                                    GetFriendlyByteSize(freespace / fslaves.Length),
                                    GetFriendlyByteSize((freemin == long.MaxValue) ? 0 : freemin)
                                    );
                            }
                        }
                        break;

                    case "copy":
                    case "cp":
                        if (args.Length < 2)
                        {
                            Console.Error.WriteLine("Error: dfs copy command needs arguments: <from-path> <to-path>");
                            SetFailure();
                            return;
                        }
                        {
                            bool isdfs0 = args[0].StartsWith("dfs://", StringComparison.OrdinalIgnoreCase);
                            bool isdfs1 = args[1].StartsWith("dfs://", StringComparison.OrdinalIgnoreCase);
                            if (isdfs0 && isdfs1)
                            {
                                Console.Error.WriteLine("Error: dfs copy DFS-to-DFS not supported yet");
                                SetFailure();
                                return;
                            }
                            if (!isdfs0 && !isdfs1)
                            {
                                //Console.Error.WriteLine("Error: dfs copy local-to-local not supported");
                                Console.Error.WriteLine("Error: dfs copy must contain at least one dfs://");
                                SetFailure();
                                return;
                            }
                            if (isdfs0)
                            {
                                DfsGet(args);
                            }
                            else //if (isdfs1)
                            {
                                DfsPut(args);
                            }
                        }
                        break;

                    case "get":
                        DfsGet(args);
                        break;

                    case "getbinary":
                        DfsGetBinary(args);
                        break;

                    case "put":
                        DfsPut(args);
                        break;

                    case "fput":
                        DfsFPut(args);
                        break;

                    case "fget":
                        DfsFGet(args);
                        break;

                    case "copyto":
                        DfsCopyTo(args);
                        break;

                    case "putbinary":
                        DfsPutBinary(args);
                        break;

                    case "bulkget":
                        DfsBulkGet(args);
                        break;

                    case "bulkput":
                        DfsBulkPut(args);
                        break;

                    case "shuffle":
                        DfsShuffle(args);
                        break;

                    case "getjobs":
                        if (args.Length < 1)
                        {
                            Console.Error.WriteLine("Argument expected: <localpath.dj>");
                            SetFailure();
                            return;
                        }
                        if (new System.IO.DirectoryInfo(args[0]).Exists)
                        {
                            Console.Error.WriteLine("Argument cannot be a directory. Argument expected: <localpath.dj>");
                            SetFailure();
                            return;
                        }
                        EnsureNetworkPath(args[0]);
                        //using (LockDfsMutex())
                        {
                            dfs dc = LoadDfsConfig();
                            int count = 0;
                            using (System.IO.StreamWriter sw = new System.IO.StreamWriter(args[0]))
                            {
                                for (int i = 0; i < dc.Files.Count; i++)
                                {
                                    dfs.DfsFile f = dc.Files[i];
                                    if (0 == string.Compare(f.Type, DfsFileTypes.JOB, StringComparison.OrdinalIgnoreCase))
                                    {
                                        try
                                        {
                                            if (f.Nodes.Count < 1)
                                            {
                                                throw new Exception("Error: -exec jobs file not in correct jobs DFS format");
                                            }

                                            dfs.DfsFile.FileNode fn = dc.Files[i].Nodes[0];

                                            string content = System.IO.File.ReadAllText(Surrogate.NetworkPathForHost(fn.Host.Split(';')[0]) + @"\" + fn.Name);

                                            sw.Write(f.Name);
                                            sw.Write('\0');
                                            sw.Write(f.Type);
                                            sw.Write('\0');
                                            sw.Write(content);
                                            sw.Write('\0');

                                            count++;
                                        }
                                        catch (Exception e)
                                        {
                                            Console.Error.WriteLine("Unable to get job '{0}': {1}", f.Name, e.Message);
                                        }
                                    }
                                }
                            }
                            Console.WriteLine("Saved {0} jobs files to jobs archive '{1}'", count, args[0]);
                        }
                        break;

                    case "putjobs":
                        if (args.Length < 1)
                        {
                            Console.Error.WriteLine("Argument expected: <localpath.dj>");
                            SetFailure();
                            return;
                        }
                        EnsureNetworkPath(args[0]);
                        {
                            string[] segs = System.IO.File.ReadAllText(args[0]).Split('\0'); // 0: name, 1: type, 2: content, etc.
                            int count = 0;
                            for (int si = 0; si + 2 < segs.Length; si += 3)
                            {
                                try
                                {
                                    string fname = segs[si + 0];
                                    string ftype = segs[si + 1];
                                    string fcontent = segs[si + 2];
                                    if (0 != string.Compare(ftype, DfsFileTypes.JOB, StringComparison.OrdinalIgnoreCase))
                                    {
                                        throw new Exception("File '" + fname + "' is of type '" + ftype + "', not of expected type '" + DfsFileTypes.JOB + "'");
                                    }
                                    else
                                    {
                                        if (!DfsPutJobsFileContent(fname, fcontent))
                                        {
                                            throw new Exception("Unable to write job '" + fname + "' to DFS; ensure that the file does not already exist in DFS");
                                        }
                                    }
                                    Console.WriteLine("  {0}", fname);
                                    count++;
                                }
                                catch (Exception e)
                                {
                                    Console.Error.WriteLine("Problem importing job: {0}", e.Message);
                                }
                            }
                            Console.WriteLine("Done importing {0} jobs files into DFS", count);
                        }
                        break;

                    case "combine":
                        // Note: datanode chunk file header keeps the old file offset.
                        {
                            //System.Threading.Thread.Sleep(8000);
                            using (LockDfsMutex())
                            {
                                dfs dc = LoadDfsConfig();
                                List<string> inputs = new List<string>();
                                string outfn = null;
                                bool nextoutfn = false;
                                foreach (string arg in args)
                                {
                                    if (nextoutfn)
                                    {
                                        if (null != outfn)
                                        {
                                            throw new Exception("Too many output files");
                                        }
                                        outfn = arg;
                                    }
                                    else
                                    {
                                        if ("+" == arg)
                                        {
                                            nextoutfn = true;
                                        }
                                        else if (arg.Length > 0 && '+' == arg[0])
                                        {
                                            if (null != outfn)
                                            {
                                                throw new Exception("Too many output files");
                                            }
                                            outfn = arg.Substring(1);
                                        }
                                        else
                                        {
                                            inputs.AddRange(SplitInputPaths(dc, arg));
                                        }
                                    }
                                }
                                if (0 == inputs.Count)
                                {
                                    Console.Error.WriteLine("No input files to combine");
                                    SetFailure();
                                    return;
                                }
                                bool outisin = false;
                                if (null == outfn)
                                {
                                    outfn = inputs[inputs.Count - 1];
                                    outisin = true;
                                }
                                if (outfn.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                                {
                                    outfn = outfn.Substring(6);
                                }
                                string reason = "";
                                if (dfs.IsBadFilename(outfn, out reason))
                                {
                                    Console.Error.WriteLine("Invalid output file: {0}", reason);
                                    SetFailure();
                                    return;
                                }
                                if (null != DfsFindAny(dc, outfn))
                                {
                                    if (outisin)
                                    {
                                        if (!QuietMode && InteractiveMode)
                                        {
                                            Console.Write("The specified file already exists in DFS; overwrite? ");
                                            ConsoleFlush();
                                            for (; ; )
                                            {
                                                string s = Console.ReadLine();
                                                char ch = '\0';
                                                if (0 != s.Length)
                                                {
                                                    ch = char.ToUpper(s[0]);
                                                }
                                                if ('N' == ch)
                                                {
                                                    Console.WriteLine("    Aborted by user");
                                                    return;
                                                }
                                                else if ('Y' == ch)
                                                {
                                                    break; // !
                                                }
                                                else
                                                {
                                                    Console.Write("Overwrite, yes or no? ");
                                                    ConsoleFlush();
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        Console.Error.WriteLine("Output file for combine already exists: {0}", outfn);
                                        SetFailure();
                                        return;
                                    }
                                }
                                //if (verbose)
                                {
                                    //Console.WriteLine("Combining {0} input files into file '{1}'", inputs.Count, outfn);
                                }
                                {
                                    dfs.DfsFile dfout = new dfs.DfsFile();
                                    dfout.Nodes = new List<dfs.DfsFile.FileNode>(inputs.Count * 32);
                                    dfout.Name = outfn;
                                    dfout.Size = 0;
                                    int RecordLength = int.MinValue;
                                    for (int i = 0; i < inputs.Count; i++)
                                    {
                                        dfs.DfsFile df = DfsFindAny(dc, inputs[i]);
                                        if (null == df)
                                        {
                                            Console.Error.WriteLine("Combine error: input file '{0}' does not exist in DFS or was included more than once", inputs[i]);
                                            SetFailure();
                                            return;
                                        }
                                        if (0 != string.Compare(df.Type, DfsFileTypes.NORMAL, StringComparison.OrdinalIgnoreCase)
                                            && 0 != string.Compare(df.Type, DfsFileTypes.BINARY_RECT, StringComparison.OrdinalIgnoreCase))
                                        {
                                            Console.Error.WriteLine("DFS file '{0}' is not of expected type", df.Name);
                                            SetFailure();
                                            return;
                                        }
                                        {
                                            int reclen = df.RecordLength;
                                            if (int.MinValue != RecordLength
                                                && reclen != RecordLength)
                                            {
                                                Console.Error.WriteLine("Error: Record lengths of all input files must match; DFS file '{0}' has record length of {1}, expected record length of {2}", df.Name, (-1 == reclen) ? "<none>" : reclen.ToString(), (-1 == RecordLength) ? "<none>" : RecordLength.ToString());
                                                SetFailure();
                                                return;
                                            }
                                            RecordLength = reclen;
#if DEBUG
                                            if (int.MinValue == RecordLength)
                                            {
                                                throw new Exception("DEBUG: (int.MinValue == RecordLength) after first file");
                                            }
#endif
                                        }
                                        int j = dfout.Nodes.Count;
                                        dfout.Nodes.AddRange(df.Nodes);
                                        for (; j < dfout.Nodes.Count; j++)
                                        {
                                            dfout.Nodes[j].Position = dfout.Size; // !
                                            dfout.Size += dfout.Nodes[j].Length; // !
                                        }
                                        dc.Files.Remove(df); // Ok since a failure will bail this out entirely, since the next DFS read re-loads.
                                    }
                                    if (RecordLength > 0)
                                    {
                                        dfout.XFileType = DfsFileTypes.BINARY_RECT + "@" + RecordLength.ToString();
                                    }
                                    dc.Files.Add(dfout);
                                    UpdateDfsXml(dc); // !
                                    //if (verbose)
                                    {
                                        Console.WriteLine("Combined {0} input files into file '{1}' of resulting size {2}", inputs.Count, outfn, GetFriendlyByteSize(dfout.Size));
                                    }
                                }
                            }
                        }
                        break;

                    case "info":
                    case "information":
                        {
                            dfs dc = LoadDfsConfig();
                            if (null == dc)
                            {
                                Console.Error.WriteLine("    No " + dfs.DFSXMLNAME);
                                SetFailure();
                            }
                            else
                            {
                                string[] slaves = dc.Slaves.SlaveList.Split(',', ';');
                                
                                bool mt = false;
                                bool shortname = true;
                                List<string> largs = new List<string>();
                                for (int i = 0; i < args.Length; i++)
                                {
                                    string arg = args[i].ToLower();
                                    switch(arg)
                                    {
                                        case "-mt":
                                            mt = true;
                                            break;
                                        case "-s":
                                            shortname = true;
                                            break;
                                        default:
                                            largs.Add(arg);
                                            break;
                                    }
                                }

                                if (largs.Count == 0)
                                {
                                    if (mt)
                                    {
                                        Dictionary<string, string> netpaths = new Dictionary<string, string>(slaves.Length);
                                        Dictionary<string, string> hostnames = new Dictionary<string, string>(slaves.Length);
                                        Dictionary<string, long> freesp = new Dictionary<string, long>(slaves.Length);

                                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                        new Action<string>(delegate(string host)
                                        {
                                            host = host.ToUpper();
                                            lock (hostnames)
                                            {
                                                if (shortname)
                                                {
                                                    hostnames[host] = host;
                                                }
                                                else
                                                {
                                                    hostnames[host] = IPAddressUtil.GetName(host);
                                                }

                                                string np = Surrogate.NetworkPathForHost(host);
                                                netpaths[host] = np;
                                                freesp[host] = (long)GetDiskFreeBytes(np);
                                            }                                            
                                        }), slaves, slaves.Length);

                                        {
                                            string dfsfmt = "machines=";
                                            for (int i = 0; i < slaves.Length; i++)
                                            {
                                                if (i != 0)
                                                {
                                                    dfsfmt += ",";
                                                }                                                
                                                dfsfmt += hostnames[slaves[i].ToUpper()];
                                            }
                                            dfsfmt += " processes=" + dc.Blocks.TotalCount.ToString();

                                            if (dc.DataNodeBaseSize != dfs.DataNodeBaseSize_default)
                                            {
                                                dfsfmt += " datanodebasesize=" + dc.DataNodeBaseSize.ToString();
                                            }
                                            Console.WriteLine("[DFS information]");
                                            Console.WriteLine("    Format: {0}", dfsfmt);
                                            Console.WriteLine("    Files: {0}", dc.Files.Count);
                                        }

                                        MySpace.DataMining.Threading.ThreadTools<string>.Parallel(
                                        new Action<string>(delegate(string host)
                                        {
                                            host = host.ToUpper();                
                                            try
                                            {
                                                string netpath = netpaths[host];
                                                System.IO.DirectoryInfo netdi = new System.IO.DirectoryInfo(netpath);
                                                long zdcount = 0;
                                                long zdsizes = 0;
                                                foreach (System.IO.FileInfo fi in (netdi).GetFiles("zd*.zd"))
                                                {
                                                    zdcount++;
                                                    zdsizes += fi.Length;
                                                }
                                                long sbcount = 0;
                                                long sbsizes = 0;
                                                foreach (System.IO.FileInfo fi in (netdi).GetFiles(GetSnowballFilesWildcard("*")))
                                                {
                                                    sbcount++;
                                                    sbsizes += fi.Length;
                                                }
                                                long diskfree = freesp[host];
                                                lock (hostnames)
                                                {
                                                    Console.WriteLine("        {0}:", hostnames[host]);
                                                    Console.WriteLine("            {0} data file parts", zdcount);
                                                    Console.WriteLine("            {0} cache parts", sbcount);
                                                    Console.WriteLine("            {0} total used", GetFriendlyByteSize(zdsizes + sbsizes));
                                                    Console.WriteLine("            {0} free", GetFriendlyByteSize(diskfree));
                                                }
                                            }
                                            catch
                                            {
                                                string reason;
                                                if (Surrogate.IsHealthySlaveMachine(host, out reason))
                                                {
                                                    reason = "cannot query";
                                                }
                                                lock (hostnames)
                                                {
                                                    Console.WriteLine("            Error: {0}", reason);
                                                }                                                
                                            }
                                        }), slaves, slaves.Length);                           
                                    }
                                    else
                                    {
                                        string[] netpaths = new string[slaves.Length];
                                        Dictionary<int, long> freesp = new Dictionary<int, long>();

                                        Console.WriteLine("[DFS information]");

                                        {
                                            string dfsfmt = "machines=";
                                            for (int i = 0; i < slaves.Length; i++)
                                            {
                                                if (i != 0)
                                                {
                                                    dfsfmt += ",";
                                                }                                                
                                                dfsfmt += shortname ? slaves[i].ToUpper() : IPAddressUtil.GetName(slaves[i]);

                                                string np = Surrogate.NetworkPathForHost(slaves[i]);
                                                netpaths[i] = np;
                                                freesp.Add(i, (long)GetDiskFreeBytes(np));
                                            }
                                            dfsfmt += " processes=" + dc.Blocks.TotalCount.ToString();

                                            if (dc.DataNodeBaseSize != dfs.DataNodeBaseSize_default)
                                            {
                                                dfsfmt += " datanodebasesize=" + dc.DataNodeBaseSize.ToString();
                                            }
                                            Console.WriteLine("    Format: {0}", dfsfmt);
                                        }

                                        Console.WriteLine("    Files: {0}", dc.Files.Count);

                                        List<KeyValuePair<int, long>> sfreesp = new List<KeyValuePair<int, long>>(freesp);
                                        sfreesp.Sort(
                                           delegate(KeyValuePair<int, long> firstPair, KeyValuePair<int, long> nextPair)
                                           {
                                               return -firstPair.Value.CompareTo(nextPair.Value);
                                           }
                                        );

                                        foreach (KeyValuePair<int, long> item in sfreesp)
                                        {
                                            int si = item.Key;
                                            Console.WriteLine("        {0}:", shortname ? slaves[si].ToUpper() : IPAddressUtil.GetName(slaves[si]));
                                            try
                                            {
                                                string netpath = netpaths[si];
                                                System.IO.DirectoryInfo netdi = new System.IO.DirectoryInfo(netpath);
                                                long zdcount = 0;
                                                long zdsizes = 0;
                                                foreach (System.IO.FileInfo fi in (netdi).GetFiles("zd*.zd"))
                                                {
                                                    zdcount++;
                                                    zdsizes += fi.Length;
                                                }
                                                long sbcount = 0;
                                                long sbsizes = 0;
                                                foreach (System.IO.FileInfo fi in (netdi).GetFiles(GetSnowballFilesWildcard("*")))
                                                {
                                                    sbcount++;
                                                    sbsizes += fi.Length;
                                                }
                                                long diskfree = item.Value;
                                                Console.WriteLine("            {0} data file parts", zdcount);
                                                Console.WriteLine("            {0} cache parts", sbcount);
                                                Console.WriteLine("            {0} total used", GetFriendlyByteSize(zdsizes + sbsizes));
                                                Console.WriteLine("            {0} free", GetFriendlyByteSize(diskfree));
                                            }
                                            catch
                                            {
                                                string reason;
                                                if (Surrogate.IsHealthySlaveMachine(slaves[si], out reason))
                                                {
                                                    reason = "cannot query";
                                                }
                                                Console.WriteLine("            Error: {0}", reason);
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    if (-1 != largs[0].IndexOf(':'))
                                    {
                                        string[] specs = largs[0].Split(':'); // <file>:<host>
                                        dfs.DfsFile df = DfsFindAny(dc, specs[0]);
                                        if (null == df)
                                        {
                                            Console.Error.WriteLine("    No such file: {0}", specs[0]);
                                        }
                                        else if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, true)
                                            || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, true))
                                        {
                                            bool HasSamples = df.RecordLength < 1;
                                            Console.WriteLine("[DFS file information]");
                                            Console.WriteLine("   DFS File: {0}", df.Name);
                                            string shost = "";
                                            {
                                                Console.WriteLine("   Host: {0}", specs[1].ToUpper());
                                                shost = IPAddressUtil.GetName(specs[1]);
                                            }
                                            /*string partspec = "";
                                            if (specs.Length >= 3)
                                            {
                                                partspec = specs[2];
                                            }*/
                                            {
                                                foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                                                {
                                                    int replindex = 0;
                                                    if (shost.Length > 0)
                                                    {
                                                        {
                                                            string[] fnxshosts = fn.Host.Split(';');
                                                            for (int i = 0; i < fnxshosts.Length; i++)
                                                            {
                                                                string fnshost = IPAddressUtil.GetName(fnxshosts[i]);
                                                                if (0 == string.Compare(shost, fnshost, StringComparison.OrdinalIgnoreCase))
                                                                {
                                                                    replindex = i + 1;
                                                                    break;
                                                                }
                                                            }
                                                            if (replindex < 1)
                                                            {
                                                                continue;
                                                            }
                                                        }
                                                    }
                                                    /*if (partspec.Length > 0)
                                                    {
                                                        if (0 != string.Compare(partspec, fn.Name, StringComparison.OrdinalIgnoreCase))
                                                        {
                                                            continue;
                                                        }
                                                    }*/
                                                    try
                                                    {
                                                        if (HasSamples)
                                                        {
                                                            Console.WriteLine("     {0} [{3}] ({1} data; {2} samples)",
                                                                fn.Name,
                                                                GetFriendlyByteSize((new System.IO.FileInfo(dfs.MapNodeToNetworkPath(fn))).Length),
                                                                GetFriendlyByteSize((new System.IO.FileInfo(dfs.MapNodeToNetworkPath(fn, true))).Length),
                                                                replindex
                                                                );
                                                        }
                                                        else
                                                        {
                                                            Console.WriteLine("     {0} [{2}] ({1} data)",
                                                                fn.Name,
                                                                GetFriendlyByteSize((new System.IO.FileInfo(dfs.MapNodeToNetworkPath(fn))).Length),
                                                                replindex
                                                                );
                                                        }
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        LogOutputToFile(e.ToString());
                                                        Console.WriteLine("     {0}", fn.Name);
                                                    }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            Console.Error.WriteLine("DFS file '{0}' is not of expected type", df.Type);
                                            SetFailure();
                                            return;
                                        }
                                    }
                                    else
                                    {
                                        dfs.DfsFile df = DfsFindAny(dc, largs[0]);
                                        if (null == df)
                                        {
                                            Console.Error.WriteLine("    No such file: {0}", largs[0]);
                                        }
                                        else
                                        {
                                            Console.WriteLine("[DFS file information]");
                                            if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, true)
                                                || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, true))
                                            {
                                                bool HasSamples = df.RecordLength < 1;
                                                Console.WriteLine("   DFS File: {0}", df.Name);
                                                int RecordLength = df.RecordLength;
                                                if (RecordLength > 0)
                                                {
                                                    Console.WriteLine("      Record Length: {0}", RecordLength);
                                                }
                                                Console.WriteLine("      Size: {0} ({1})", GetFriendlyByteSize(df.Size), df.Size);
                                                if (HasSamples)
                                                {
                                                    long samplesize = 0;
                                                    MySpace.DataMining.Threading.ThreadTools<dfs.DfsFile.FileNode>.Parallel(
                                                        new Action<dfs.DfsFile.FileNode>(delegate(dfs.DfsFile.FileNode fn)
                                                        {
                                                            try
                                                            {
                                                                System.IO.FileInfo fi = new System.IO.FileInfo(dfs.MapNodeToNetworkPath(fn, true));
                                                                int ss = (int)fi.Length;
                                                                System.Threading.Interlocked.Add(ref samplesize, ss);
                                                            }
                                                            catch
                                                            {
                                                            }
                                                        }), df.Nodes, slaves.Length);

                                                    string avg = "0";
                                                    if (df.Nodes.Count > 0)
                                                    {
                                                        avg = GetFriendlyByteSize(samplesize / df.Nodes.Count);
                                                    }

                                                    Console.WriteLine("      Sample Size: {0} ({1} avg)", GetFriendlyByteSize(samplesize), avg);
                                                }
                                                Console.WriteLine("      Parts: {0}", df.Nodes.Count);
                                                {
                                                    Dictionary<string, int> partsonhosts = new Dictionary<string, int>();
                                                    Dictionary<string, long> zdsizeonhosts = new Dictionary<string, long>();
                                                    Dictionary<string, long> zsasizeonhosts = new Dictionary<string, long>();
                                                    for (int i = 0; i < df.Nodes.Count; i++)
                                                    {
                                                        int value;
                                                        long zdsize;
                                                        long zsasize;
                                                        string[] xkeys = df.Nodes[i].Host.Split(';');
                                                        for (int ik = 0; ik < xkeys.Length; ik++)
                                                        {
                                                            string key = xkeys[ik].ToUpper();
                                                            if (partsonhosts.ContainsKey(key))
                                                            {
                                                                value = partsonhosts[key];
                                                                zdsize = zdsizeonhosts[key];
                                                                zsasize = zsasizeonhosts[key];
                                                            }
                                                            else
                                                            {
                                                                value = 0;
                                                                zdsize = 0;
                                                                zsasize = 0;
                                                            }
                                                            value++;
                                                            try
                                                            {
                                                                zdsize += (new System.IO.FileInfo(dfs.MapNodeToNetworkPath(df.Nodes[i]))).Length;
                                                            }
                                                            catch
                                                            {
                                                            }
                                                            if (HasSamples)
                                                            {
                                                                try
                                                                {
                                                                    zsasize += (new System.IO.FileInfo(dfs.MapNodeToNetworkPath(df.Nodes[i], true))).Length;
                                                                }
                                                                catch
                                                                {
                                                                }
                                                            }
                                                            partsonhosts[key] = value;
                                                            zdsizeonhosts[key] = zdsize;
                                                            zsasizeonhosts[key] = zsasize;
                                                        }
                                                    }
                                                    foreach (KeyValuePair<string, int> kvp in partsonhosts)
                                                    {
                                                        if (HasSamples)
                                                        {
                                                            Console.WriteLine("        {0} chunks on {1} ({2} data; {3} samples)",
                                                                kvp.Value, kvp.Key,
                                                                GetFriendlyByteSize(zdsizeonhosts[kvp.Key]), GetFriendlyByteSize(zsasizeonhosts[kvp.Key])
                                                                );
                                                        }
                                                        else
                                                        {
                                                            Console.WriteLine("        {0} chunks on {1} ({2} data)",
                                                                kvp.Value, kvp.Key,
                                                                GetFriendlyByteSize(zdsizeonhosts[kvp.Key])
                                                                );
                                                        }
                                                    }
                                                }
                                            }
                                            else if (0 == string.Compare(df.Type, "zsb", true))
                                            {
                                                Console.WriteLine("    DFS Delta: {0}", df.Name);
                                                long sbsz = 0;
                                                int sbparts = 0;
                                                {
                                                    string fnwc = GetSnowballFilesWildcard(df.Name);
                                                    //string[] slaves = dc.Slaves.SlaveList.Split(',', ';');
                                                    try
                                                    {
                                                        for (int si = 0; si < slaves.Length; si++)
                                                        {
                                                            string netpath = Surrogate.NetworkPathForHost(slaves[si]);
                                                            foreach (System.IO.FileInfo fi in (new System.IO.DirectoryInfo(netpath)).GetFiles(fnwc))
                                                            {
                                                                sbparts++;
                                                                sbsz += fi.Length;
                                                            }
                                                        }
                                                    }
                                                    catch
                                                    {
                                                        sbparts = -1;
                                                        sbsz = -1;
                                                    }
                                                }
                                                Console.WriteLine("        Size: {0} ({1})", (sbsz >= 0) ? GetFriendlyByteSize(sbsz) : "?", (sbsz >= 0) ? sbsz.ToString() : "?");
                                                Console.WriteLine("        Parts: {0}", (sbparts >= 0) ? sbparts.ToString() : "?");
                                                Console.WriteLine("        Cached Inputs: {0}", df.Nodes.Count);

                                                ConsoleColor oldcolor = ConsoleColor.Gray;
                                                string colorcode = "";
                                                string nodeName = "";

                                                foreach (dfs.DfsFile.FileNode fn in df.Nodes)
                                                {
                                                    Console.ForegroundColor = oldcolor;
                                                    Console.Write("            Input:");

                                                    if (fn.Name.StartsWith(invalidatedCacheToken))
                                                    {
                                                        nodeName = fn.Name.Substring(invalidatedCacheToken.Length + 1);
                                                        Console.ForegroundColor = ConsoleColor.DarkGray;

                                                        if (isdspace)
                                                        {
                                                            colorcode = "\u00015";
                                                        }
                                                    }
                                                    else
                                                    {
                                                        nodeName = fn.Name;
                                                        Console.ForegroundColor = oldcolor;
                                                        colorcode = "";
                                                    }

                                                    Console.WriteLine("{0}{1}{2}", colorcode, nodeName, colorcode.Length != 0 ? "\u00010" : "");
                                                }

                                                Console.ForegroundColor = oldcolor;
                                            }
                                            else
                                            {
                                                Console.Error.WriteLine("    No info for file of type '{0}'", df.Type);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        break;

                    case "partinfo":
                        {
                            if (args.Length < 1)
                            {
                                Console.Error.WriteLine("qizmt partinfo <partname>");
                                SetFailure();
                            }
                            else
                            {
                                string nodename = args[0];
                                dfs dc = LoadDfsConfig();
                                string ownerfilename = null;
                                dfs.DfsFile.FileNode fn = DfsFindFileNode(dc, nodename, out ownerfilename);
                                if (fn == null)
                                {
                                    Console.WriteLine("Part not found in dfs.xml");
                                }
                                else
                                {
                                    Console.WriteLine();
                                    Console.WriteLine("Owner file name: {0}", ownerfilename);

                                    Console.WriteLine();
                                    Console.WriteLine("Part paths in metadata:");
                                    Console.WriteLine();
                                    string[] nhosts = fn.Host.Split(';');  
                                    for (int hi = 0; hi < nhosts.Length; hi++)
                                    {
                                        Console.WriteLine(NetworkPathForHost(nhosts[hi]) + @"\" + fn.Name);                                        
                                        Console.WriteLine();
                                    } 

                                    Console.WriteLine();
                                    Console.WriteLine("Part paths in physical files:");
                                    Console.WriteLine();        
                                    ConsoleColor oldcolor = Console.ForegroundColor;
                                    string colorcode = "\u00014";
                                    string endcolorcode = "\u00010"; 
                                    for (int hi = 0; hi < nhosts.Length; hi++)
                                    {
                                        string ppath = NetworkPathForHost(nhosts[hi]) + @"\" + fn.Name;
                                        if (!System.IO.File.Exists(ppath))
                                        {
                                            Console.ForegroundColor = ConsoleColor.Red;
                                            Console.WriteLine("{0}{1} does not exist{2}", colorcode, ppath, endcolorcode);
                                            Console.ForegroundColor = oldcolor;
                                        }
                                        else
                                        {
                                            Console.WriteLine(ppath);
                                        }                                        
                                        Console.WriteLine();
                                    }                                    
                                }
                            }
                        }
                        break;

                    case "delchunk":
                        {
                            if (args.Length < 2)
                            {
                                Console.Error.WriteLine("qizmt delchunk <chunkname> <host>");
                                SetFailure();
                                return;
                            }
                            else
                            {
                                string nodename = args[0];
                                string delhost = args[1];

                                dfs.DfsFile.FileNode fn = null;
                                bool metaremoved = false;
                                using (LockDfsMutex())
                                {
                                    dfs dc = LoadDfsConfig();
                                    string ownerfilename = null;
                                    fn = DfsFindFileNode(dc, nodename, out ownerfilename);
                                    if (fn == null)
                                    {
                                        Console.WriteLine("Part not found in dfs.xml");
                                        return;
                                    }
                                    else
                                    {
                                        string[] nhosts = fn.Host.Split(';');
                                        string goodhosts = "";                                        
                                        for (int hi = 0; hi < nhosts.Length; hi++)
                                        {
                                            if (string.Compare(nhosts[hi], delhost, true) != 0)
                                            {
                                                if (goodhosts.Length > 0)
                                                {
                                                    goodhosts += ';';
                                                }
                                                goodhosts += nhosts[hi];
                                            }
                                            else
                                            {
                                                metaremoved = true;
                                            }
                                        }
                                        if (goodhosts.Length > 0)
                                        {
                                            fn.Host = goodhosts;
                                        }
                                        else
                                        {
                                            //remove this node all together
                                            dfs.DfsFile df = DfsFindAny(dc, ownerfilename);
                                            if (df == null)
                                            {
                                                Console.Error.WriteLine("Cannot locate owner file.");
                                                return;
                                            }
                                            else
                                            {
                                                long filesize = 0;
                                                List<dfs.DfsFile.FileNode> goodnodes = new List<dfs.DfsFile.FileNode>(df.Nodes.Count - 1);
                                                for (int ni = 0; ni < df.Nodes.Count; ni++)
                                                {
                                                    dfs.DfsFile.FileNode thisnode = df.Nodes[ni];
                                                    if (string.Compare(thisnode.Name, nodename, true) != 0)
                                                    {
                                                        goodnodes.Add(thisnode);
                                                        thisnode.Position = filesize;
                                                        filesize += thisnode.Length;
                                                    }
                                                }
                                                df.Size = filesize;
                                                df.Nodes = goodnodes;
                                            }
                                        }
                                        
                                        UpdateDfsXml(dc);
                                    }                                    
                                }

                                bool physicalfileremoved = false;
                                try
                                {
                                    string ppath = NetworkPathForHost(delhost) + @"\" + fn.Name;
                                    if (System.IO.File.Exists(ppath) || System.IO.File.Exists(ppath + ".zsa"))
                                    {
                                        System.IO.File.Delete(ppath);
                                        System.IO.File.Delete(ppath + ".zsa");
                                        physicalfileremoved = true;
                                    }                                    
                                }
                                catch
                                {
                                }

                                Console.WriteLine("Chunk deleted successfully from host:");
                                if (metaremoved)
                                {
                                    Console.WriteLine("Metadata removed");
                                }
                                if (physicalfileremoved)
                                {
                                    Console.WriteLine("Physical file deleted");
                                }
                            }
                        }
                        break;

                    case "\u0040format":
                    case "format":
                        {
                            EnterAdminCmd();
                            bool verify = false;
                            if (args.Length == 1 &&
                                (0 == string.Compare(args[0], "vacuum", true) || 0 == string.Compare(args[0], "vacuum=true", true)))
                            {
                                Console.Error.WriteLine("Use: {0} killall", appname);
                            }
                            else // Normal format...
                            {
                                int blockcount = -1;
                                int sortedblockcount = -1;
                                string[] slavelist = null;
                                int datanodebasesize = 0;
                                int zmapblockcount = 0;
                                int zblockcount = 0;
                                //int zblockaddbuffersize = 0;
                                //int zblockreadbuffersize = 0;
                                int filebuffersizeoverride = 0;
                                byte compresszmapblocks = 127;
                                byte compressdfschunks = 127;
                                int numargs = 0;
                                int replication = 0;
                                ulong btreeCapSize = 0;
                                int logexechistory = 0;
                                int cooktimeout = -1;
                                int cookretries = -1;
                                bool mt = false;
                                bool metaonly = false;
                                string metabackuplocation = null;
                                int failovertimeout = -1;
                                int failoverdocheck = -1;

                                foreach (string arg in args)
                                {
                                    string optname = "", optvalue = "";
                                    {
                                        int oi = arg.IndexOf('=');
                                        if (-1 == oi)
                                        {
                                            optname = arg;
                                            optvalue = "";
                                        }
                                        else
                                        {
                                            optname = arg.Substring(0, oi);
                                            optvalue = arg.Substring(oi + 1);
                                        }
                                    }
                                    numargs++;
                                    switch (optname.ToLower())
                                    {
                                        case "blocks":
                                        case "processes":
                                        case "groupedprocesses":
                                            blockcount = int.Parse(optvalue);
                                            break;

                                        case "sortedprocesses":
                                            sortedblockcount = int.Parse(optvalue);
                                            break;

                                        case "slaves":
                                        case "machines":
                                            if (optvalue[0] == '@')
                                            {
                                                slavelist = Surrogate.GetHostsFromFile(optvalue.Substring(1));
                                            }
                                            else
                                            {
                                                slavelist = optvalue.Split(';', ',');
                                            }                                            
                                            break;

                                        case "replication":
                                        case "replicationfactor":
                                            replication = int.Parse(optvalue);
                                            break;

                                        case "datanodebasesize":
                                            datanodebasesize = ParseCapacity(optvalue);
                                            break;

                                        case "zmapblockcount":
                                            zmapblockcount = int.Parse(optvalue);
                                            break;

                                        case "zblockcount":
                                            zblockcount = int.Parse(optvalue);
                                            break;

                                        case "zblockaddbuffersize":
                                            //zblockaddbuffersize = ParseCapacity(optvalue);
                                            Console.Error.WriteLine("zblockaddbuffersize no longer supported, use FileBufferSizeOverride");
                                            SetFailure();
                                            return;

                                        case "zblockreadbuffersize":
                                            //zblockreadbuffersize = ParseCapacity(optvalue);
                                            Console.Error.WriteLine("zblockreadbuffersize no longer supported, use FileBufferSizeOverride");
                                            SetFailure();
                                            return;

                                        case "filebuffersizeoverride":
                                            filebuffersizeoverride = ParseCapacity(optvalue);
                                            break;

                                        case "compresszmapblocks":
                                            switch (optvalue.ToLower())
                                            {
                                                case "true":
                                                case "1":
                                                    compresszmapblocks = 1;
                                                    break;

                                                case "false":
                                                case "0":
                                                    compresszmapblocks = 0;
                                                    break;

                                                default:
                                                    throw new Exception("Unknown value for 'compresszmapblocks'");
                                            }
                                            break;

                                        case "compressdfschunks":
                                            switch (optvalue.ToLower())
                                            {
                                                case "true":
                                                case "1":
                                                    compressdfschunks = 1;
                                                    break;

                                                case "false":
                                                case "0":
                                                    compressdfschunks = 0;
                                                    break;

                                                default:
                                                    throw new Exception("Unknown value for 'compressdfschunks'");
                                            }
                                            break;

                                        case "btreecapsize":
                                            btreeCapSize = (ulong)AELight.ParseLongCapacity((optvalue));
                                            break;

                                        case "logexechistory":
                                            logexechistory = int.Parse(optvalue);
                                            break;

                                        case "vacuum":
                                            Console.Error.WriteLine("Error: 'vacuum' cannot be used with other options", arg);
                                            return;

                                        case "?":
                                            numargs--;
                                            break;

                                        case "verify":
                                            switch (optvalue.ToLower())
                                            {
                                                case "true":
                                                case "1":
                                                    verify = true;
                                                    break;

                                                case "false":
                                                case "0":
                                                    verify = false;
                                                    break;

                                                default:
                                                    throw new Exception("Unknown value for 'Verify'");
                                            }
                                            break;

                                        case "cooktimeout":
                                            cooktimeout = int.Parse(optvalue);
                                            break;

                                        case "cookretries":
                                            cookretries = int.Parse(optvalue);
                                            break;

                                        case "multithreaded":
                                            switch (optvalue.ToLower())
                                            {
                                                case "true":
                                                case "1":
                                                    mt = true;
                                                    break;

                                                case "false":
                                                case "0":
                                                    mt = false;
                                                    break;

                                                default:
                                                    throw new Exception("Unknown value for 'Multithreaded'");
                                            }                                            
                                            break;

                                        case "metaonly":
                                            switch (optvalue.ToLower())
                                            {
                                                case "true":
                                                case "1":
                                                    metaonly = true;
                                                    break;

                                                case "false":
                                                case "0":
                                                    metaonly = false;
                                                    break;

                                                default:
                                                    throw new Exception("Unknown value for 'MetaOnly'");
                                            }
                                            break;

                                        case "metabackuplocation":
                                        case "metabackup":
                                            metabackuplocation = optvalue;
                                            break;

                                        case "failovertimeout":
                                            failovertimeout = int.Parse(optvalue);
                                            break;

                                        case "failoverdocheck":
                                            failoverdocheck = int.Parse(optvalue);
                                            break;

                                        default:
                                            Console.Error.WriteLine("Error: unknown option for dfs format: {0}", arg);
                                            return;
                                    }
                                }

                                if (0 == numargs)
                                {
                                    Console.Error.WriteLine("Format arguments:");
                                    Console.Error.WriteLine("    Machines=<host1>[,<host2>...]");
                                    Console.Error.WriteLine("    [Processes=<num>]");
                                    //Console.Error.WriteLine("    [SortedProcesses=<num>]");
                                    Console.Error.WriteLine("    [Replication=<num>]");
                                    Console.Error.WriteLine("    [DataNodeBaseSize=<size>]");
                                    //Console.Error.WriteLine("    [ZMapBlockCount=<count>]");
                                    Console.Error.WriteLine("    [ZBlockCount=<size>]");
                                    //Console.Error.WriteLine("    [ZBlockAddBufferSize=<size>]");
                                    //Console.Error.WriteLine("    [ZBlockReadBufferSize=<size>]");
                                    Console.Error.WriteLine("    [FileBufferSizeOverride=<size>]");
                                    Console.Error.WriteLine("    [CompressZMapBlocks=<bool>]");
                                    Console.Error.WriteLine("    [CompressDfsChunks=<bool>]");
                                    Console.Error.WriteLine("    [LogExecHistory=<num>]");
                                    Console.Error.WriteLine("    [BTreeCapSize=<size>]");
                                    Console.Error.WriteLine("    [CookTimeout=<ms>]");
                                    Console.Error.WriteLine("    [CookRetries=<num>]");
                                    Console.Error.WriteLine("    [MetaBackupLocation=<dir>]");
                                    Console.Error.WriteLine("    [Verify=<bool>]");
                                    Console.Error.WriteLine("    [Multithreaded=<bool>]");
                                    Console.Error.WriteLine("    [MetaOnly=<bool>]");
                                    return;
                                }

                                if (null == slavelist)
                                {
                                    Console.Error.WriteLine("Error: \"Machines=<host1>[,<host2>...]\" expected");
                                    SetFailure();
                                    return;
                                }

                                {
                                    Dictionary<string, bool> alls = new Dictionary<string, bool>(slavelist.Length);
                                    foreach (string ss in slavelist)
                                    {
                                        string coolss = IPAddressUtil.GetName(ss);
                                        if (alls.ContainsKey(coolss))
                                        {
                                            Console.Error.WriteLine("host in there twice {0} lol", ss);
                                            SetFailure();
                                            return;
                                        }
                                        alls.Add(coolss, true);
                                    }
                                }

                                if (verify)
                                {
                                    string[] sl = new string[1];
                                    bool vOK = true;
                                    foreach (string s in slavelist)
                                    {
                                        sl[0] = s;
                                        if (!VerifyHostPermissions(sl))
                                        {
                                            Console.Error.WriteLine("Ensure the Windows service is installed and running on '{0}'", s);
                                            vOK = false;
                                        }
                                    }

                                    if (vOK)
                                    {
                                        Console.WriteLine("All machines are verified.");
                                    }
                                    else
                                    {
                                        Console.WriteLine();
                                        Console.Error.WriteLine("Unable to format.");
                                        SetFailure();
                                        return;
                                    }
                                }

                                if (dfs.DfsConfigExists(DFSXMLPATH, 1))
                                {
                                    Console.WriteLine("DFS exists; reformatting...");
                                    Console.WriteLine("Consider running killall after format");
                                    if (!metaonly)
                                    {

                                    }
                                }
                                else
                                {
                                }

                                try
                                {
                                    System.IO.File.Delete(DFSXMLPATH);
                                }
                                catch
                                {
                                }

                                {
                                    dfs dc = new dfs();
                                    dc.InitNew();

                                    string sslavelist = "";
                                    {
                                        StringBuilder sb = new StringBuilder();
                                        for (int i = 0; i < slavelist.Length; i++)
                                        {
                                            if (sb.Length != 0)
                                            {
                                                sb.Append(';');
                                            }
                                            sb.Append(slavelist[i].Trim());
                                        }
                                        sslavelist = sb.ToString();
                                    }
                                    dc.Slaves.SlaveList = sslavelist;

                                    dc.Blocks = new dfs.ConfigBlocks();
                                    if (blockcount <= 0)
                                    {
                                        blockcount = NearestPrimeGE(slavelist.Length * Surrogate.NumberOfProcessors);
                                    }
                                    dc.Blocks.TotalCount = blockcount;

                                    if (sortedblockcount <= 0)
                                    {
                                        sortedblockcount = slavelist.Length * Surrogate.NumberOfProcessors;
                                    }
                                    dc.Blocks.SortedTotalCount = sortedblockcount;

                                    if (datanodebasesize > 0)
                                    {
                                        dc.DataNodeBaseSize = datanodebasesize;
                                    }

                                    if (zmapblockcount > 0)
                                    {
                                        dc.slave.zblocks.count = zmapblockcount;
                                    }

                                    if (replication > 0)
                                    {
                                        if (replication > slavelist.Length)
                                        {
                                            Console.Error.WriteLine("Cannot format with replication factor higher than the number of machines in the cluster (replication {0} > {1} machines)", replication, slavelist.Length);
                                            SetFailure();
                                            return;
                                        }
                                        dc.Replication = replication;
                                    }

                                    if (btreeCapSize > 0)
                                    {
                                        dc.BTreeCapSize = btreeCapSize;
                                    }

                                    if (logexechistory > 0)
                                    {
                                        dc.LogExecHistory = logexechistory;
                                    }

                                    if (zblockcount > 0)
                                    {
                                        dc.slave.zblocks.count = zblockcount;
                                    }

                                    /*if (zblockaddbuffersize > 0)
                                    {
                                        dc.slave.zblocks.addbuffersize = zblockaddbuffersize;
                                    }*/

                                    /*if (zblockreadbuffersize > 0)
                                    {
                                        dc.slave.zblocks.readbuffersize = zblockreadbuffersize;
                                    }*/

                                    if (filebuffersizeoverride > 0)
                                    {
                                        dc.slave.FileBufferSizeOverride = filebuffersizeoverride;
                                    }

                                    if (127 != compressdfschunks)
                                    {
                                        dc.slave.CompressDfsChunks = compressdfschunks;
                                    }

                                    if (127 != compressdfschunks)
                                    {
                                        dc.slave.CompressZMapBlocks = compresszmapblocks;
                                    }

                                    if (cooktimeout >= 0)
                                    {
                                        dc.slave.CookTimeout = cooktimeout;
                                    }

                                    if (cookretries >= 0)
                                    {
                                        dc.slave.CookRetries = cookretries;
                                    }

                                    if (failovertimeout >= 0)
                                    {
                                        dc.FailoverTimeout = failovertimeout; 
                                    }

                                    if (failoverdocheck >= 0)
                                    {
                                        dc.FailoverDoCheck = failoverdocheck;
                                    }

                                    try
                                    {
                                        if (null != metabackuplocation)
                                        {
                                            if (string.Empty == metabackuplocation)
                                            {
                                                dc.MetaBackup = "";
                                                //Console.WriteLine("MetaBackupLocation explicitly disabled");
                                            }
                                            else
                                            {
                                                if (metabackuplocation.StartsWith(@"\\"))
                                                {
                                                    dc.MetaBackup = metabackuplocation;
                                                }
                                                else // If not a network path, make it one (relative to current-machine/surrogate).
                                                {
                                                    // Using GetHostName here becuase during format, the current machine is the surrogate.
                                                    dc.MetaBackup = Surrogate.LocalPathToNetworkPath(metabackuplocation, System.Net.Dns.GetHostName());
                                                }

                                                try
                                                {
                                                    EnsureMetaBackupLocation(dc);
                                                }
                                                catch
                                                {
                                                    dc.MetaBackup = "";
                                                    throw;
                                                }

                                                foreach (string fn in System.IO.Directory.GetFiles(dc.GetMetaBackupLocation()))
                                                {
                                                    System.IO.File.Delete(fn);
                                                }

                                            }
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        LogOutputToFile(e.ToString());
                                        Console.Error.WriteLine(e.Message);
                                    }

                                    //Delete dfsxml from slaves.
                                    {
                                        string[] slaves = dc.Slaves.SlaveList.Split(';');

                                        string self = GetSelfHost(slaves);

                                        foreach (string slave in slaves)
                                        {
                                            if (self != slave)
                                            {
                                                string dfsxmlpath = Surrogate.NetworkPathForHost(slave) + "\\" + dfs.DFSXMLNAME;

                                                // Not using dfs.DfsConfigExists() here because we're testing all slaves.
                                                if (System.IO.File.Exists(dfsxmlpath))
                                                {
                                                    System.IO.File.Delete(dfsxmlpath);
                                                }
                                            }
                                        }
                                    }

                                    UpdateDfsXml(dc);

                                    try
                                    {
                                        // Ensure master isn't an old slave.
                                        System.IO.File.Delete(AELight_Dir + @"\slave.dat");
                                    }
                                    catch
                                    {
                                    }
                                    foreach (string slave in dc.Slaves.SlaveList.Split(';'))
                                    {
                                        WriteSlaveDat(slave);
                                    }

                                    Console.WriteLine("DFS setup: {0} processes on {1} machines", dc.Blocks.TotalCount, slavelist.Length);
                                }
                            }
                        }
                        break;

                    case "invalidate":
                        if (!dfs.DfsConfigExists(DFSXMLPATH))
                        {
                            Console.Error.WriteLine("DFS not setup; use:  {0} format", appname);
                            SetFailure();
                            return;
                        }
                        if (args.Length < 2)
                        {
                            Console.Error.WriteLine("Error: dfs invalidate command needs arguments: <cacheName> <fileNodeName>");
                            SetFailure();
                            return;
                        }
                        DfsInvalidateCachedFileNode(args);
                        break;

                    default:
                        Console.Error.WriteLine("Unrecognized DFS command: " + cmd);
                        SetFailure();
                        return;
                }
            }
        }


        internal static void EnsureMetaBackupLocation(dfs dc)
        {
            // Make sure the backup location is writable and isn't the same as the primary store!
            string mbl = dc.GetMetaBackupLocation();
            if (!mbl.StartsWith(@"\\"))
            {
                throw new Exception("Must supply a network path for new metabackup location");
            }
            if (!System.IO.Path.IsPathRooted(mbl))
            {
                throw new Exception("MetaBackupLocation must be an absolute path");
            }
            try
            {
                System.IO.Directory.CreateDirectory(mbl);
            }
            catch
            {
            }
            string mblfn = @"mbl-" + Guid.NewGuid().ToString() + @".test";
            string mblfp = mbl + @"\" + mblfn;
            try
            {
                System.IO.File.WriteAllText(mblfp, "MetaBackupLocation write test" + Environment.NewLine);
            }
            catch (Exception e)
            {
                throw new Exception("Unable to write to MetaBackupLocation " + mbl, e);
            }
            bool mblfexists = System.IO.File.Exists(AELight_Dir + @"\" + mblfn);
            try
            {
                System.IO.File.Delete(mblfp);
            }
            catch
            {
            }
            if (mblfexists)
            {
                throw new Exception("MetaBackupLocation is the same as the normal meta-data storage location: " + mbl);
            }
        }


        internal static void WriteSlaveDat(string slavehost)
        {
            WriteSlaveDat(slavehost, System.Net.Dns.GetHostName());
        }

        internal static void WriteSlaveDat(string slavehost, string masterhost)
        {
            string slavedat = "master=" + masterhost + Environment.NewLine;
            System.IO.File.WriteAllText(NetworkPathForHost(slavehost) + @"\slave.dat", slavedat);
        }


        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        static extern bool GetDiskFreeSpaceEx(
            string DirectoryName,
            out ulong FreeBytesAvailable,
            out ulong TotalNumberOfBytes,
            out ulong TotalNumberOfFreeBytes);

        internal static ulong GetDiskFreeBytes(string dir)
        {
            if (null != dir)
            {
                if (dir.StartsWith(@"\\"))
                {
                    if (!dir.EndsWith(@"\"))
                    {
                        dir += @"\";
                    }
                }
            }
            ulong fba, tnb, tnfb;
            if (!GetDiskFreeSpaceEx(dir, out fba, out tnb, out tnfb))
            {
                return 0;
            }
            return fba;
        }

    }
}

