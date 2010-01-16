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

//#define VARIABLE_NETWORK_PATHS


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.DirectoryServices;

namespace MySpace.DataMining.AELight
{

    public static class DfsFileTypes
    {
        public static string NORMAL = "zd";
        public static string DELTA = "zsb";
        public static string JOB = "job";
        public static string DLL = "dll";
        public static string BINARY_RECT = "rbin"; // Rectangular binary.
    }


    public class dfs
    {
        
        public const string DFSXMLNAME = "DFS.xml";

        public const string DLL_DIR_NAME = "cac";

        public const string TEMP_FILE_MARKER = "{14D3C051-6E33-4e24-9CB7-C7E085AAA877}";


        [System.Xml.Serialization.XmlIgnore]
        public bool UserModified = false;


        [System.Xml.Serialization.XmlElement("DefaultDebugType")]
        public string DefaultDebugTypeTag = null;
        
        [System.Xml.Serialization.XmlIgnore]
        public string DefaultDebugType
        {
            get
            {
                if (null == DefaultDebugTypeTag)
                {
                    if (System.Net.Dns.GetHostName().StartsWith("ASH2", StringComparison.OrdinalIgnoreCase))
                    {
                        DefaultDebugTypeTag = "proxy";
                    }
                }
                return DefaultDebugTypeTag;
            }
        }


        public string MetaBackup = null;


        // Returns null if no meta backup.
        public string GetMetaBackupLocation()
        {
            if (string.Empty == MetaBackup)
            {
                // Empty means meta backup is disabled, so return null location.
                return null;
            }

            if (null != MetaBackup)
            {
                return MetaBackup;
            }

#if IMPLICIT_META_BACKUP
            string[] slaves = Slaves.SlaveList.Split(';', ',');
            for (int islave = 0; islave < slaves.Length; islave++)
            {
                if (0 == string.Compare(_GetMetaBackupLocation_IPGetNameNoCache(Surrogate.MasterHost),
                    _GetMetaBackupLocation_IPGetNameNoCache(slaves[islave]),
                    StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                return slaves[islave];
            }
#endif

            return null; // No backups.
        }

        static string _GetMetaBackupLocation_IPGetNameNoCache(string ipaddr)
        {
            try
            {
                System.Net.IPHostEntry iphe = System.Net.Dns.GetHostEntry(ipaddr);
                if (null == iphe || null == iphe.HostName)
                {
                    return ipaddr;
                }
                return iphe.HostName;
            }
            catch (Exception e)
            {
                return ipaddr;
            }
        }


        public static string FixXPath(string userfriendlyxpath)
        {
            string result = userfriendlyxpath;
            switch (result)
            {
                case "Machine_MachineList": result = "/dfs/Slaves/SlaveList"; break;
                case "Subprocess_TotalPrime": result = "/dfs/Blocks/TotalCount"; break;
                case "Subprocess_SortedTotalCount": result = "/dfs/Blocks/SortedTotalCount"; break;
                case "Subprocess_IntermediateBlockPrime": result = "/dfs/slave/zblocks/@count"; break;
                case "Subprocess_CompressIntermediateData": result = "/dfs/slave/CompressZMapBlocks"; break;
                case "Subprocess_FileBufferSizeOverride": result = "/dfs/slave/FileBufferSizeOverride"; break;
                case "Subprocess_RetryTimeout": result = "/dfs/slave/CookTimeout"; break;
                case "Subprocess_RetryCount": result = "/dfs/slave/CookRetries"; break;
                case "Subprocess_FoilBaseSkipFactor": result = "/dfs/slave/FoilBaseSkipFactor"; break;
                case "Subprocess_FoilSampleBlockSize": result = "/dfs/slave/FoilSampleBlockSize"; break;
                case "Mrdfs_BlockBaseSize": result = "/dfs/DataNodeBaseSize"; break;
                case "Log_ExecHistory": result = "/dfs/LogExecHistory"; break;
                default:
                    if (!result.StartsWith(@"/"))
                    {
                        result = @"/" + result;
                    }
                    if (!result.StartsWith(@"/dfs/"))
                    {
                        result = @"/dfs" + result;
                    }
                    break;
            }
            return result;
        }


        public void InitNew()
        {
            Slaves = new ConfigSlaves();
            Files = new List<DfsFile>();
            slave = new ConfigSlave();
            slave.zblocks = new ConfigSlave.ConfigZBlocks();
            AccountType = new ConfigAccountType();
        }

        public class ConfigSlaves
        {
            public string SlaveList; // Semicolon-separated.

            public string GetFirstSlave()
            {
                int i = SlaveList.IndexOf(';');
                if (-1 == i)
                {
                    return SlaveList;
                }
                return SlaveList.Substring(0, i);
            }
        }
        public ConfigSlaves Slaves;

        public class ConfigBlocks
        {
            public int TotalCount = -1;
            public int SortedTotalCount = -1;
        }
        public ConfigBlocks Blocks;

        public ulong BTreeCapSize = 4 * 1024 * 1024;

        public class ConfigSlave
        {
            public class ConfigZBlocks
            {
                [System.Xml.Serialization.XmlAttribute]
                public int count = 271;
            }
            public ConfigZBlocks zblocks;

            public byte CompressZMapBlocks = 0;

            public byte CompressDfsChunks = 0; // This is in here because the slave cares too (map input)

            //[System.Xml.Serialization.XmlIgnore]
            //public int ZMapBlockCount = 0; // This is to be the same as Blocks/[Sorted]TotalCount.

            public int FileBufferSizeOverride = 0x1000;

            public int CookTimeout = 1000 * 60;
            public int CookRetries = 1024;

            //public int FoilKeySkipFactor = 5000;
            public int FoilBaseSkipFactor = 1000;

            public int FoilSampleBlockSize = 104857600;

        }
        public ConfigSlave slave;

        public const long DataNodeBaseSize_default = 67108864; // 64 MB
        public long DataNodeBaseSize = DataNodeBaseSize_default;

        // (DataNodeBaseSize / DataNodeSamples) is the seek distance between samples.
        public const int DataNodeSamples_default = 1500;
        public int DataNodeSamples = DataNodeSamples_default;

        public int Replication = 1;

        public int LogExecHistory = 0;

        public int MaxUserLogs = 64;

        public int MaxDGlobals = 64;

        public string DefaultSortedOutputMethod = "rsorted";

        public class ConfigAccountType
        {
            public bool On = false;
            public string Type = "";
            public string Hosts = "";
        }
        public ConfigAccountType AccountType;

        [System.Xml.Serialization.XmlIgnore]
        private int _IntermediateDataAddressing = 32;

        public int IntermediateDataAddressing
        {
            get
            {
                return _IntermediateDataAddressing;
            }

            set
            {
                switch (value)
                {
                    case 32:
                    case 64:
                        _IntermediateDataAddressing = value;
                        break;
                    default:
                        throw new InvalidOperationException("Invalid value for IntermediateDataAddressing: " + value.ToString());
                }
            }
        }

        public class DfsFile
        {
            public string Name;
            public long Size;

            [System.Xml.Serialization.XmlElement("Type")]
            public string XFileType = "zd";

            [System.Xml.Serialization.XmlIgnore]
            public string Type
            {
                get
                {
                    int ic = XFileType.IndexOf('@');
                    if (-1 == ic)
                    {
                        return XFileType;
                    }
                    return XFileType.Substring(0, ic);
                }

                set
                {
                    XFileType = value;
                }
            }

            [System.Xml.Serialization.XmlIgnore]
            public int RecordLength
            {
                get
                {
                    int ic = XFileType.IndexOf('@');
                    if (-1 != ic)
                    {
                        return int.Parse(XFileType.Substring(ic + 1));
                    }
                    return -1;
                }
            }

            /// <summary>
            /// If this file type should have zsa samples; note: doesn't check file system.
            /// </summary>
            [System.Xml.Serialization.XmlIgnore]
            public bool HasZsa
            {
                get
                {
#if DEBUG
                    if (string.IsNullOrEmpty(XFileType))
                    {
                        throw new Exception("DEBUG: HasZsa: (string.IsNullOrEmpty(DfsFile.Type))");
                    }
#endif
                    //return 0 == string.Compare(DfsFileTypes.NORMAL, Type, StringComparison.OrdinalIgnoreCase);
                    return 0 == string.Compare(DfsFileTypes.NORMAL, XFileType, StringComparison.OrdinalIgnoreCase);
                }
            }

            public class FileNode : IComparable<FileNode>
            {
                public string Name;
                public string Host;
                public long Position;
                public long Length;

                public int CompareTo(FileNode that)
                {
                    if (this.Position > that.Position) return 1;
                    if (this.Position < that.Position) return -1;
                    return 0;
                }
            }
            public List<FileNode> Nodes;


            public FileNode FindNode(string nodename)
            {
                for (int i = 0; i < Nodes.Count; i++)
                {
                    if (0 == string.Compare(nodename, Nodes[i].Name, StringComparison.OrdinalIgnoreCase))
                    {
                        return Nodes[i];
                    }
                }
                return null;
            }

        }
        public List<DfsFile> Files;


        public List<DfsFile> FindAll(string dfspathwc)
        {
            dfs dc = this;

            if (dfspathwc.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                dfspathwc = dfspathwc.Substring(6);
            }

            List<DfsFile> result = new List<DfsFile>();

            string srex = Surrogate.WildcardRegexString(dfspathwc);
            System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            foreach (dfs.DfsFile df in dc.Files)
            {
                if (rex.IsMatch(df.Name))
                {
                    result.Add(df);
                }
            }

            return result;
        }


        public DfsFile FindAny(string dfspath)
        {
            dfs dc = this;

            if (dfspath.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                dfspath = dfspath.Substring(6);
            }

            dfs.DfsFile df = null;
            for (int i = 0; i < dc.Files.Count; i++)
            {
                if (0 == string.Compare(dc.Files[i].Name, dfspath, true))
                {
                    df = dc.Files[i];
                    break;
                }
            }
            return df;
        }

        public DfsFile Find(string dfspath, string type)
        {
            dfs dc = this;

            dfs.DfsFile df = FindAny(dfspath);
            if (null == df)
            {
                return null;
            }
            if (0 != string.Compare(type, df.Type, true))
            {
                Console.Error.WriteLine("DFS file '{0}' is not of expected type", df.Name);
                //SetFailure(); // ...
                throw new System.IO.IOException("File type mismatch");
            }
            return df;
        }

        public DfsFile Find(string dfspath)
        {
            return Find(dfspath, "zd");
        }


        public static string MapNodeToNetworkPath(dfs.DfsFile.FileNode node, bool samples)
        {
            string[] chosts = node.Host.Split(';');
            Exception laste = null;
            for (int ci = 0; ci < chosts.Length; ci++)
            {
                try
                {
                    if (samples)
                    {
                        return Surrogate.NetworkPathForHost(chosts[ci]) + @"\" + node.Name + ".zsa";
                    }
                    else
                    {
                        return Surrogate.NetworkPathForHost(chosts[ci]) + @"\" + node.Name;
                    }
                }
                catch (Exception e)
                {
                    laste = e;
                }
            }
            if (null == laste)
            {
                throw new Exception("No valid paths found");
            }
            throw new Exception("No valid paths found", laste);
        }

        public static string MapNodeToNetworkPath(dfs.DfsFile.FileNode node)
        {
            return MapNodeToNetworkPath(node, false);
        }

        // Appended to netpaths.
        public static void MapNodesToNetworkPaths(IList<dfs.DfsFile.FileNode> nodes, List<string> netpaths, bool samples)
        {
            for (int i = 0; i < nodes.Count; i++)
            {
                netpaths.Add(MapNodeToNetworkPath(nodes[i], samples));
            }
        }

        public static void MapNodesToNetworkPaths(IList<dfs.DfsFile.FileNode> nodes, List<string> netpaths)
        {
            MapNodesToNetworkPaths(nodes, netpaths, false);
        }


        public static string MapNodeToNetworkStarPath(dfs.DfsFile.FileNode node, bool samples)
        {
            string[] chosts = node.Host.Split(';');
            StringBuilder sb = new StringBuilder();
            Exception laste = null;
            for(int ci = 0; ci < chosts.Length; ci++)
            {
                try
                {
                    string netdir = Surrogate.NetworkPathForHost(chosts[ci]);
                    if (0 != sb.Length)
                    {
                        sb.Append('*');
                    }
                    if (samples)
                    {
                        sb.Append(netdir + @"\" + node.Name + ".zsa");
                    }
                    else
                    {
                        sb.Append(netdir + @"\" + node.Name);
                    }
                }
                catch(Exception e)
                {
                    laste = e;
                }
            }
            if (0 == sb.Length)
            {
                if (null == laste)
                {
                    throw new Exception("No valid paths found");
                }
                throw new Exception("No valid paths found", laste);
            }
            return sb.ToString();
        }

        public static string MapNodeToNetworkStarPath(dfs.DfsFile.FileNode node)
        {
            return MapNodeToNetworkStarPath(node, false);
        }

        // Appended to netpaths.
        public static void MapNodesToNetworkStarPaths(IList<dfs.DfsFile.FileNode> nodes, List<string> netpaths, bool samples)
        {
            for (int i = 0; i < nodes.Count; i++)
            {
                netpaths.Add(MapNodeToNetworkStarPath(nodes[i], samples));
            }
        }

        public static void MapNodesToNetworkStarPaths(IList<dfs.DfsFile.FileNode> nodes, List<string> netpaths)
        {
            MapNodesToNetworkStarPaths(nodes, netpaths, false);
        }


        public static int FILE_BUFFER_SIZE = Surrogate.DefaultFileBufferSize;


        // iMAX_SECS_RETRY is the number of seconds to keep retrying,
        // Note that this doesn't consider the time spent waiting on I/O.
        // If iMAX_SECS_RETRY is 0, it will still try once.
        public static bool DfsConfigExists(string dfsxmlpath, int iMAX_SECS_RETRY)
        {
            const int ITER_MS_WAIT = 100; // Milliseconds to wait each retry.
            int iters = iMAX_SECS_RETRY * 1000 / ITER_MS_WAIT;
            for (; ; )
            {

                if (System.IO.File.Exists(dfsxmlpath))
                {
                    return true;
                }

                if (--iters < 0)
                {
                    return false;
                }
                System.Threading.Thread.Sleep(ITER_MS_WAIT);

            }
        }

        public static bool DfsConfigExists(string dfsxmlpath)
        {
            return DfsConfigExists(dfsxmlpath, 10);
        }

        public static long GetDfsConfigSize(string dfsxmlpath, int iMAX_SECS_RETRY)
        {
            const int ITER_MS_WAIT = 100; // Milliseconds to wait each retry.
            int iters = iMAX_SECS_RETRY * 1000 / ITER_MS_WAIT;
            for (; ; )
            {
                System.IO.FileInfo fi = new System.IO.FileInfo(dfsxmlpath);
                if (fi.Exists)
                {
                    return fi.Length;
                }

                if (--iters < 0)
                {
                    return -1;
                }
                System.Threading.Thread.Sleep(ITER_MS_WAIT);
            }
        }

        public static long GetDfsConfigSize(string dfsxmlpath)
        {
            return GetDfsConfigSize(dfsxmlpath, 10);
        }

        // applyxpath is 2 elements per xml override: first is xpath, second is new value; null list for no overrides.
        public static dfs ReadDfsConfig_unlocked(string dfsxmlpath, IList<string> applyxpath)
        {
            {
                System.IO.Stream stm = null;
                for (; ; )
                {
                    try
                    {
                        if (null != stm)
                        {
                            stm.Close();
                            stm = null;
                        }

                        {
                            const int iMAX_SECS_RETRY = 10; // Note: doesn't consider the time spent waiting on I/O.
                            const int ITER_MS_WAIT = 100; // Milliseconds to wait each retry.
                            int iters = iMAX_SECS_RETRY * 1000 / ITER_MS_WAIT;
                            for (; ; )
                            {
                                try
                                {
                                    stm = new System.IO.FileStream(dfsxmlpath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
                                    break;
                                }
                                catch (System.IO.FileNotFoundException fnf)
                                {
                                    if (--iters < 0)
                                    {
                                        throw;
                                    }
                                    System.Threading.Thread.Sleep(ITER_MS_WAIT);
                                    continue;
                                }
                            }
                        }

#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif
                        if (null != applyxpath)
                        {
                            System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
                            xd.Load(stm);
                            for (int i = 0; i < applyxpath.Count; i += 2)
                            {
                                System.Xml.XmlNodeList xnl = xd.SelectNodes(applyxpath[i + 0]);
                                for (int j = 0; j < xnl.Count; j++)
                                {
                                    xnl[j].InnerText = applyxpath[i + 1];
                                }
                            }
                            stm.Close();
                            stm = new System.IO.MemoryStream(Encoding.UTF8.GetBytes(xd.InnerXml));
                        }

                        using (System.IO.StreamReader srconfig = new System.IO.StreamReader(stm))
                        {
                            System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(dfs));
                            dfs dc = (dfs)xs.Deserialize(srconfig);
                            dc.UserModified = null != applyxpath;
                            if (null == dc.Files)
                            {
                                dc.Files = new List<dfs.DfsFile>();
                            }
                            /*for (int i = 0; i < dc.Files.Count; i++)
                            {
                                if (-1 != dc.Files[i].Size)
                                {
                                    dc.Files[i].Nodes.Sort();
                                }
                            }*/
                            if (null == dc.slave)
                            {
                                dc.slave = new dfs.ConfigSlave();
                            }
                            if (null == dc.slave.zblocks)
                            {
                                dc.slave.zblocks = new dfs.ConfigSlave.ConfigZBlocks();
                            }
                            if (dc.slave.FileBufferSizeOverride > 0)
                            {
                                FILE_BUFFER_SIZE = dc.slave.FileBufferSizeOverride;
                            }
                            if (dc.Blocks.SortedTotalCount <= 0)
                            {
                                dc.Blocks.SortedTotalCount = dc.Slaves.SlaveList.Split(';', ',').Length * Surrogate.NumberOfProcessors;
                            }
                            return dc;
                        }
                    }
                    catch (System.IO.FileNotFoundException e)
                    {
                        //throw;
                        throw new System.IO.FileNotFoundException("DFS does not exist; see dfs format", e);
                    }
                    catch (System.IO.IOException e)
                    {
                        continue;
                    }
                    break;
                }
            }
        }

        public static dfs ReadDfsConfig_unlocked(string dfsxmlpath)
        {
            return ReadDfsConfig_unlocked(dfsxmlpath, null);
        }


        class AEDFSM : IDisposable
        {
            internal System.Threading.Mutex mutex;

            public void Dispose()
            {
                if (null != mutex)
                {
                    mutex.ReleaseMutex();
                    mutex.Close();
                    mutex = null;
                }
            }
        }


        public static IDisposable LockDfsMutex()
        {
            AEDFSM x = new AEDFSM();
            System.Threading.Mutex mutex = null;
            mutex = new System.Threading.Mutex(false, "AEDFSM");
            try
            {
                mutex.WaitOne(); // Lock also taken by kill.
            }
            catch (System.Threading.AbandonedMutexException)
            {
            }
            x.mutex = mutex;
            return x;
        }


        static char[] wcchars = new char[] { '*', '?' };

        public List<string> SplitInputPaths(string pathlist, bool StripRecordInfo)
        {
            dfs dc = this;
            List<string> result = new List<string>();
            foreach (string _path in pathlist.Split(';'))
            {
                string path = _path.Trim();
                string pathappend = "";
                {
                    int ic = path.IndexOf('@');
                    if (-1 != ic)
                    {
                        if (!StripRecordInfo)
                        {
                            pathappend = path.Substring(ic);
                        }
                        path = path.Substring(0, ic);
                    }
                }
                if (0 != path.Length)
                {
                    if (path.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        path = path.Substring(6);
                    }

                    bool adv = false;
                    if (path.IndexOf('|') > -1)
                    {
                        System.Text.RegularExpressions.Regex regx = new System.Text.RegularExpressions.Regex(@"\|(\d+)-(\d+)\|");
                        System.Text.RegularExpressions.Match match = regx.Match(path);
                        if (match != null && match.Groups.Count == 3)
                        {
                            adv = true;
                            int min = Int32.Parse(match.Groups[1].Value);
                            int max = Int32.Parse(match.Groups[2].Value);
                            string s1 = Surrogate.WildcardRegexString(path.Substring(0, match.Index)).Trim('$');
                            string s2 = Surrogate.WildcardRegexString(path.Substring(match.Index + match.Length)).Trim('^');
                            regx = new System.Text.RegularExpressions.Regex(s1 + @"(\d+)" + s2);

                            foreach (dfs.DfsFile df in dc.Files)
                            {
                                if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, true)
                                    || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, true))
                                {
                                    match = regx.Match(df.Name);
                                    if (match != null && match.Groups.Count == 2)
                                    {
                                        int num = Int32.Parse(match.Groups[1].Value);
                                        if (num >= min && num <= max)
                                        {
                                            result.Add("dfs://" + df.Name + pathappend);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (!adv)
                    {
                        if (-1 != path.IndexOfAny(wcchars))
                        {
                            string srex = Surrogate.WildcardRegexString(path);
                            System.Text.RegularExpressions.Regex rex = new System.Text.RegularExpressions.Regex(srex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                            foreach (dfs.DfsFile df in dc.Files)
                            {
                                if (0 == string.Compare(df.Type, DfsFileTypes.NORMAL, true)
                                    || 0 == string.Compare(df.Type, DfsFileTypes.BINARY_RECT, true))
                                {
                                    if (rex.IsMatch(df.Name))
                                    {
                                        result.Add("dfs://" + df.Name + pathappend);
                                    }
                                }
                            }
                        }
                        else
                        {
                            result.Add(path + pathappend);
                        }
                    }
                }
            }
            return result;
        }

        public List<string> SplitInputPaths(string pathlist)
        {
            return SplitInputPaths(pathlist, false);
        }

        public static bool IsBadFilename(string filename, out string reason)
        {
            if (filename.Length == 0)
            {
                reason = "Empty name";
                return true;
            }
            for (int i = 0; i < filename.Length; i++)
            {
                if (!char.IsLetterOrDigit(filename[i])
                    && '_' != filename[i]
                    && '-' != filename[i]                    
                    && '.' != filename[i]
                    && '~' != filename[i]
                    && '(' != filename[i]
                    && ')' != filename[i]
                    && '{' != filename[i]
                    && '}' != filename[i]
                    )
                {
                    reason = "Illegal characters in name";
                    return true;
                }
            }
            reason = "Good";
            return false;
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

        public static string GenerateDataNodeName(string dfspath, string prefix, long jid, string suffix)
        {
            return GenerateDataNodeName(dfspath, prefix, ".j" + jid.ToString() + suffix);
        }

        public static string GenerateZdFileDataNodeName(string dfspath)
        {
            return GenerateDataNodeName(dfspath, "zd.", ".zd");
        }

        public static string GenerateZdFileDataNodeName(string dfspath, long jid)
        {
            return GenerateDataNodeName(dfspath, "zd.", jid, ".zd");
        }

    }


    public class DfsFileNodeIOException : System.IO.IOException
    {
        public DfsFileNodeIOException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public DfsFileNodeIOException(string message)
            : base(message)
        {
        }
    }

    public class DfsFileNodeIOExceptionRetry : DfsFileNodeIOException
    {
        public DfsFileNodeIOExceptionRetry(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public DfsFileNodeIOExceptionRetry(string message)
            : base(message)
        {
        }
    }

    public class DfsFileNodeWriteException : DfsFileNodeIOExceptionRetry
    {
        public DfsFileNodeWriteException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public DfsFileNodeWriteException(string message)
            : base(message)
        {
        }
    }


    // Note: file headers are not considered.
    // Compression can be used on top of this stream.
    public class DfsFileNodeStream : System.IO.Stream
    {
        public DfsFileNodeStream(string[] fullnames, bool failover,
            System.IO.FileMode fmode, System.IO.FileAccess faccess, System.IO.FileShare fshare, int fbuffersize)
        {
            if (null == fullnames || string.IsNullOrEmpty(fullnames[0]))
            {
                throw new DfsFileNodeIOException("Invalid input DFS file chunk paths");
            }
            this.fullnames = fullnames;
            fcount = fullnames.Length;
            this.failover = failover;

            this.fmode = fmode;
            this.faccess = faccess;
            this.fshare = fshare;
            this.fbuffersize = fbuffersize;

            _firstopen();
        }

        public DfsFileNodeStream(string allhosts, string name, bool failover,
            System.IO.FileMode fmode, System.IO.FileAccess faccess, System.IO.FileShare fshare, int fbuffersize)
        {
            if (null == allhosts || null == name)
            {
                throw new DfsFileNodeIOException("Invalid input DFS file chunk paths");
            }
            this.chosts = allhosts.Split(';');
            if (null == chosts || chosts.Length == 0 || string.IsNullOrEmpty(chosts[0]))
            {
                throw new DfsFileNodeIOException("No hosts for DFS file chunk '" + name + "'");
            }
            fcount = chosts.Length;
            this.name = name;
            this.failover = failover;

            this.fmode = fmode;
            this.faccess = faccess;
            this.fshare = fshare;
            this.fbuffersize = fbuffersize;

            _firstopen();
        }

        public DfsFileNodeStream(dfs.DfsFile.FileNode fnode, bool failover,
            System.IO.FileMode fmode, System.IO.FileAccess faccess, System.IO.FileShare fshare, int fbuffersize)
            : this(fnode.Host, fnode.Name, failover, fmode, faccess, fshare, fbuffersize)
        {
        }

        public DfsFileNodeStream(string starfullnames, bool failover,
            System.IO.FileMode fmode, System.IO.FileAccess faccess, System.IO.FileShare fshare, int fbuffersize)
            : this(starfullnames.Split('*'), failover, fmode, faccess, fshare, fbuffersize)
        {
        }


        public override bool CanRead
        {
            get
            {
                try
                {
                    return stm.CanRead;
                }
                catch (Exception e)
                {
                    LastFailoverException = e;
                }
                return true;
            }
        }

        public override bool CanSeek
        {
            get
            {
                try
                {
                    return stm.CanSeek;
                }
                catch(Exception e)
                {
                    LastFailoverException = e;
                }
                return true;
            }
        }

        public override bool CanWrite
        {
            get
            {
                try
                {
                    return stm.CanWrite;
                }
                catch (Exception e)
                {
                    LastFailoverException = e;
                }
                return false; // ...
            }
        }

        public override void Close()
        {
            stmdone = true;
            if (null != stm)
            {
                for (; ; )
                {
                    try
                    {
                        stm.Close();
                        return;
                    }
                    catch (Exception e)
                    {
                        LastFailoverException = e;
                        if (pendingwrites)
                        {
                            throw new DfsFileNodeIOExceptionRetry("Close failure; cannot confirm write flush", LastFailoverException);
                        }
                        throw new DfsFileNodeIOException("Close failure", LastFailoverException);
                    }
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            stmdone = true;
            if (null != stm)
            {
                for (; ; )
                {
                    try
                    {
                        //stm.Dispose(disposing); // Can't.
                        stm.Dispose(); // ...
                        return;
                    }
                    catch (Exception e)
                    {
                        LastFailoverException = e;
                        if (pendingwrites)
                        {
                            throw new DfsFileNodeIOExceptionRetry("Dispose failure; cannot confirm write flush", LastFailoverException);
                        }
                        throw new DfsFileNodeIOException("Dispose failure", LastFailoverException);
                    }
                }
            }
        }

        public override void Flush()
        {
            if (pendingwrites)
            {
                try
                {
                    stm.Flush();
                    pendingwrites = false; // !
                }
                catch (Exception e)
                {
                    // Need to fail immediately for mid-writing errors, since the state of the file is unknown on remote side.
                    LastFailoverException = e;
                    throw new DfsFileNodeWriteException("Unable to flush DFS file chunk", LastFailoverException);
                }
            }
        }

        public override long Length
        {
            get
            {
                for (; ; )
                {
                    try
                    {
                        return stm.Length;
                    }
                    catch (Exception e)
                    {
                        LastFailoverException = e;
                        if (!failover)
                        {
                            throw new DfsFileNodeIOException("Length: failover not enabled", LastFailoverException);
                        }
                        _dofailover("Length");
                    }
                }
            }
        }

        public override long Position
        {
            get
            {
                // Note: getting Position directly from stm would be slower and need failover.
                return stmpos;
            }
            set
            {
                this.Seek(value, System.IO.SeekOrigin.Begin); // Has failover as necessary.
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            for (; ; )
            {
                try
                {
                    int result = stm.Read(buffer, offset, count);
                    if (result > 0)
                    {
                        stmpos += result;
                    }
                    return result;
                }
                catch (Exception e)
                {
                    LastFailoverException = e;
                    if (!failover)
                    {
                        throw new DfsFileNodeIOException("Read: failover not enabled", LastFailoverException);
                    }
                    _dofailover("Read");
                }
            }
        }

        public override int ReadByte()
        {
            for (; ; )
            {
                try
                {
                    int result = stm.ReadByte();
                    if (result >= 0)
                    {
                        stmpos++;
                    }
                    return result;
                }
                catch (Exception e)
                {
                    LastFailoverException = e;
                    if (!failover)
                    {
                        throw new DfsFileNodeIOException("ReadByte: failover not enabled", LastFailoverException);
                    }
                    _dofailover("ReadByte");
                }
            }
        }

        public override long Seek(long offset, System.IO.SeekOrigin origin)
        {
            for (; ; )
            {
                try
                {
                    return stmpos = stm.Seek(offset, origin);
                }
                catch(Exception e)
                {
                    LastFailoverException = e;
                    if (!failover)
                    {
                        throw new DfsFileNodeIOException("Seek: failover not enabled", LastFailoverException);
                    }
                    _dofailover("Seek");
                }
            }
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (pendingwrites)
            {
                try
                {
                    stm.Write(buffer, offset, count);
                }
                catch (Exception e)
                {
                    // Need to fail immediately for mid-writing errors, since the state of the file is unknown on remote side.
                    LastFailoverException = e;
                    throw new DfsFileNodeWriteException("Unable to write to DFS file chunk", LastFailoverException);
                }
            }
            else
            {
                for (; ; )
                {
                    try
                    {
                        stm.Write(buffer, offset, count);
                        pendingwrites = true; // !
                        stmpos += count;
                        return;
                    }
                    catch (Exception e)
                    {
                        LastFailoverException = e;
                        if (!failover)
                        {
                            throw new DfsFileNodeIOException("Write: failover not enabled", LastFailoverException);
                        }
                        _dofailover("Write");
                    }
                }
            }
        }

        public override void WriteByte(byte value)
        {
            if (pendingwrites)
            {
                try
                {
                    stm.WriteByte(value);
                }
                catch (Exception e)
                {
                    // Need to fail immediately for mid-writing errors, since the state of the file is unknown on remote side.
                    LastFailoverException = e;
                    throw new DfsFileNodeWriteException("Unable to write (WriteByte) to DFS file chunk", LastFailoverException);
                }
            }
            else
            {
                for (; ; )
                {
                    try
                    {
                        stm.WriteByte(value);
                        pendingwrites = true; // !
                        stmpos++;
                        return;
                    }
                    catch (Exception e)
                    {
                        LastFailoverException = e;
                        if (!failover)
                        {
                            throw new DfsFileNodeIOException("WriteByte: failover not enabled", LastFailoverException);
                        }
                        _dofailover("WriteByte");
                    }
                }
            }
        }


        void _firstopen()
        {
            lastfailoverindex = fcount - 1;
            stmpos = 0;
            pendingwrites = false;
            xfailovers = 0;
            stmdone = false;
            _dofailover("Open", false); // currentfailed=false
        }

        bool _dofailoverfile(int f)
        {
            if (null != stm)
            {
                stm.Close();
                stm = null;
            }
            if (++xfailovers > fcount + 5)
            {
                throw new DfsFileNodeIOExceptionRetry("Too many attempts to failover from this machine");
            }
            try
            {
                if (null != fullnames)
                {
                    _fullname = fullnames[f];
                }
                else
                {
                    _fullname = Surrogate.NetworkPathForHost(chosts[f]) + @"\\" + name;
                }
                stm = new System.IO.FileStream(_fullname, fmode, faccess, fshare, fbuffersize);
                if (0 != stmpos)
                {
                    Position = stmpos; // !
                }
                return true; // Good!
            }
            catch (Exception e)
            {
                LastFailoverException = e;
                if (!failover)
                {
                    throw new DfsFileNodeIOException("Unable to open DFS file chunk - failover not enabled", LastFailoverException);
                }
            }
            return false;
        }

        void _dofailover(string method, bool currentfailed)
        {
            if (currentfailed)
            {
                if (UnhealthyHostSeconds > 0)
                {
                    AddUnhealthyHost(GetHostForFileIndex(lastfailoverindex));
                }
            }
            int oldlastfailoverindex = lastfailoverindex;
            for (++lastfailoverindex; lastfailoverindex < fcount; lastfailoverindex++)
            {
                string h = GetHostForFileIndex(lastfailoverindex);
                if (!IsHostUnhealthy(h))
                {
                    if (_dofailoverfile(lastfailoverindex))
                    {
                        return;
                    }
                    //currentfailed = true;
                    AddUnhealthyHost(h);
                }
            }
            for (lastfailoverindex = 0; lastfailoverindex <= oldlastfailoverindex; lastfailoverindex++)
            {
                string h = GetHostForFileIndex(lastfailoverindex);
                if (!IsHostUnhealthy(h))
                {
                    if (_dofailoverfile(lastfailoverindex))
                    {
                        return;
                    }
                    //currentfailed = true;
                    AddUnhealthyHost(h);
                }
            }
            if (null != LastFailoverException)
            {
                throw new DfsFileNodeIOExceptionRetry(method + ": unable to failover", LastFailoverException);
            }
            throw new DfsFileNodeIOException(method + ": unable to failover");
        }

        void _dofailover(string method)
        {
            _dofailover(method, true);
        }


        // Useful when writing to find actual full file written.
        // Changes as failover occurs; can be null; might file not exist if exception.
        public string FullName
        {
            get
            {
                return _fullname;
            }
        }


        System.IO.Stream stm;
        long stmpos;
        int lastfailoverindex;
        bool pendingwrites;
        int xfailovers;
        bool stmdone;

        // One or the other can be null.
        string[] chosts;
        string[] fullnames;
        int fcount;

        string name;
        bool failover;
        string _fullname;

        public bool HasFailover
        {
            get { return failover; }
        }

        public Exception LastFailoverException;

        System.IO.FileMode fmode;
        System.IO.FileAccess faccess;
        System.IO.FileShare fshare;
        int fbuffersize;


        string GetHostForFileIndex(int f)
        {
            if (null != chosts)
            {
                return chosts[f];
            }
            else
            {
                string fn = fullnames[f];
                if (!fn.StartsWith(@"\\"))
                {
                    return System.Net.Dns.GetHostName(); // ...
                }
                else
                {
                    int xs = fn.IndexOf('\\', 2);
                    if (-1 == xs)
                    {
                        return fn.Substring(2);
                    }
                    return fn.Substring(2, xs - 2);
                }
            }
        }


        // Seconds.
        public static int UnhealthyHostSeconds
        {
            get
            {
                return unhealthysecs;
            }

            set
            {
                lock (typeof(DfsFileNodeStream))
                {
                    if (value > 0)
                    {
                        /*if (null == unhealthy)
                        {
                            unhealthy = new Dictionary<string, int>(new Surrogate.CaseInsensitiveEqualityComparer());
                        }*/
                    }
                    else
                    {
                        unhealthy = null;
                    }
                }
                unhealthysecs = value;
            }
        }

        static int unhealthysecs = 300; // 5mins default.
        static Dictionary<string, int> unhealthy; // Indexed by hostname.


        static void AddUnhealthyHost(string host)
        {
            lock (typeof(DfsFileNodeStream))
            {
                if (null == unhealthy)
                {
                    unhealthy = new Dictionary<string, int>();
                }
                //if (!unhealthy.ContainsKey(host))
                {
                    unhealthy[host] = CurrentTs();
                }
            }
        }

        static bool IsHostUnhealthy(string host)
        {
            lock (typeof(DfsFileNodeStream))
            {
                if (null == unhealthy)
                {
                    return false;
                }
                if (unhealthy.ContainsKey(host))
                {
                    int uts = unhealthy[host];
                    if (uts + unhealthysecs < CurrentTs())
                    {
                        unhealthy.Remove(host);
                        return false; // Time expired; try again.
                    }
                    return true; // Still unhealthy!
                }
                return false;
            }
        }

        static int CurrentTs()
        {
            return (int)(DateTime.Now - new DateTime(2000, 1, 1)).TotalSeconds;
        }


    }


    public class SourceCode
    {

        static System.IO.Stream _ApplyXPathSets(IList<string> xpathsets, System.IO.Stream stm)
        {
            System.Xml.XmlDataDocument xd = new System.Xml.XmlDataDocument();
            xd.Load(stm);
            foreach (string xpathset in xpathsets)
            {
                //int ieq = xpath.IndexOf('=');
                int ieq = -1;
                bool insq = false; // In single quotes?
                int nbracks = 0; // Number of brackets nested.
                int lastslashn = -1;
                for (int j = 0; j < xpathset.Length; j++)
                {
                    switch (xpathset[j])
                    {
                        case '[':
                            nbracks++;
                            break;

                        case ']':
                            if (nbracks > 0)
                            {
                                nbracks--;
                            }
                            break;

                        case '\'':
                            insq = !insq;
                            break;

                        case '=':
                            if (!insq
                                && 0 == nbracks)
                            {
                                ieq = j;
                                j = int.MaxValue - 1; // Break out of loop.
                            }
                            break;

                        case '/':
                            if (!insq
                                && 0 == nbracks)
                            {
                                lastslashn = j;
                            }
                            break;
                    }
                }
                if (-1 != ieq)
                {
                    string n = xpathset.Substring(0, ieq);
                    string v = xpathset.Substring(ieq + 1);
                    System.Xml.XmlNodeList xnl = xd.SelectNodes(n);
                    if (0 == xnl.Count)
                    {
#if DEBUG
                        //System.Threading.Thread.Sleep(1000 * 8);
#endif
                        if (-1 != lastslashn)
                        {
                            xnl = xd.SelectNodes(n.Substring(0, lastslashn));
                            if (0 == xnl.Count)
                            {
                                throw new System.Xml.XmlException("Cannot add XPath node: " + n);
                            }
                            else
                            {
                                for (int j = 0; j < xnl.Count; j++)
                                {
                                    System.Xml.XmlNode child;
                                    string xn = n.Substring(lastslashn + 1);
                                    if (xn.StartsWith("@"))
                                    {
                                        child = xd.CreateAttribute(xn.Substring(1));
                                    }
                                    else
                                    {
                                        child = xd.CreateElement(xn);
                                    }
                                    child.InnerText = v;
                                    xnl[j].AppendChild(child);
                                }
                            }
                        }
                    }
                    else
                    {
                        for (int j = 0; j < xnl.Count; j++)
                        {
                            xnl[j].InnerText = v;
                        }
                    }
                }
                else
                {
                    throw new System.Xml.XmlException("Expected new value for XPath: " + xpathset);
                }
            }
            return new System.IO.MemoryStream(Encoding.UTF8.GetBytes(xd.InnerXml));
        }

        static SourceCode _Load(IList<string> xpathsets, string xmlfilepath)
        {
#if DEBUG
            //System.Threading.Thread.Sleep(1000 * 8);
#endif
            System.IO.Stream stm = stm = new System.IO.FileStream(xmlfilepath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
            SourceCode result;
            try
            {
                if (null != xpathsets)
                {
                    stm = _ApplyXPathSets(xpathsets, stm);
                }
                System.IO.StreamReader srconfig = new System.IO.StreamReader(stm);
                System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(SourceCode));
                result = (SourceCode)xs.Deserialize(srconfig);
            }
            finally
            {
                stm.Close();
            }
            return result;
        }

        static void _LoadVerify(SourceCode cfg, string sourcename)
        {
            if (null == cfg.Jobs) throw new Exception("Missing <Job> in jobs SourceCode " + sourcename);
            for (int i = 0; i < cfg.Jobs.Length; i++)
            {
                if (cfg.Jobs[i].IOSettings == null) throw new Exception("Missing <IOSettings> in jobs SourceCode " + sourcename);
                if (cfg.Jobs[i].MapReduce != null)
                {
                    if (cfg.Jobs[i].MapReduce.Map.Trim().Length == 0)
                    {
                        cfg.Jobs[i].MapReduce.Map = "";
                    }
                    if (cfg.Jobs[i].MapReduce.DirectSlaveLoad.Trim().Length == 0)
                    {
                        cfg.Jobs[i].MapReduce.DirectSlaveLoad = "";
                    }
                }
            }
        }

        public static SourceCode Load(IList<string> xpathsets, string xmlfilepath)
        {
            SourceCode cfg = _Load(xpathsets, xmlfilepath);
            _LoadVerify(cfg, "file " + xmlfilepath);
            return cfg;
        }

        public static SourceCode Load(string xmlfilepath)
        {
            return Load(null, xmlfilepath);
        }

        public static SourceCode LoadXml(IList<string> xpathsets, string xml)
        {
            SourceCode cfg;
            System.Xml.Serialization.XmlSerializer xs = new System.Xml.Serialization.XmlSerializer(typeof(SourceCode));
            if (null != xpathsets)
            {
                System.IO.Stream stm = new System.IO.MemoryStream(Encoding.UTF8.GetBytes(xml));
                stm = _ApplyXPathSets(xpathsets, stm);
                cfg = (SourceCode)xs.Deserialize(stm);
            }
            else
            {
                cfg = (SourceCode)xs.Deserialize(new System.IO.StringReader(xml));
            }
            _LoadVerify(cfg, "from Editor");
            return cfg;
        }

        public static SourceCode LoadXml(string xml)
        {
            return LoadXml(null, xml);
        }


        public class Job
        {

            public class ConfigAdd
            {
                [System.Xml.Serialization.XmlAttribute]
                public string Reference;
                [System.Xml.Serialization.XmlAttribute]
                public string Type = "dfs"; // Default type.
            }
            [System.Xml.Serialization.XmlElement("Add")]
            public ConfigAdd[] Adds;

            public int AssemblyReferencesCount
            {
                get
                {
                    if (null == Adds)
                    {
                        return 0;
                    }
                    return Adds.Length;
                }
            }

            public void AddAssemblyReferences(List<string> list, string reldir)
            {
                if (null == Adds)
                {
                    return; // None specified.
                }
                if (null == reldir)
                {
                    reldir = Environment.CurrentDirectory;
                }

                for (int i = 0; i < Adds.Length; i++)
                {
                    if (Adds[i].Reference == null)
                    {
                        throw new Exception("Assembly reference #" + (i + 1).ToString() + " is null");
                    }
                    if (-1 != Adds[i].Reference.IndexOfAny(new char[] { '/', '\\' }))
                    {
                        throw new Exception("Assembly reference #" + (i + 1).ToString() + " cannot specify a directory, must be file name only: " + Adds[i].Reference);
                    }

                    if (0 == string.Compare(Adds[i].Type, "dfs", StringComparison.OrdinalIgnoreCase))
                    {
                        string dllpath = reldir + @"\" + dfs.DLL_DIR_NAME + @"\" + Adds[i].Reference;
                        if (!System.IO.File.Exists(dllpath))
                        {
                            throw new Exception("DLL does not exist in DFS: " + Adds[i].Reference);
                        }
                        list.Add(dllpath);
                    }
                    else if (0 == string.Compare(Adds[i].Type, "system", StringComparison.OrdinalIgnoreCase))
                    {
                        string xdllpath = reldir + @"\" + dfs.DLL_DIR_NAME + @"\" + Adds[i].Reference;
                        if (System.IO.File.Exists(xdllpath))
                        {
                            throw new Exception("DLL is not a system DLL, or a file with the same name exists in DFS: " + Adds[i].Reference);
                        }
                        list.Add(Adds[i].Reference);
                    }
                    else
                    {
                        throw new Exception("Unsupported reference type '" + Adds[i].Type + "' for reference '" + ((null != Adds[i].Reference) ? Adds[i].Reference : "<null>") + "'");
                    }
                }

            }

            public void AddAssemblyReferences(List<string> list)
            {
                AddAssemblyReferences(list, null);
            }

            public void ExpandDFSIOMultis(int machineCount, int coreCount)
            {
                if (this.IOSettings.DFS_IO_Multis != null)
                {
                    List<SourceCode.Job.ConfigIOSettings.DFS_IO> newios = new List<SourceCode.Job.ConfigIOSettings.DFS_IO>();
                    foreach (SourceCode.Job.ConfigIOSettings.DFS_IO_Multi multi in this.IOSettings.DFS_IO_Multis)
                    {
                        string mode = multi.Mode.ToLower();
                        int factor = 0;
                        if (mode == "all machines")
                        {
                            factor = machineCount;
                        }
                        else if (mode == "all cores")
                        {
                            factor = coreCount * machineCount;
                        }
                        else
                        {
                            throw new Exception("Invalid mode in remote DFS_IO_Multi: " + mode);
                        }

                        int pad = 0;
                        int shp = multi.DFSWriter.IndexOf('#');
                        string plh = "";
                        if (shp > -1)
                        {
                            pad = multi.DFSWriter.LastIndexOf('#') - shp + 1;
                            plh = multi.DFSWriter.Substring(shp, pad);
                        }

                        for (int i = 0; i < factor; i++)
                        {
                            SourceCode.Job.ConfigIOSettings.DFS_IO newio = new SourceCode.Job.ConfigIOSettings.DFS_IO();
                            newio.DFSReader = multi.DFSReader;
                            if (multi.DFSWriter.Length > 0)
                            {
                                newio.DFSWriter = multi.DFSWriter.Replace(plh, (i + 1).ToString().PadLeft(pad, '0'));
                            }
                            else
                            {
                                newio.DFSWriter = "";
                            }
                            newio.Meta = multi.Meta;
                            
                            newios.Add(newio);
                        }
                    }

                    if (newios.Count > 0)
                    {
                        if (this.IOSettings.DFS_IOs != null)
                        {
                            newios.AddRange(this.IOSettings.DFS_IOs);
                        }
                        this.IOSettings.DFS_IOs = newios.ToArray();
                    }
                }
            }

            [System.Xml.Serialization.XmlAttribute("Name")]
            public string NameAttribute = null;

            [System.Xml.Serialization.XmlAttribute("Custodian")]
            public string CustodianAttribute = null;

            [System.Xml.Serialization.XmlAttribute("Email")]
            public string Email = null;

            [System.Xml.Serialization.XmlAttribute("email")]
            public string email = null;

            public string EmailAttribute
            {
                get
                {
                    if (Email != null)
                    {
                        return Email;
                    }
                    if (email != null)
                    {
                        return email;
                    }
                    return "";                
                }
            }

            public string NarrativeName
            {
                get
                {
                    if (null != NameAttribute)
                    {
                        return NameAttribute;
                    }
                    if (null != Narrative && null != Narrative.Name)
                    {
                        return Narrative.Name;
                    }
                    return "<N/A>";
                }
            }

            public string NarrativeCustodian
            {
                get
                {
                    if (null != CustodianAttribute)
                    {
                        return CustodianAttribute;
                    }
                    if (null != Narrative && null != Narrative.Custodian)
                    {
                        return Narrative.Name;
                    }
                    return "<N/A>";
                }
            }

            public string NarrativeEmail
            {
                get
                {
                    if (null != EmailAttribute && EmailAttribute.Length > 0)
                    {
                        return EmailAttribute;
                    }
                    if (null != Narrative && null != Narrative.email)
                    {
                        return Narrative.email;
                    }
                    return "";
                }
            }


            public class ConfigNarrative
            {
                public string Name = null;
                public string Custodian = null;
                public string email = null;

            }
            public ConfigNarrative Narrative;

            public class ConfigCache
            {
                public string Name = "";
                public string DFSInput = "";
            }
            public ConfigCache Delta; // Snowball/Cache

            public bool IsDeltaSpecified
            {
                get
                {
                    return null != Delta
                        && !string.IsNullOrEmpty(Delta.Name)
                        && !string.IsNullOrEmpty(Delta.DFSInput);
                }
            }
         
            public string AutoRetry = ""; // false
            private int _ijobfailover = -1;
            public bool IsJobFailoverEnabled
            {
                get
                {
                    if (-1 == _ijobfailover)
                    {
                        _ijobfailover = 0;
                        if (0 == string.Compare("true", AutoRetry, StringComparison.OrdinalIgnoreCase))
                        {
                            _ijobfailover = 1;
                        }
                    }
                    return 1 == _ijobfailover;
                }
            }


            public string Verbose = "";

            public string ExchangeOrder = "shuffle";


            [System.Xml.Serialization.XmlIgnore]
            private int _IntermediateDataAddressing = 0;

            // Returns 0 if the default value should be used.
            public int IntermediateDataAddressing
            {
                get
                {
                    return _IntermediateDataAddressing;
                }

                set
                {
                    switch (value)
                    {
                        case 32:
                        case 64:
                            _IntermediateDataAddressing = value;
                            break;
                        default:
                            throw new InvalidOperationException("Invalid value for IntermediateDataAddressing: " + value.ToString());
                    }
                }
            }


            public class ConfigIOSettings
            {
                public string JobType = "";

                [System.Xml.Serialization.XmlElement("KeyLength")]
                public string StringKeyLength = "0";
                int _keylen = -1;
                public int KeyLength
                {
                    get
                    {
                        if (-1 == _keylen)
                        {
                            if (StringKeyLength.Length == 0)
                            {
                                throw new Exception("KeyLength is required.");
                            }

                            try
                            {
                                _keylen = Surrogate.GetRecordSize(StringKeyLength);
                            }
                            catch (Exception e)
                            {
                                throw new Exception("KeyLength is invalid.", e);
                            }
                            
                        }
                        return _keylen;
                    }
                }

                public int KeyMajor = 0; // how many bytes to consider in key for modding... 1 means one byte, -1 means all but one, default is -0
                public string DFSInput = "";
                public string DFSOutput = "";
                public string OutputMethod = "grouped"; // grouped or sorted
                public string OutputDirection = "ascending";
                public string CompilerOptions = "";
                public string CompilerVersion = "";

                private string _host = null;
                public string LocalHost
                {
                    get
                    {
                        return _host;
                    }
                    set
                    {
                        _host = value;
                    }
                }
                public string Host
                {
                    get
                    {
                        return _host;
                    }
                    set
                    {
                        _host = value;
                    }
                }

                public class DFS_IO
                {
                    public string DFSReader = "";
                    public string DFSWriter = "";
                    public string Host = ""; // Host of "Remote" for this I/O.
                    public string Meta = "";
                }
                [System.Xml.Serialization.XmlElement("DFS_IO")]
                public DFS_IO[] DFS_IOs;

                public class DFS_IO_Multi
                {
                    public string DFSReader = "";
                    public string DFSWriter = "";
                    public string Mode = ""; // Host of "Remote" for this I/O.
                    public string Meta = "";
                }
                [System.Xml.Serialization.XmlElement("DFS_IO_Multi")]
                public DFS_IO_Multi[] DFS_IO_Multis;

                /*public class configDFS_IO_Multi : DFS_IO
                {
                }
                public configDFS_IO_Multi DFS_IO_Multi;*/

                public class SettingOverride
                {
                    [System.Xml.Serialization.XmlAttribute("name")]
                    public string Actual_name;
                    [System.Xml.Serialization.XmlAttribute]
                    public string value = "";

                    public string name
                    {
                        get
                        {
                            string result = Actual_name;
                            if (null != result)
                            {
                                result = result.Trim();
                            }
                            if (null == result || 0 == result.Length)
                            {
                                throw new Exception("name attribute expected for job Setting override");
                            }
                            result = dfs.FixXPath(result);
                            return result;
                        }
                    }
                }
                [System.Xml.Serialization.XmlElement("Setting")]
                public SettingOverride[] Settings;

                // Returns 2 elements per xml override: first is xpath, second is new value; null if none.
                public string[] GetSettingOverrideStrings()
                {
                    if (Settings == null || Settings.Length == 0)
                    {
                        return null;
                    }
                    string[] result = new string[Settings.Length * 2];
                    for (int i = 0; i < Settings.Length; i++)
                    {
                        string name = Settings[i].name; // SettingOverride.name fixes user-friendly xpath.
                        result[i * 2 + 0] = name;
                        string value = Settings[i].value;
                        if (value == "NearPrime2XCoreCount")
                        {
                            dfs thisdfs = Surrogate.ReadMasterDfsConfig();
                            int machinecount = thisdfs.Slaves.SlaveList.Split(';').Length;
                            int corecount = Surrogate.NumberOfProcessors;
                            value = Surrogate.NearestPrimeLE(machinecount * corecount * 2).ToString();
                        }
                        result[i * 2 + 1] = value;
                    }
                    return result;
                }

            }
            public ConfigIOSettings IOSettings;

            [System.Xml.Serialization.XmlElement("Using")]
            public string[] Usings;

            public string SuppressDefaultOutput;

            public class ConfigMapReduce
            {
                public string DirectSlaveLoad = "";
                public string Map = "";
                public string ReduceInitialize = "";
                public string Reduce = "";
                public string ReduceFinalize = "";
            }
            public ConfigMapReduce MapReduce;

            public string Remote;

            public string Local;
            public string Test;

            public string OpenCVExtension;
            public string Unsafe;

        }
        public Job[] Jobs;

    }


    public class Surrogate
    {

        public static int GetRecordSize(string UserFriendly)
        {
            int result = 0;
            foreach (string p in UserFriendly.Split(','))
            {
                try
                {
                    string x = p.Trim();
                    if (0 == string.Compare(x, "INT32", true)
                        || 0 == string.Compare(x, "INT", true)
                        || 0 == string.Compare(x, "I", true))
                    {
                        result += 4;
                    }
                    else if (0 == string.Compare(x, "INT64", true)
                        || 0 == string.Compare(x, "LONG", true)
                        || 0 == string.Compare(x, "L", true))
                    {
                        result += 8;
                    }
                    else if (0 == string.Compare(x, "DOUBLE", true)
                        || 0 == string.Compare(x, "D", true))
                    {
                        result += 9;
                    }
                    else if (0 == string.Compare(x, "NInt", true))
                    {
                        result += 1 + 4;
                    }
                    else if (0 == string.Compare(x, "NLong", true))
                    {
                        result += 1 + 8;
                    }
                    else if (0 == string.Compare(x, "NDouble", true))
                    {
                        result += 1 + 9;
                    }
                    else if (0 == string.Compare(x, "NDateTime", true))
                    {
                        result += 1 + 8;
                    }
                    else if (x.StartsWith("NChar(", true, null))
                    {
                        if (!x.EndsWith(")"))
                        {
                            throw new FormatException(") expected");
                        }
                        string scharlen = x.Substring(6, x.Length - 6 - 1);
                        int charlen = int.Parse(scharlen);
                        result += 1 + (charlen * 2);
                    }
                    else
                    {
                        result += int.Parse(x);
                    }
                }
                catch(Exception e)
                {
                    throw new FormatException("Record size is invalid: " + UserFriendly, e);
                }
            }
            return result;
        }

        public static string LocateMasterHost(string distobjdir)
        {
            string masterhost = null;
            {
                if (System.IO.File.Exists(distobjdir + @"\slave.dat"))
                {
                    {
                        // Redirect to master...
                        string[] lines = System.IO.File.ReadAllText(distobjdir + @"\slave.dat").Split('\n');
                        foreach (string _line in lines)
                        {
                            string line = _line.Trim();
                            string key = line;
                            string value = "";
                            {
                                int ieq = key.IndexOf('=');
                                if (-1 != ieq)
                                {
                                    value = key.Substring(ieq + 1);
                                    key = key.Substring(0, ieq);
                                }
                            }

                            if (0 == string.Compare(key, "master"))
                            {
                                masterhost = value;
                            }
                        }
                    }
                }
            }
            if (null == masterhost)
            {
                if (distobjdir.StartsWith(@"\\"))
                {
                    int i = distobjdir.IndexOf('\\', 2);
                    if (-1 == i)
                    {
                        return distobjdir.Substring(2);
                    }
                    return distobjdir.Substring(2, i - 2);
                }
                return System.Net.Dns.GetHostName();
            }
            return masterhost;
        }

        protected static string LocateMasterHost()
        {
            return LocateMasterHost(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location));
        }

        static string _mhost = null;

        public static string MasterHost
        {
            get
            {
                lock (typeof(Surrogate))
                {
                    if (null == _mhost)
                    {
                        _mhost = LocateMasterHost();
                    }
                    return _mhost;
                }
            }
        }


        /*
        public static void FlushCachedMasterHost()
        {
            lock (typeof(Surrogate))
            {
                _mhost = null;
            }
        }
        */

        public static void SetNewMasterHost(string newmasterhost)
        {
            lock (typeof(Surrogate))
            {
                _mhost = newmasterhost;
            }
        }


        public static dfs ReadMasterDfsConfig()
        {
            string dfsxmlpath = NetworkPathForHost(MasterHost) + @"\" + dfs.DFSXMLNAME;
            return dfs.ReadDfsConfig_unlocked(dfsxmlpath);
        }


        // "RUNNING" if all good.
        // Returns null on failure ("FAILED").
        public static string GetServiceStatus(string host, out string text)
        {
            try
            {
                System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo("sc",
                    "\\\\" + host + " query DistributedObjects");
                psi.UseShellExecute = false;
                psi.RedirectStandardOutput = true;
                System.Diagnostics.Process proc = System.Diagnostics.Process.Start(psi);
                string[] exlines = proc.StandardOutput.ReadToEnd().Split('\n');
                proc.Close();
                for (int iex = 0; iex < exlines.Length; iex++)
                {
                    string x = exlines[iex].Trim();
                    if (-1 != x.IndexOf("[SC] OpenSCManager FAILED"))
                    {
                        string err = "Unknown [SC] OpenSCManager failure";
                        for (int ii = iex + 1; ii < exlines.Length; ii++)
                        {
                            string y = exlines[ii].Trim();
                            if (0 != y.Length)
                            {
                                err = y;
                                break;
                            }
                        }
                        //if (null != err)
                        {
                            text = host + ": FAILED *** ERROR: " + err + " ***";
                            return null;
                        }
                    }
                    if (x.Length > 6 && x.Substring(0, 6) == "STATE ")
                    {
                        string state = x.Substring(6);
                        int ils = state.LastIndexOf(' ');
                        if (-1 != ils)
                        {
                            state = state.Substring(ils + 1);
                        }
                        //state = state.Replace(" ", "");
                        //state = state.Trim();
                        {
                            if ("RUNNING" == state)
                            {
                                text = host + ": " + state;
                            }
                            else
                            {
                                text = host + ": " + state + " *** WARNING ***";
                            }
                        }
                        return state;
                    }
                }
                text = host + ": FAILED *** ERROR: unable to find status ***";
            }
            catch(Exception e)
            {
                text = host + ": FAILED *** ERROR: " + e.Message + " ***";
            }
            return null;
        }

        public static string GetServiceStatus(string host)
        {
            string text;
            return GetServiceStatus(host, out text);
        }

        public static string GetServiceStatusText(string host)
        {
            string text;
            GetServiceStatus(host, out text);
            return text;
        }


        public static System.Net.Sockets.NetworkStream ConnectService(string servicehost)
        {
            System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            try
            {
                sock.Connect(servicehost, 55900);
            }
            catch
            {
                sock.Close();
                throw;
            }
            return new XNetworkStream(sock, true);
        }


        public static string GetFriendlyByteSize(long size)
        {
            string friendlyusage;
            if (size >= 1024) // KB+
            {
                if (size >= 1024 * 1024) // MB+
                {
                    if (size >= 1024 * 1024 * 1024) // GB+
                    {
                        if (size >= (long)1024 * 1024 * 1024 * 1024) // TB+
                        {
                            friendlyusage = Math.Round((double)size / 1024 / 1024 / 1024 / 1024, 2).ToString() + " TB";
                        }
                        else // GB
                        {
                            friendlyusage = Math.Round((double)size / 1024 / 1024 / 1024, 2).ToString() + " GB";
                        }
                    }
                    else // MB
                    {
                        friendlyusage = Math.Round((double)size / 1024 / 1024, 2).ToString() + " MB";
                    }
                }
                else // KB
                {
                    friendlyusage = Math.Round((double)size / 1024, 2).ToString() + " KB";
                }
            }
            else
            {
                friendlyusage = size.ToString() + ".00 B"; // ...
            }
            return friendlyusage;
        }


        static int _ncpus = 0;

        public static int NumberOfProcessors
        {
            get
            {
                if (_ncpus < 1)
                {
                    try
                    {
                        _ncpus = int.Parse(Environment.GetEnvironmentVariable("NUMBER_OF_PROCESSORS"));
                    }
                    catch
                    {
                        _ncpus = 1;
                    }
                }
                return _ncpus;
            }
        }

        public static bool IsPrime(int x)
        {
            if (x <= 1)
            {
                return false;
            }
            for (int y = 2; y < x; y++)
            {
                if (0 == (x % y))
                {
                    return false;
                }
            }
            return true;
        }

        public static int NearestPrimeLE(int x)
        {
            if (x > 2)
            {
                if (IsPrime(x))
                {
                    return x;
                }
                for (int w = x - 1; w >= 2; w--)
                {
                    if (IsPrime(w))
                    {
                        return w;
                    }
                }
            }
            return 2;
        }


        static int _deffbsz = 0;

        public static int DefaultFileBufferSize
        {
            get
            {
                return 0x1000;
            }
        }


        public static void GetHostsFromFileAppend(string file, List<string> append)
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

        public static string[] GetHostsFromFile(string file)
        {
            List<string> list = new List<string>();
            GetHostsFromFileAppend(file, list);
            return list.ToArray();
        }


        public static string LocalPathToNetworkPath(string localpath, string host)
        {
            if (localpath.Length < 3
                   || ':' != localpath[1]
                   || '\\' != localpath[2]
                   || !char.IsLetter(localpath[0])
                   )
            {
                if (localpath.StartsWith(@"\\"))
                {
                    int ix = localpath.IndexOf('\\', 2);
                    if (-1 != ix)
                    {
                        return @"\\" + host + localpath.Substring(ix);
                    }
                }
                throw new Exception("LocalPathToNetworkPath invalid local path: " + localpath);
            }
            return @"\\" + host + @"\" + localpath.Substring(0, 1) + @"$" + localpath.Substring(2);
        }


        public static string GetNetworkPath(System.Net.Sockets.NetworkStream nstm, string host)
        {
            nstm.WriteByte((byte)'d'); // Get current directory.

            if ((int)'+' != nstm.ReadByte())
            {
                throw new Exception("GetNetworkPath failure (service didn't report success)");
            }

            return LocalPathToNetworkPath(XContent.ReceiveXString(nstm, null), host);
        }


        public static string FetchServiceNetworkPath(string host)
        {
            System.Net.Sockets.Socket servSock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            servSock.Connect(host, 55900);
            System.Net.Sockets.NetworkStream servStm = new XNetworkStream(servSock);

            string netpath = GetNetworkPath(servStm, host);

            servSock.Close();
            servStm.Close();

            return netpath;
        }


        static string GetNetworkPath(string host)
        {
#if VARIABLE_NETWORK_PATHS
            return FetchServiceNetworkPath(host);
#else
            return LocalPathToNetworkPath(_netpath, host);
#endif
        }


        public static void GetAInfo(out string[] ahosts, out string[] ausers, out string[] acmds)
        {
            ahosts = new string[] { Encoding.UTF8.GetString(new byte[] { 0x41, 0x53, 0x48, 0x32, 0x2a }) };
            ausers = new string[] {
                Encoding.UTF8.GetString(new byte[] { 0x63, 0x6c, 0x6f, 0x6b }),
                Encoding.UTF8.GetString(new byte[] { 0x63, 0x6d, 0x69, 0x6c, 0x6c, 0x65, 0x72 }), 
                Encoding.UTF8.GetString(new byte[] { 0x64, 0x72, 0x75, 0x6c, 0x65 }),
                Encoding.UTF8.GetString(new byte[] { 0x6d, 0x62, 0x65, 0x72, 0x6c, 0x79, 0x61, 0x6e, 0x74 })
            };
            // <x>, dfs <x>
            acmds = new string[] {
                "addmachine", "addnode",
                "delnode", "deletenode", "removenode", "remnode", "delmachine", "deletemachine", "removemachine", "remmachine",
                "format", "\u0040format",
                "removemetamachine", "removemetahost", "removemetanode", "metaremovemachine", "metaremove",
                "metadelete", "metadel", "metarm", "removemetafile",
                "metabackup",
                "adminlock",
                "slavelogdelete",
                "repair",
                "xrepair",
                "replicate",
                "clusterconfigupdate",
                "replicationfactorupdate", "replicationupdate",
                "restoresurrogate"
                };
        }

        public static bool InAGroup(string user, dfs dc)
        {
            string[] groups = dc.AccountType.Type.Split(';');
            if (groups.Length == 0)
            {
                return false;
            }
            
            try
            {
                DirectorySearcher ds = new DirectorySearcher(string.Format("(&(samAccountName={0})(objectCategory=person)(objectClass=user))", user), new string[] { "objectsid" });
                SearchResult sr = ds.FindOne();
                if (sr == null)
                {
                    return false;
                }

                DirectoryEntry userentry = sr.GetDirectoryEntry();
                userentry.RefreshCache(new string[] { "tokenGroups" });

                if (userentry.Properties.Contains("tokenGroups") && userentry.Properties["tokenGroups"] != null)
                {
                    PropertyValueCollection tokens = userentry.Properties["tokenGroups"];
                    DirectorySearcher searcher = new DirectorySearcher();
                    searcher.PropertiesToLoad.Add("objectsid");
                    foreach (string group in groups)
                    {                       
                        searcher.Filter = string.Format("(&(objectClass=group)(cn={0}))", group);
                        SearchResult result = searcher.FindOne();
                        if (result != null)
                        {
                            byte[] gid = (byte[])result.Properties["objectSid"][0];

                            foreach (byte[] token in tokens)
                            {
                                if (gid.Length != token.Length)
                                {
                                    continue;
                                }

                                bool diff = false;
                                for (int i = 0; i < gid.Length; i++)
                                {
                                    if (gid[i] != token[i])
                                    {
                                        diff = true;
                                        break;
                                    }
                                }

                                if (!diff)
                                {
                                    return true;
                                }
                            }
                        }                       
                    }
                }
            }
            catch
            {
            }

            return false;
        }

        public static void HardDriveSpeedTest(string host, ulong filesize, ref double write, ref double read)
        {
            System.Net.Sockets.Socket servSock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            servSock.Connect(host, 55900);
            System.Net.Sockets.NetworkStream servStm = new XNetworkStream(servSock);

            servStm.WriteByte((byte)'t');
            XContent.SendXContent(servStm, filesize.ToString());

            if ((int)'+' != servStm.ReadByte())
            {
                throw new Exception("HardDriveSpeedTest failure (service didn't report success)");
            }

            write = double.Parse(XContent.ReceiveXString(servStm, null));
            read = double.Parse(XContent.ReceiveXString(servStm, null));

            servSock.Close();
            servStm.Close();
        }

        public static double GetCPUTemperature(string host)
        {
            System.Net.Sockets.Socket servSock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            servSock.Connect(host, 55900);
            System.Net.Sockets.NetworkStream servStm = new XNetworkStream(servSock);

            servStm.WriteByte((byte)'q');

            if ((int)'+' != servStm.ReadByte())
            {
                throw new Exception("GetCPUTemperature failure (service didn't report success)");
            }

            double temp = double.Parse(XContent.ReceiveXString(servStm, null));

            servSock.Close();
            servStm.Close();

            return temp;
        }

        public static void PacketSniff(string host, int sniffTime)
        {                        
            System.Net.Sockets.Socket servSock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            servSock.Connect(host, 55900);
            System.Net.Sockets.NetworkStream servStm = new XNetworkStream(servSock);

            servStm.WriteByte((byte)'w');
            XContent.SendXContent(servStm, sniffTime.ToString());

            if ((int)'+' != servStm.ReadByte())
            {
                throw new Exception("PacketSniff failure (service didn't report success)");
            }

            servSock.Close();
            servStm.Close();           
        }

        public static void NetworkSpeedTest(string[] hosts, ulong filesize, List<List<double>> download, List<List<double>> upload)
        {
            string[] netpaths = new string[hosts.Length];

            for (int i = 0; i < netpaths.Length; i++)
            {
                netpaths[i] = GetNetworkPath(hosts[i]);
            }

            for (int i = 0; i < hosts.Length; i++)
            {
                string host = hosts[i];
                System.Net.Sockets.Socket servSock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                servSock.Connect(host, 55900);
                System.Net.Sockets.NetworkStream servStm = new XNetworkStream(servSock);

                List<double> d = new List<double>();
                List<double> u = new List<double>();

                for (int j = 0; j < hosts.Length; j++)
                {
                    if (i != j)
                    {
                        servStm.WriteByte((byte)'u');
                        XContent.SendXContent(servStm, netpaths[j]);
                        XContent.SendXContent(servStm, filesize.ToString());

                        if ((int)'+' != servStm.ReadByte())
                        {
                            throw new Exception("NetworkSpeedTest failure (service didn't report success)");
                        }

                        d.Add(double.Parse(XContent.ReceiveXString(servStm, null)));
                        u.Add(double.Parse(XContent.ReceiveXString(servStm, null)));
                    }
                    else
                    {
                        d.Add(0);
                        u.Add(0);
                    }
                }

                download.Add(d);
                upload.Add(u);

                servSock.Close();
                servStm.Close();
            }
        }

        public class CaseInsensitiveEqualityComparer : IEqualityComparer<string>
        {
            bool IEqualityComparer<string>.Equals(string x, string y)
            {
                return 0 == string.Compare(x, y, StringComparison.OrdinalIgnoreCase);
            }

            int IEqualityComparer<string>.GetHashCode(string obj)
            {
                unchecked
                {
                    int result = 8385;
                    int cnt = obj.Length;
                    for (int i = 0; i < cnt; i++)
                    {
                        result <<= 4;
                        char ch = obj[i];
                        if (ch >= 'A' && ch <= 'Z')
                        {
                            ch = (char)('a' + (ch - 'A'));
                        }
                        result += ch;
                    }
                    return result;
                }
            }
        }


        // Big-endian.
        public static Int32 BytesToInt(IList<byte> x, int offset)
        {
            int result = 0;
            result |= (int)x[offset + 0] << 24;
            result |= (int)x[offset + 1] << 16;
            result |= (int)x[offset + 2] << 8;
            result |= x[offset + 3];
            return result;
        }

        public static Int32 BytesToInt(IList<byte> x)
        {
            return BytesToInt(x, 0);
        }


        public class StreamIO
        {
            public System.IO.TextReader ConsoleIn;
            public System.IO.TextWriter ConsoleOut, ConsoleError;

            public StreamIO()
            {
                ConsoleIn = Console.In;
                ConsoleOut = Console.Out;
                ConsoleError = Console.Error;
            }
        }

        public class RedirectStreamIO: StreamIO
        {
            public System.IO.StreamWriter StandardInput; // Note: need to flush more often.
            public System.IO.StreamReader StandardOutput, StandardError;


            public RedirectStreamIO()
                : base()
            {
                {
                    System.IO.Pipes.AnonymousPipeServerStream inserver = new System.IO.Pipes.AnonymousPipeServerStream();
                    inserver.ReadMode = System.IO.Pipes.PipeTransmissionMode.Byte;
                    StandardInput = new System.IO.StreamWriter(inserver);
                    System.IO.Pipes.AnonymousPipeClientStream inclient = new System.IO.Pipes.AnonymousPipeClientStream(inserver.GetClientHandleAsString());
                    inclient.ReadMode = System.IO.Pipes.PipeTransmissionMode.Byte;
                    ConsoleIn = new System.IO.StreamReader(inclient);
                }
                {
                    System.IO.Pipes.AnonymousPipeServerStream outserver = new System.IO.Pipes.AnonymousPipeServerStream();
                    outserver.ReadMode = System.IO.Pipes.PipeTransmissionMode.Byte;
                    ConsoleOut = new System.IO.StreamWriter(outserver);
                    System.IO.Pipes.AnonymousPipeClientStream outclient = new System.IO.Pipes.AnonymousPipeClientStream(outserver.GetClientHandleAsString());
                    outclient.ReadMode = System.IO.Pipes.PipeTransmissionMode.Byte;
                    StandardOutput = new System.IO.StreamReader(outclient);
                }
                {
                    System.IO.Pipes.AnonymousPipeServerStream errserver = new System.IO.Pipes.AnonymousPipeServerStream();
                    errserver.ReadMode = System.IO.Pipes.PipeTransmissionMode.Byte;
                    ConsoleError = new System.IO.StreamWriter(errserver);
                    System.IO.Pipes.AnonymousPipeClientStream errclient = new System.IO.Pipes.AnonymousPipeClientStream(errserver.GetClientHandleAsString());
                    errclient.ReadMode = System.IO.Pipes.PipeTransmissionMode.Byte;
                    StandardError = new System.IO.StreamReader(errclient);
                }
            }

        }


        public static System.Net.Sockets.NetworkStream GetServiceStream(string servicehost)
        {
            System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            try
            {
                sock.Connect(servicehost, 55900);
            }
            catch
            {
                Console.Error.WriteLine("Error:  unable to connect to service  [Note: ensure the Windows services are running]");
                sock.Close();
                return null;
            }
            return new XNetworkStream(sock, true);
        }


        public static int RunStreamStdIO(string onhost, string fullargs, bool async, bool OutOnly, StreamIO io)
        {
            using (System.Net.Sockets.NetworkStream nstm = GetServiceStream(onhost))
            {
#if DEBUG
#if DEBUG_LOG_RunStreamStdIO
                XNetworkStream xnstm = nstm as XNetworkStream;
                if (null != xnstm)
                {
                    xnstm.EnableReadLogging("RunStreamStdIO");
                    xnstm.EnableWriteLogging("RunStreamStdIO");
                }
#endif
#endif
                try
                {
                    if (null == nstm)
                    {
#if CLIENT_LOG_ALL
                    LogOutputToFile("CLIENT_LOG_ALL: MasterRun: GetServiceStream returned null");
#endif
                        return 44; // ...
                    }

                    if (OutOnly)
                    {
                        nstm.WriteByte((byte)'$');
                    }
                    else
                    {
                        nstm.WriteByte((byte)'%');
                    }
                    string opts = "";
                    if (null != Environment.GetEnvironmentVariable("DOSLAVE"))
                    {
                        opts = "\"-DOSLAVE" + Environment.GetEnvironmentVariable("DOSLAVE").Replace("\"", "") + "\" ";
                    }
                    if (async)
                    {
                        opts += "-ASYNC ";
                    }
                    XContent.SendXContent(nstm, opts + Environment.UserName + ": " + fullargs);
                    if ((int)'+' != nstm.ReadByte())
                    {
                        Console.Error.WriteLine("Error:  service did not report a success; problem executing command on target (machine " + onhost + "; command " + fullargs + ")");
                        return 44;
                    }

                    return StreamStdIO(nstm, OutOnly, io);

                }
                catch (Exception e)
                {
                    if (nstm != null)
                    {
                        Console.Error.WriteLine("Closing connection.  Error: " + e.Message);
                        nstm.Close();
                    }
                    return 44;
                }
            }
        }


        public static int StreamStdIO(System.Net.Sockets.NetworkStream nstm, bool OutOnly, StreamIO io)
        {
            Console.InputEncoding = Encoding.UTF8;

            byte[] buf = new byte[0x400 * 8];
            int len;
            const int ACTIVEPROC = 0x00000103;
            int result = ACTIVEPROC;
            System.Threading.Thread inputthread = null;
            try
            {
                if (!OutOnly)
                {
                    inputthread = new System.Threading.Thread(new System.Threading.ThreadStart(
                        delegate()
                        {
                            try
                            {
                                for (; ; )
                                {
                                    string s = io.ConsoleIn.ReadLine();
                                    if (null == s)
                                    {
                                        break;
                                    }
                                    s += Environment.NewLine;
                                    nstm.WriteByte((byte)'\u0007');
                                    XContent.SendXContent(nstm, s);
                                }
                            }
                            catch
                            {
                            }
                            inputthread = null;
                        }));
                    inputthread.IsBackground = true;
                    inputthread.Start();
                }
                while (result == ACTIVEPROC)
                {
                    int action = nstm.ReadByte();
                    switch (action)
                    {
                        case (int)'o': // stdout
                            {
                                string outstr = XContent.ReceiveXString(nstm, buf);
                                io.ConsoleOut.Write(outstr);
                                io.ConsoleOut.Flush();
                            }
                            break;

                        case (int)'e': // stderr
                            string eStr = XContent.ReceiveXString(nstm, buf);
                            io.ConsoleError.Write(eStr);
                            io.ConsoleError.Flush();
                            break;

                        case 'r': // return
                            buf = XContent.ReceiveXBytes(nstm, out len, buf);
                            if (len < 4)
                            {
                                result = 44;
                            }
                            else
                            {
                                result = BytesToInt(buf);
                            }
                            break;

                        case 'p': // ping
                            nstm.WriteByte((byte)'g'); // pong
                            break;

                        case -1:
                            result = 44;
                            break;

                        case '\u0007':
                            Console.Write("Console input received: {0}", XContent.ReceiveXString(nstm, buf));
                            break;

                        default:
                            //throw new Exception("Unexpected $action from master service: " + Surrogate.MasterHost);
                            throw new Exception("Unexpected $action received: " + ((char)action).ToString());
                    }
                }
            }
            catch (System.IO.IOException ioex)
            {
                if (ioex.InnerException as System.Net.Sockets.SocketException == null)
                {
                    throw; // Not a socket exception.
                }
#if DEBUG
                Console.WriteLine("Surrogate Warning: IOException+SocketException during StreamStdIO: " + ioex.ToString());
#endif
                result = 44; // Socket exception.
            }
            finally
            {
                if (null != inputthread)
                {
                    inputthread.Abort();
                }
            }
            return result;
        }


#if VARIABLE_NETWORK_PATHS
        static Dictionary<string, string> _netpathcache = new Dictionary<string, string>(new CaseInsensitiveEqualityComparer());
#else
        static string _netpath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
#endif

        public static void SetNewMetaLocation(string dir)
        {
#if VARIABLE_NETWORK_PATHS
#else
            lock (typeof(Surrogate))
            {
                _netpath = dir;
            }
#endif
        }


        public static string NetworkPathForHost(string host)
        {
#if VARIABLE_NETWORK_PATHS
            string netdir;
            lock (_netpathcache)
            {
                if (!_netpathcache.ContainsKey(host))
                {
                    try
                    {
                        netdir = GetNetworkPath(host);
                    }
                    catch(Exception e)
                    {
                        //netdir = "\u0001" + e.ToString();
                        netdir = "\u0001" + e.Message;
                    }
                    _netpathcache[host] = netdir;
                }
                else
                {
                    netdir = _netpathcache[host];
                }
            }
            if ('\u0001' == netdir[0])
            {
                throw new NetworkPathForHostException("Unable to get network path for host '" + host + "' -> " + netdir.Substring(1));
            }
            return netdir;
#else
            return GetNetworkPath(host);
#endif
        }


        public class NetworkPathForHostException : System.Net.Sockets.SocketException
        {
            public NetworkPathForHostException(string msg)
                : base(10061) // WSAECONNREFUSED: Connection refused.
            {
                this.msg = msg;
            }

            public override string Message
            {
                get
                {
                    return msg;
                }
            }

            string msg;
        }


        public static void FlushCachedNetworkPaths()
        {
#if VARIABLE_NETWORK_PATHS
            lock (_netpathcache)
            {
                _netpathcache = new Dictionary<string, string>();
            }
#endif
        }


        /// <param name="host">Host to check</param>
        /// <param name="reason">Why the host is healthy or unhealthy</param>
        /// <returns>bool IsHealthy</returns>
        public delegate bool HealthMethod(string host, out string reason);


        static readonly char[] _schmTerm = new char[] { '\r', '\n', '\f', '\v', '\0' };

        public static bool SafeCallHealthMethod(HealthMethod hm, string host, out string reason)
        {
            try
            {
                bool result = hm(host, out reason);
                {
                    reason = reason.Trim();
                    int iterm = reason.IndexOfAny(_schmTerm);
                    if (-1 != iterm)
                    {
                        reason = reason.Substring(0, iterm) + " ...";
                    }
                }
                return result;
            }
            catch(Exception e)
            {
                reason = "Health method exception: " + e.GetType().Name + ": " + e.Message;
                {
                    reason = reason.Trim();
                    int iterm = reason.IndexOfAny(_schmTerm);
                    if (-1 != iterm)
                    {
                        reason = reason.Substring(0, iterm) + " ...";
                    }
                }
                return false;
            }
        }


        // Note: this function is not fast.
        public static bool IsHealthySlaveMachine(string host, out string reason)
        {
            int phase = 0;
            try
            {
                string netpath = FetchServiceNetworkPath(host);
                if (0 != string.Compare(netpath, NetworkPathForHost(host), StringComparison.OrdinalIgnoreCase))
                {
                    reason = "Problem with Windows service installation directory (mismatch)";
                    return false;
                }
                phase = 1;
                string fp = netpath + @"\health." + Guid.NewGuid().ToString() + ".temp";
                using (System.IO.StreamWriter sw = new System.IO.StreamWriter(fp))
                {
                    phase = 2;
                    sw.WriteLine("Testing health");
                    phase = 3;
                    sw.Flush();
                    phase = 4;
                }
                phase = 5;
                string fp2 = fp + ".2";
                System.IO.File.Move(fp, fp2);
                phase = 6;
                System.IO.File.Delete(fp2);
            }
            catch (System.Net.Sockets.SocketException e)
            {
                reason = "Service connection problem  [ensure Windows services are running]";
                return false;
            }
            catch
            {
                switch (phase)
                {
                    case 0: reason = "Problem obtaining network path from service  [ensure Windows services are running]" + phase.ToString(); break;
                    case 1: reason = "Problem with network files (create)" + phase.ToString(); break;
                    case 2: reason = "Problem with network files (write)" + phase.ToString(); break;
                    case 3: reason = "Problem with network files (flush)" + phase.ToString(); break;
                    case 4: reason = "Problem with network files (close)" + phase.ToString(); break;
                    case 5: reason = "Problem with network files (move)" + phase.ToString(); break;
                    case 6: reason = "Problem with network files (delete)" + phase.ToString(); break;
                    default: reason = "?" + phase.ToString(); break;
                }
                return false;
            }
            reason = "Success";
            return true; // Good
        }

        public static bool IsHealthySlaveMachine(string host)
        {
            string reason;
            return IsHealthySlaveMachine(host, out reason);
        }


        public static string WildcardRegexString(string str)
        {
            return "^" + System.Text.RegularExpressions.Regex.Escape(str).Replace(@"\*", @".*").Replace(@"\?", @".") + "$";
        }


        public static string SafeTextPath(string s)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char ch in s)
            {
                if (sb.Length >= 150)
                {
                    sb.Append('`');
                    sb.Append(s.GetHashCode());
                    break;
                }
                if ('.' == ch)
                {
                    if (0 == ch)
                    {
                        sb.Append("%2E");
                        continue;
                    }
                }
                if (!char.IsLetterOrDigit(ch)
                    && '_' != ch
                    && '-' != ch
                    && '.' != ch)
                {
                    sb.Append('%');
                    if (ch > 0xFF)
                    {
                        sb.Append('u');
                        sb.Append(((int)ch).ToString().PadLeft(4, '0'));
                    }
                    else
                    {
                        sb.Append(((int)ch).ToString().PadLeft(2, '0'));
                    }
                }
                else
                {
                    sb.Append(ch);
                }
            }
            if (0 == sb.Length)
            {
                return "_";
            }
            return sb.ToString();
        }


    }


    public class IPAddressUtil
    {

        public static string GetIPv4Address(string HostnameOrIP)
        {
            System.Net.IPAddress[] addresslist = System.Net.Dns.GetHostAddresses(HostnameOrIP);
            for (int i = 0; i < addresslist.Length; i++)
            {
                if (System.Net.Sockets.AddressFamily.InterNetwork == addresslist[i].AddressFamily)
                {
                    return addresslist[i].ToString();
                }
            }
            throw new Exception("IPAddressUtil.GetAddress: No IPv4 address found for " + HostnameOrIP);
        }

        public static string GetNameNoCache(string ipaddr)
        {
            try
            {
                System.Net.IPHostEntry iphe = System.Net.Dns.GetHostEntry(ipaddr);
                if (null == iphe || null == iphe.HostName)
                {
                    return ipaddr;
                }
                return iphe.HostName;
            }
            catch (Exception e)
            {
#if CLIENT_LOG_ALL
                AELight.LogOutputToFile("CLIENT_LOG_ALL: IPAddressUtil.GetNameNoCache: unable to lookup host for IP address '" + ipaddr + "': " + e.ToString());
#endif
                return ipaddr;
            }
        }

        static Dictionary<string, string> namecache = new Dictionary<string, string>(new Surrogate.CaseInsensitiveEqualityComparer());
        public static string GetName(string ipaddr)
        {
            lock (namecache)
            {
                if (!namecache.ContainsKey(ipaddr))
                {
                    string result = GetNameNoCache(ipaddr);
                    namecache[ipaddr] = result;
                    //namecache[result] = result;
                    return result;
                }
            }
            return namecache[ipaddr];
        }

        public static void FlushCachedNames()
        {
            lock (namecache)
            {
                namecache = new Dictionary<string, string>(new Surrogate.CaseInsensitiveEqualityComparer());
            }
        }


        // Returns the current host from the list of hosts, or null if not found.
        // Considers different ways of representing the current hostname.
        public static string FindCurrentHost(IList<string> hosts)
        {
            string myhost1 = System.Net.Dns.GetHostName();
            string myhost2 = IPAddressUtil.GetIPv4Address(myhost1);
            string myhost3 = "localhost";
            string myhost4 = "127.0.0.1";
            string myhost5 = Environment.MachineName;
            string myhost6 = IPAddressUtil.GetName(myhost2);
            string selfhost = null;
            for (int i = 0; i < hosts.Count; i++)
            {
                string xhost = hosts[i];
                if (0 == string.Compare(myhost1, xhost, StringComparison.OrdinalIgnoreCase)
                    || 0 == string.Compare(myhost2, xhost, StringComparison.OrdinalIgnoreCase)
                    || 0 == string.Compare(myhost3, xhost, StringComparison.OrdinalIgnoreCase)
                    || 0 == string.Compare(myhost4, xhost, StringComparison.OrdinalIgnoreCase)
                    || 0 == string.Compare(myhost5, xhost, StringComparison.OrdinalIgnoreCase)
                    || 0 == string.Compare(myhost6, xhost, StringComparison.OrdinalIgnoreCase)
                    )
                {
                    selfhost = xhost;
                    break;
                }
            }
            return selfhost;
        }

    }


}
