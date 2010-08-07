using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySpace.DataMining.DistributedObjects;


namespace RDBMS_DBCORE
{
    public partial class Qa
    {

        public abstract class Job
        {
            public string Exec(params string[] args)
            {
                Init(args);
                BuffersContext rb = StartBuffers();
                Run();
                ResetBuffers(rb);
                return ReadToEnd();
            }

            public virtual void Init(params string[] args)
            {
#if DEBUG
                bool foundempty = false;
                foreach (string darg in args)
                {
                    if (0 == darg.Length)
                    {
                        foundempty = true;
                    }
                    else
                    {
                        if (foundempty)
                        {
                            System.Diagnostics.Debugger.Launch();
                            throw new Exception("DEBUG:  " + GetType().FullName + ".Exec() has an empty argument");
                        }
                    }
                }
#endif
                DSpace_ExecArgs = args;
            }

            protected abstract void Run();

            protected string[] DSpace_ExecArgs;

            protected void DSpace_Log(string line)
            {
                _log.AppendLine(line);
            }

            StringBuilder _log = new StringBuilder();

            public string ReadToEnd()
            {
                string result = _log.ToString();
                _log.Length = 0;
                return result;
            }


            protected internal struct InputDataInfo
            {
                public int StartIndex;
                public string FileName;
                public int RecordLength;
            }

            protected internal IEnumerator<ByteSlice> GetInputData(IList<string> InputFiles, IList<InputDataInfo> AppendInputInfo)
            {
                List<ByteSlice> inputlist = new List<ByteSlice>();
                for (int iif = 0; iif < InputFiles.Count; iif++)
                {
                    int reclen;
                    string dfsfile = InputFiles[iif];
                    if (dfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        dfsfile = dfsfile.Substring(6);
                    }
                    {
                        int iat = dfsfile.IndexOf('@');
                        if (-1 == iat)
                        {
                            reclen = -1;
                        }
                        else
                        {
                            if (!int.TryParse(dfsfile.Substring(iat + 1), out reclen) || reclen < 1)
                            {
                                throw new Exception("Invalid record length: dfs://" + dfsfile);
                            }
                            dfsfile = dfsfile.Substring(0, iat);
                        }
                    }
                    {
                        InputDataInfo idi;
                        idi.FileName = dfsfile;
                        idi.RecordLength = reclen;
                        idi.StartIndex = inputlist.Count;
                        AppendInputInfo.Add(idi);
                    }
                    byte[] content = dfsclient.GetFileContent(dfsfile);
                    if (-1 == reclen)
                    {
                        int istart = 0;
                        for (int i = 0; ; i++)
                        {
                            if (i == content.Length)
                            {
                                if (istart != content.Length)
                                {
                                    inputlist.Add(ByteSlice.Prepare(content, istart, i - istart));
                                }
                                break;
                            }
                            if ('\r' == content[i] || '\r' == content[i])
                            {
                                if (i != istart)
                                {
                                    inputlist.Add(ByteSlice.Prepare(content, istart, i - istart));
                                }
                                if (i + 1 < content.Length && '\n' == content[i + 1])
                                {
                                    i++;
                                }
                                istart = i + 1;
                            }
                        }
                    }
                    else
                    {
                        if (0 != (content.Length % reclen))
                        {
                            throw new Exception("Invalid file size of " + content.Length + " of DFS file '" + dfsfile + "' for record length of " + reclen);
                        }
                        for (int ir = 0; ir < content.Length; ir += reclen)
                        {
                            inputlist.Add(ByteSlice.Prepare(content, ir, reclen));
                        }
                    }
                }
                return inputlist.GetEnumerator();
            }

            protected internal IEnumerator<ByteSlice> GetInputData(IList<string> InputFiles, out int RecordLength)
            {
                RecordLength = -1;
                List<InputDataInfo> AppendInputInfo = new List<InputDataInfo>();
                IEnumerator<ByteSlice> result = GetInputData(InputFiles, AppendInputInfo);
                if (AppendInputInfo.Count > 0)
                {
                    RecordLength = AppendInputInfo[0].RecordLength;
                }
                return result;
            }

            protected internal IEnumerator<ByteSlice> GetInputData(IList<string> InputFiles)
            {
                int RecordLength;
                return GetInputData(InputFiles, out RecordLength);
            }


            protected struct BuffersContext
            {
                internal int stack;
                internal int recordset;
                public object other;
            }

            protected virtual BuffersContext StartBuffers()
            {
                BuffersContext rb;
                rb.stack = Stack.StartStack();
                rb.recordset = recordset.StartBuffers();
                rb.other = null;
                return rb;
            }

            protected virtual void ResetBuffers(BuffersContext rb)
            {
                Stack.ResetStack(rb.stack);
                recordset.ResetBuffers(rb.recordset);
            }

        }


        public abstract class JobCall
        {
            public readonly string Name;

            public JobCall(string name)
            {
                this.Name = name;
            }

            public abstract string Call(string args);

            public string Call()
            {
                return Call("");
            }


            protected static string[] ParseArgs(string args)
            {
                List<string> result = new List<string>();
                args = args.TrimEnd();
                for (; ; )
                {
                    args = args.TrimStart();
                    if (args.Length < 1)
                    {
                        break;
                    }
                    if (args[0] == '"')
                    {
                        for (int i = 1; ; i++)
                        {
                            if (i >= args.Length)
                            {
                                result.Add(args.Substring(1));
                                args = "";
                                break;
                            }
                            if (args[i] == '\\')
                            {
                                i++;
                                continue;
                            }
                            if (args[i] == '"')
                            {
                                result.Add(args.Substring(1, i - 1));
                                args = args.Substring(i + 1);
                                break;
                            }
                        }

                    }
                    else
                    {
                        int isp = args.IndexOf(' ');
                        if (-1 == isp)
                        {
                            result.Add(args);
                            args = "";
                            break;
                        }
                        else
                        {
                            result.Add(args.Substring(0, isp));
                            args = args.Substring(isp + 1);
                        }
                    }
                }
                return result.ToArray();
            }

        }


        // protected override void Run()
        public abstract class Local : Job
        {
        }


        public abstract class LocalCall : JobCall
        {
            public LocalCall(string name)
                : base(name)
            {
            }
        }

        public class LocalCallShellExec : RemoteCall
        {
            public readonly string JobFile;

            public LocalCallShellExec(string name, string jobfile)
                : base(name)
            {
                this.JobFile = jobfile;
            }

            public override string Call(string args)
            {
                StringBuilder sbxpath = new StringBuilder();
                return Shell("Qizmt exec " + sbxpath + "\"" + this.JobFile + "\" " + args);
            }

        }

        public class LocalCallInProc : RemoteCall
        {
            Local local;

            public LocalCallInProc(string name, Local remote)
                : base(name)
            {
                this.local = remote;
            }

            public override string Call(string args)
            {
                return local.Exec(ParseArgs(args));
            }

        }


        public class Remote : Job
        {

            public int DSpace_InputRecordLength = -1;
            public int DSpace_OutputRecordLength = -1;
            public int DSpace_ProcessID = 0;

            public List<string> InputFiles = new List<string>();
            public string OutputFile;

            // input.MoveNext() to go through all input, one record or line at a time.
            public virtual void OnRemote(IEnumerator<ByteSlice> input, System.IO.Stream output)
            {
            }


            protected override void Run()
            {
                IEnumerator<ByteSlice> input = GetInputData(InputFiles, out DSpace_InputRecordLength);
                System.IO.MemoryStream output = new System.IO.MemoryStream();
                string outputdfsfile = null;
                string outputdfsfiletype = null;
                if(OutputFile != null)
                {
                    outputdfsfile = OutputFile;
                    if (outputdfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        outputdfsfile = outputdfsfile.Substring(6);
                    }
                    int reclen;
                    {
                        int iat = outputdfsfile.IndexOf('@');
                        if (-1 == iat)
                        {
                            reclen = -1;
                            outputdfsfiletype = "zd";
                        }
                        else
                        {
                            if (!int.TryParse(outputdfsfile.Substring(iat + 1), out reclen) || reclen < 1)
                            {
                                throw new Exception("Invalid record length: dfs://" + outputdfsfile);
                            }
                            outputdfsfile = outputdfsfile.Substring(0, iat);
                            DSpace_OutputRecordLength = reclen;
                            outputdfsfiletype = "rbin@" + reclen;
                        }
                    }
                }

                OnRemote(input, output);

                if (null == outputdfsfile)
                {
#if DEBUG
                    if (output.ToArray().Length != 0)
                    {
                        throw new Exception("Remote output with no output file specified");
                    }
#endif
                }
                else
                {
                    dfsclient.SetFileContent(outputdfsfile, outputdfsfiletype, output.ToArray());
                }

            }

        }


        public abstract class RemoteCall : JobCall
        {
            public string OverrideInput;
            public string OverrideOutput;

            public RemoteCall(string name)
                : base(name)
            {
            }

        }

        public class RemoteCallShellExec : RemoteCall
        {
            public readonly string JobFile;

            public RemoteCallShellExec(string name, string jobfile)
                : base(name)
            {
                this.JobFile = jobfile;
            }

            public override string Call(string args)
            {
                StringBuilder sbxpath = new StringBuilder();
                if (null != OverrideInput)
                {
                    sbxpath.Append("\"//Job[@Name='" + this.Name + "']/IOSettings/DFS_IO/DFSReader=" + OverrideInput + "\" ");
                }
                if (null != OverrideOutput)
                {
                    sbxpath.Append("\"//Job[@Name='" + this.Name + "']/IOSettings/DFS_IO/DFSWriter=" + OverrideOutput + "\" ");
                }
                return Shell("Qizmt exec " + sbxpath + "\"" + this.JobFile + "\" " + args);
            }

        }

        public class RemoteCallInProc : RemoteCall
        {
            Remote remote;

            public RemoteCallInProc(string name, Remote remote)
                : base(name)
            {
                this.remote = remote;
            }

            public override string Call(string args)
            {
                if (null != OverrideInput)
                {
                    foreach (string oinp in OverrideInput.Split(';'))
                    {
                        string inp = oinp.Trim();
                        if (inp.Length != 0)
                        {
                            remote.InputFiles.Add(inp);
                        }
                    }
                }
                if (null != OverrideOutput)
                {
                    remote.OutputFile = OverrideOutput;
                }
                return remote.Exec(ParseArgs(args));
            }

        }


        public abstract class MapReduceOutput
        {
            public abstract void Add(ByteSlice x);
        }

        public abstract class MapReduce : Job
        {

            public int DSpace_KeyLength = 0;
            public int DSpace_ProcessID = 0;
            public int DSpace_ProcessCount = 1;
            public int DSpace_InputRecordLength = -1;
            public int DSpace_OutputRecordLength = -1;

            public List<string> InputFiles = new List<string>();
            public string OutputFile;

            public int MapInputFileIndex = -1;
            public string MapInputFileName = null;


            public virtual void Map(ByteSlice line, MapOutput output)
            {
            }

            public virtual void ReduceInitialize()
            {
            }

            public virtual void Reduce(ByteSlice key, IEnumerator<ByteSlice> values, MapReduceOutput output)
            {
            }

            public virtual void ReduceFinalize()
            {
            }


            protected internal virtual List<KeyValuePair<ByteSlice, ByteSlice>> RunMap(
                IEnumerator<ByteSlice> input, List<InputDataInfo> InputInfo)
            {
                long mapiter = -1;
                StaticGlobals.MapIteration = mapiter;
                JMapOutput output = new JMapOutput();
                BuffersContext rb = StartBuffers();
                int nextii = -1;
                int nextstartindex = -1;
                if (InputInfo.Count > 0)
                {
                    nextii = 0;
                    nextstartindex = 0;
                }
                while (input.MoveNext())
                {
                    StaticGlobals.MapIteration = ++mapiter;
                    if (mapiter == nextstartindex)
                    {
                        this.MapInputFileIndex = nextii;
                        this.MapInputFileName = InputInfo[nextii].FileName;
                        if (++nextii < InputInfo.Count)
                        {
                            nextstartindex = InputInfo[nextii].StartIndex;
                        }
                        else
                        {
                            nextii = -1;
                            nextstartindex = -1;
                        }
                    }
                    Map(input.Current, output);
                    ResetBuffers(rb);
                }
                return output.results;
            }

            public List<KeyValuePair<ByteSlice, ByteSlice>> RunMap(IEnumerator<ByteSlice> input)
            {
                List<InputDataInfo> InputInfo = new List<InputDataInfo>(0);
                return RunMap(input, InputInfo);
            }

            public virtual List<KeyValuePair<ByteSlice, ByteSlice>> RunMap()
            {
#if DEBUG
                //System.Diagnostics.Debugger.Launch();
#endif
                List<InputDataInfo> AppendInputInfo = new List<InputDataInfo>();
                IEnumerator<ByteSlice> input = GetInputData(InputFiles, AppendInputInfo);
                return RunMap(input, AppendInputInfo);
            }

            public virtual void RunReduce(List<KeyValuePair<ByteSlice, ByteSlice>> mapped)
            {
                long reduceiter = -1;
                StaticGlobals.ReduceIteration = reduceiter;
                BuffersContext rb = StartBuffers();
                string outputdfsfile = null;
                string outputdfsfiletype = null;
                if (OutputFile != null)
                {
                    outputdfsfile = OutputFile;
                    if (outputdfsfile.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
                    {
                        outputdfsfile = outputdfsfile.Substring(6);
                    }
                    int reclen;
                    {
                        int iat = outputdfsfile.IndexOf('@');
                        if (-1 == iat)
                        {
                            reclen = -1;
                            outputdfsfiletype = "zd";
                        }
                        else
                        {
                            if (!int.TryParse(outputdfsfile.Substring(iat + 1), out reclen) || reclen < 1)
                            {
                                throw new Exception("Invalid record length: dfs://" + outputdfsfile);
                            }
                            outputdfsfile = outputdfsfile.Substring(0, iat);
                            //DSpace_OutputRecordLength = reclen;
                            DSpace_OutputRecordLength = reclen;
                            outputdfsfiletype = "rbin@" + reclen;
                        }
                    }
                }

                ReduceInitialize();
                ResetBuffers(rb);

                JReduceInput input = new JReduceInput(mapped);
                JReduceOutput output = new JReduceOutput();
                while (input._MoveNextKey())
                {
                    StaticGlobals.ReduceIteration = ++reduceiter;
                    Reduce(input._CurrentKey, input, output);
                    ResetBuffers(rb);
                }

                ReduceFinalize();
                ResetBuffers(rb);

                if (null == outputdfsfile)
                {
#if DEBUG
                    if (output.results.Count != 0)
                    {
                        throw new Exception("Reduce output with no output file specified");
                    }
#endif
                }
                else
                {
                    bool addnewlines = (DSpace_OutputRecordLength < 1);
                    int contentlength = 0;
                    checked
                    {
                        int trailing = addnewlines ? 2 : 0;
                        for (int i = 0; i < output.results.Count; i++)
                        {
                            contentlength += output.results[i].Length + trailing;
                        }
                    }
                    byte[] content = new byte[contentlength];
                    {
                        int ic = 0;
                        for (int i = 0; i < output.results.Count; i++)
                        {
                            output.results[i].CopyTo(content, ic);
                            ic += output.results[i].Length;
                            if (addnewlines)
                            {
                                content[ic++] = (byte)'\r';
                                content[ic++] = (byte)'\n';
                            }
                        }
                        if (ic != content.Length)
                        {
                            throw new Exception("DEBUG:  RunReduce: reduce output content length miscalculation");
                        }
                    }
                    dfsclient.SetFileContent(outputdfsfile, outputdfsfiletype, content, contentlength);
                }
            }


            protected override void Run()
            {
#if DEBUGmrrun
                System.Diagnostics.Debugger.Launch();
#endif
                List<KeyValuePair<ByteSlice, ByteSlice>> mapout = RunMap();
                RunSort(mapout);
                RunReduce(mapout);
            }


            public void RunSort(List<KeyValuePair<ByteSlice, ByteSlice>> mapout)
            {
                mapout.Sort(new Comparison<KeyValuePair<ByteSlice, ByteSlice>>(_kcmp));
            }


            static int _kcmp(KeyValuePair<ByteSlice, ByteSlice> kvp1, KeyValuePair<ByteSlice, ByteSlice> kvp2)
            {
#if DEBUG
                if (kvp1.Key.Length != kvp2.Key.Length)
                {
                    throw new Exception("Reduce key comparison: key length mismatch");
                }
#endif
                for (int i = 0; i < kvp1.Key.Length; i++)
                {
                    int cdiff = kvp1.Key[i] - kvp2.Key[i];
                    if (0 != cdiff)
                    {
                        return cdiff;
                    }
                }
                return 0;
            }


            class JMapOutput : MapOutput
            {
                internal List<KeyValuePair<ByteSlice, ByteSlice>> results = new List<KeyValuePair<ByteSlice, ByteSlice>>();
                
                int keylen = 0;
                byte[] jbuf = new byte[1024 * 8];
                int jbufpos = 0;

                public override void Add(IList<byte> keybuf, int keyoffset, int keylength,
                    IList<byte> valuebuf, int valueoffset, int valuelength)
                {
                    ByteSlice key = ByteSlice.Prepare(keybuf, keyoffset, keylength);
                    ByteSlice value = ByteSlice.Prepare(valuebuf, valueoffset, valuelength);
                    if (key.Length + value.Length + jbufpos > jbuf.Length)
                    {
                        jbuf = new byte[1024 * 8 + (key.Length + value.Length) * 4];
                        jbufpos = 0;
                    }
                    key.CopyTo(jbuf, jbufpos);
                    ByteSlice newkey = ByteSlice.Prepare(jbuf, jbufpos, key.Length);
                    jbufpos += key.Length;
                    value.CopyTo(jbuf, jbufpos);
                    ByteSlice newvaule = ByteSlice.Prepare(jbuf, jbufpos, value.Length);
                    jbufpos += value.Length;
                    if (0 == keylen)
                    {
                        keylen = newkey.Length;
                    }
                    else
                    {
                        if (keylen != newkey.Length)
                        {
#if DEBUG
                            System.Diagnostics.Debugger.Launch();
#endif
                            throw new Exception("Map output wrong key length");
                        }
                    }
                    results.Add(new KeyValuePair<ByteSlice, ByteSlice>(newkey, newvaule));
                }

            }

            class JReduceInput : IEnumerator<ByteSlice>
            {
                List<KeyValuePair<ByteSlice, ByteSlice>> entries;
                int ientry = -1;
                bool startedany = false;

                int icurkey = -1;
                bool startedcurkey = false;
                bool finishedcurkey = false;

                internal JReduceInput(List<KeyValuePair<ByteSlice, ByteSlice>> entries)
                {
                    this.entries = entries;
                }

                internal bool _MoveNextKey()
                {
                    if (startedany)
                    {
                        while (MoveNext())
                        {
                        }
                    }
                    startedany = true;
                    ientry++;
                    if (ientry >= entries.Count)
                    {
                        ientry = entries.Count;
                        icurkey = -1;
                        finishedcurkey = true;
                        return false;
                    }
                    icurkey = ientry;
                    startedcurkey = false;
                    finishedcurkey = false;
                    return true;
                }

                internal ByteSlice _CurrentKey
                {
                    get
                    {
                        return entries[icurkey].Key;
                    }
                }

                public ByteSlice Current
                {
                    get
                    {
                        if (!startedcurkey || finishedcurkey)
                        {
                            throw new InvalidOperationException("Reduce values.Current called before first element or after last element");
                        }
                        return entries[ientry].Value;
                    }
                }

                public void Dispose()
                {
                    throw new NotSupportedException();
                }

                object System.Collections.IEnumerator.Current
                {
                    get { return this.Current; }
                }

                public bool MoveNext()
                {
                    if (finishedcurkey)
                    {
                        return false;
                    }
                    if (!startedcurkey)
                    {
                        startedcurkey = true;
                        return true;
                    }
                    if (ientry + 1 >= entries.Count)
                    {
                        finishedcurkey = true;
                        return false;
                    }
                    if (0 != _kcmp(entries[icurkey], entries[ientry + 1]))
                    {
                        finishedcurkey = true;
                        return false;
                    }
                    ientry++;
                    return true;
                }

                public void Reset()
                {
                    startedcurkey = false;
                    finishedcurkey = false;
                    ientry = icurkey;
                }

            }

            class JReduceOutput : MapReduceOutput
            {
                internal List<ByteSlice> results = new List<ByteSlice>();

                byte[] jbuf = new byte[1024 * 4];
                int jbufpos = 0;

                public override void Add(ByteSlice x)
                {
                    if (x.Length + jbufpos > jbuf.Length)
                    {
                        jbuf = new byte[1024 * 4 + (x.Length) * 4];
                        jbufpos = 0;
                    }
                    x.CopyTo(jbuf, jbufpos);
                    ByteSlice newx = ByteSlice.Prepare(jbuf, jbufpos, x.Length);
                    results.Add(newx);
                    jbufpos += x.Length;
                }
            }

        }


        public abstract class MapReduceCall : JobCall
        {
            public int OverrideKeyLength = 0;
            public string OverrideInput = null;
            public string OverrideOutput = null;
            public string OverrideOutputMethod = null;
            public string OverrideFaultTolerantExecutionMode = null;

            public MapReduceCall(string name)
                : base(name)
            {
            }

        }

        public class MapReduceCallShellExec : MapReduceCall
        {
            public readonly string JobFile;

            public MapReduceCallShellExec(string name, string jobfile)
                : base(name)
            {
                this.JobFile = jobfile;
            }

            public override string Call(string args)
            {
                StringBuilder sbxpath = new StringBuilder();
                if (0 != OverrideKeyLength)
                {
                    sbxpath.Append("\"//Job[@Name='" + this.Name + "']/IOSettings/KeyLength=" + OverrideKeyLength + "\" ");
                }
                if (null != OverrideInput)
                {
                    sbxpath.Append("\"//Job[@Name='" + this.Name + "']/IOSettings/DFSInput=" + OverrideInput + "\" ");
                }
                if (null != OverrideOutput)
                {
                    sbxpath.Append("\"//Job[@Name='" + this.Name + "']/IOSettings/DFSOutput=" + OverrideOutput + "\" ");
                }
                if (null != OverrideOutputMethod)
                {
                    sbxpath.Append("\"//Job[@Name='" + this.Name + "']/IOSettings/OutputMethod=" + OverrideOutputMethod + "\" ");
                }
                if (null != OverrideFaultTolerantExecutionMode)
                {
                    sbxpath.Append("\"//Job[@Name='" + this.Name + "']/FaultTolerantExecution/Mode=" + OverrideFaultTolerantExecutionMode + "\" ");
                }
                return Shell("Qizmt exec " + sbxpath + "\"" + this.JobFile + "\" " + args);
            }

        }

        public class MapReduceCallInProc : MapReduceCall
        {

            public List<string> InputFiles = new List<string>();
            public string OutputFile;

            MapReduce map, reduce;

            public MapReduceCallInProc(string name, MapReduce mr)
                : base(name)
            {
                this.map = mr;
                this.reduce = mr;
            }

            public MapReduceCallInProc(string name, MapReduce map, MapReduce reduce)
                : base(name)
            {
                this.map = map;
                this.reduce = reduce;
            }

            public override string Call(string args)
            {
                if (null != OverrideInput)
                {
                    foreach (string oinp in OverrideInput.Split(';'))
                    {
                        string inp = oinp.Trim();
                        if (inp.Length != 0)
                        {
                            map.InputFiles.Add(inp);
                        }
                    }
                }
                if (null != OverrideOutput)
                {
                    reduce.OutputFile = OverrideOutput;
                }

                bool mrsame = object.ReferenceEquals(map, reduce);
                string[] aargs = ParseArgs(args);
                map.Init(aargs);
                List<KeyValuePair<ByteSlice, ByteSlice>> mapout = map.RunMap();
                map.RunSort(mapout);
                if (!mrsame)
                {
                    reduce.Init(aargs);
                }
                reduce.RunReduce(mapout);
                if (mrsame)
                {
                    return map.ReadToEnd();
                }
                return map.ReadToEnd() + reduce.ReadToEnd();
            }

        }

    }

}
