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
    partial class AELight
    {

        public enum GenerateType
        {
            BINARY,
            ASCII,
            WORDS,
        }

        public enum GenerateRandom
        {
            RANDOM, // .NET Random class
            DRANDOM,
            FRANDOM,
        }

        public static GenerateType GetGenerateType(string x)
        {
            if (-1 != x.IndexOf("ascii", StringComparison.OrdinalIgnoreCase))
            {
                return GenerateType.ASCII;
            }
            if (-1 != x.IndexOf("word", StringComparison.OrdinalIgnoreCase))
            {
                return GenerateType.WORDS;
            }
            return GenerateType.BINARY;
        }


        public static void Generate(List<string> xpaths, string outname, long sizeoutput, long rowsize, GenerateType type, int writersCount, GenerateRandom genrand)
        {

            int RecordLength = -1;
            if (outname.StartsWith("dfs://", StringComparison.OrdinalIgnoreCase))
            {
                outname = outname.Substring(6);
            }
            {
                int iat = outname.IndexOf('@');
                if (-1 != iat)
                {
                    try
                    {
                        RecordLength = Surrogate.GetRecordSize(outname.Substring(iat + 1));
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.Message);
                        SetFailure();
                        return;
                    }
                    outname = outname.Substring(0, iat);
                }
            }
            string reason = "";
            if (dfs.IsBadFilename(outname, out reason))
            {
                Console.Error.WriteLine("Invalid output-dfsfile: {0}", reason);
                SetFailure();
                return;
            }

            dfs dc = LoadDfsConfig();
            if (null != DfsFindAny(dc, outname))
            {
                Console.Error.WriteLine("Output file already exists in DFS: {0}", outname);
                SetFailure();
                return;
            }

            if (RecordLength > 0)
            {
                if (rowsize < 0)
                {
                    rowsize = RecordLength;
                }
                else
                {
                    if (rowsize > RecordLength)
                    {
                        Console.Error.WriteLine("Row data cannot be greater than DFS record length");
                        SetFailure();
                        return;
                    }
                }
                if (type != GenerateType.BINARY)
                {
                    Console.Error.WriteLine("Error: must specify type=bin when generating a rectangular binary DFS file");
                    SetFailure();
                    return;
                }
            }
            else
            {
                if (rowsize < 1)
                {
                    rowsize = 100;
                }
            }

            string tempfnpost = "." + Guid.NewGuid().ToString() + "." + System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
            string jobsfn = "gen-jobs.xml" + tempfnpost;

            string rnd = "";
            string nextrandBin = "";
            string nextrandAscii = "";
            string nextrandLen = "";
            string nextrandLower = "";
            string nextrandUpper = "";
            if (genrand == GenerateRandom.RANDOM)
            {
                rnd = "Random rnd = new Random(unchecked(System.DateTime.Now.Millisecond + System.Diagnostics.Process.GetCurrentProcess().Id + DSpace_BlockID));";
                nextrandBin = "rnd.Next()";
                nextrandAscii = "rnd.Next((int)' ' + 1, (int)'~' + 1)";
                nextrandLen = "rnd.Next(3, 9 + 1)";
                nextrandLower = "rnd.Next((int)'a', (int)'z' + 1)";
                nextrandUpper = "rnd.Next((int)'A', (int)'Z' + 1)";
            }
            else if (genrand == GenerateRandom.FRANDOM)
            {
                rnd = "FRandom.Seed(unchecked(System.DateTime.Now.Millisecond + System.Diagnostics.Process.GetCurrentProcess().Id + DSpace_BlockID));";
                nextrandBin = "FRandom.Next()";
                nextrandAscii = "FRandom.Next((int)' ' + 1, (int)'~' + 1)";
                nextrandLen = "FRandom.Next(3, 9 + 1)";
                nextrandLower = "FRandom.Next((int)'a', (int)'z' + 1)";
                nextrandUpper = "FRandom.Next((int)'A', (int)'Z' + 1)";
            }
            else if (genrand == GenerateRandom.DRANDOM)
            {
                if (GenerateType.ASCII == type)
                {
                    rnd = @"DRandom rnd = new DRandom(false, true, (int)' ' + 1, (int)'~' + 1);
                         DRandom rndLen = null;
                         DRandom rndLower = null;
                         DRandom rndUpper = null;";
                    nextrandBin = "rnd.Next()";
                }
                else if (GenerateType.WORDS == type)
                {
                    rnd = @"DRandom rndLen = new DRandom(false, true, 3, 9 + 1);
                            DRandom rndLower = new DRandom(false, true, (int)'a', (int)'z' + 1);
                            DRandom rndUpper = new DRandom(false, true, (int)'A', (int)'Z' + 1);
                            DRandom rnd = null;";
                }
                else
                {
                    rnd = @"DRandom rnd = new DRandom();
                         DRandom rndLen = null;
                         DRandom rndLower = null;
                         DRandom rndUpper = null;";
                }
                nextrandBin = "rnd.Next()";
                nextrandAscii = "rnd.Next()";
                nextrandLen = "rndLen.Next()";
                nextrandLower = "rndLower.Next()";
                nextrandUpper = "rndUpper.Next()";
            }

            try
            {
                string dline = "";
                string drline = "";
#if DEBUG
                dline = "#line 1 \"GEN.ds\"" + Environment.NewLine;
                drline = "#line default" + Environment.NewLine;
#endif
                string outsuffix = "";
                if (RecordLength > 0)
                {
                    outsuffix = "@" + RecordLength;
                }
                string[] slaves = dc.Slaves.SlaveList.Split(';');
                int maxWriters = (writersCount > 0) ? writersCount : Surrogate.NumberOfProcessors * slaves.Length;
                using (System.IO.StreamWriter sw = System.IO.File.CreateText(jobsfn))
                {
                    sw.Write(@"<SourceCode>
  <Jobs>
    <Job>
      <Narrative>
        <Name>Generate Data</Name>
        <Custodian></Custodian>
        <email></email>
      </Narrative>
      <IOSettings>
        <JobType>remote</JobType>");
                    for (int si = 0; si < maxWriters; si++)
                    {
                        sw.WriteLine(@"        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://" + outname + ".gen" + si.ToString() + tempfnpost + outsuffix + @"</DFSWriter>
        </DFS_IO>");
                    }
                    sw.WriteLine((@"      </IOSettings>
      <Remote>
        <![CDATA[
" + dline + @"
        public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
        {
            const bool IS_RBIN_FILE = " + ((RecordLength > 0) ? "true" : "false") + @"; // Is rectangular binary output file?
            const bool IS_ASCII = " + ((GenerateType.ASCII == type) ? "true" : "false") + @";
            const bool IS_WORDS = " + ((GenerateType.WORDS == type) ? "true" : "false") + @";
            const long size = " + sizeoutput.ToString() + @";
            const long rowsize = " + rowsize.ToString() + @";
            long fullrecordsize = rowsize + " + Environment.NewLine.Length + @"; 
            if(IS_RBIN_FILE)
            {
                fullrecordsize = Qizmt_OutputRecordLength;
            }
            long numrows = size / fullrecordsize;
            if((size % fullrecordsize) != 0)
            {
                numrows++;
            }
            " + rnd + @" 

            List<byte> onerow = new List<byte>((rowsize > 16777216) ? 16777216 : (int)rowsize);
            long numrowsPART = numrows / DSpace_BlocksTotalCount;
            if(IS_RBIN_FILE)
            {
                if(DSpace_BlockID < (numrows % DSpace_BlocksTotalCount))
                {
                    numrowsPART++;
                }
            }
            else
            {
                if(0 == DSpace_BlockID)
                {
                    numrowsPART += numrows % DSpace_BlocksTotalCount;
                }
            }
            for(long rn = 0; rn < numrowsPART; rn++)
            {
                onerow.Clear();
                byte b;
                if(IS_WORDS)
                {
                    bool fupper = true;
                    long remain = rowsize - 1; // Don't count trailing dot.
                    while(remain - 1 >= 3) // -1 here for word separator space.
                    {
                        if(onerow.Count > 0)
                        {
                            onerow.Add((byte)' ');
                            remain--;
                        }
                        long wlen = " + nextrandLen + @";
                        
                        if(wlen > remain - 1)
                        {
                            wlen = remain - 1;
                        }
                        for(int wi = 0; wi < wlen; wi++)
                        {
                            if(fupper)
                            {
                                fupper = false;
                                b = (byte)" + nextrandUpper + @";
                            }
                            else
                            {
                                b = (byte)" + nextrandLower + @";
                            }
                            onerow.Add(b);
                        }
                        remain -= wlen;
                    }
                    onerow.Add((byte)'.'); // Don't count trailing dot.
                    while(remain > 0)
                    {
                        onerow.Add((byte)' ');
                        remain--;
                    }
                }
                else
                {
                    for(int nb = 0; nb < rowsize; nb++)
                    {
                        if(IS_ASCII)
                        {
                            b = (byte)" + nextrandAscii + @";
                        }
                        else // Binary.
                        {
                            for(;;)
                            {
                                b = (byte)" + nextrandBin + @";
                                if(!IS_RBIN_FILE)
                                {
                                    if(b == 0 || b == '\n' || b == '\r')
                                    {
                                        continue;
                                    }
                                }
                                break;
                            }
                        }
                        onerow.Add(b);
                    }
                }
                if(IS_RBIN_FILE)
                {
                    while(onerow.Count < DSpace_OutputRecordLength)
                    {
                        if(IS_ASCII || IS_WORDS)
                        {
                            //onerow.Add((byte)' ');
                            throw new NotImplementedException();
                        }
                        else
                        {
                            onerow.Add((byte)0);
                        }
                    }
                    dfsoutput.WriteRecord(onerow);
                }
                else
                {
                    dfsoutput.WriteLine(onerow);
                }
            }
        }
" + drline + @"
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
").Replace("`", "\""));
                    /*
                    //dfsoutput.Write(onerow);
                    //dfsoutput.Write(`" + rowsep.Replace("\\", "\\\\").Replace("\n", "\\n").Replace("\r", "\\r").Replace("\"", "\\\"") + @"`);
                     * */
                }
                {
                    // File jobsfn exists.
                    Console.WriteLine("Generating data...");
                    Exec("", LoadConfig(xpaths, jobsfn), new string[] { }, false, false);
                    if (null != DfsFindAny(dc, outname))
                    {
                        SetFailure();
                        return;
                    }
                    Shell("DSpace -dfs combine \"dfs://" + outname + ".gen*" + tempfnpost + "\" + \"" + outname + "\"");
                    if (RecordLength > 0)
                    {
                        Console.WriteLine("Done; file '{0}' written into DFS with {1} of random data per {2} record",
                            outname, GetFriendlyByteSize(rowsize), GetFriendlyByteSize(RecordLength));
                    }
                    else
                    {
                        Console.WriteLine("Done; file '{0}' written into DFS with {1} rows", outname, GetFriendlyByteSize(rowsize));
                    }
                }
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
        }

        public static void CheckSummt(string hashname, string dfsreader)
        {
            dfs dc = LoadDfsConfig();
            if (null == DfsFindAny(dc, dfsreader))
            {
                Console.Error.WriteLine("Input file does not exist in DFS: {0}", dfsreader);
                SetFailure();
                return;
            }

            string tempfnpost = ".$" + Guid.NewGuid().ToString() + ".$" + System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
            string jobsfn = "gen" + hashname + "-jobs.xml" + tempfnpost;
            try
            {
                using (System.IO.StreamWriter sw = System.IO.File.CreateText(jobsfn))
                {
                    sw.Write((@"<SourceCode>
<Jobs>
 <Job Name=`Generate " + hashname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>4</KeyLength>
        <DFSInput>" + dfsreader + @"</DFSInput>
        <DFSOutput></DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
           ulong lsum = 0;
           public virtual void Map(ByteSlice line, MapOutput output)
            { 
                
                 byte[] buf = line.ToBytes();
              
                 for(int i=0; i < buf.Length ; i++)
                   {    
                     lsum += buf[i];
                   }   
              if(StaticGlobals.Qizmt_Last)
              {
               recordset rValue = recordset.Prepare();
               recordset rKey = recordset.Prepare();
               rKey.PutInt(0);
               rValue.PutULong(lsum);
               output.Add(rKey,rValue); 
               }
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
             
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {  
               ulong lsum = 0;
               for(int i = 0; i < values.Length; i++)
                  {
                      recordset res = recordset.Prepare(values.Items[i]);
                      ulong temp = res.GetULong();
                      lsum += temp;
                  }
              
              
                StringBuilder sbresult = new StringBuilder(32);
                sbresult.Append(`\n" + hashname + @"` + ` of `);
                sbresult.Append(@`dfs://" + dfsreader + @"`);
                sbresult.Append(`:  `);
                sbresult.Append(lsum);
                DSpace_Log(sbresult.ToString());
               
             }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
</Jobs>
</SourceCode>
").Replace("`", "\""));
                }

                Console.WriteLine("Generating {0}...", hashname);
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
            }

       }

        public static void GenerateHash(string hashname, string dfsreader)
        {
            dfs dc = LoadDfsConfig();
            if (null == DfsFindAny(dc, dfsreader))
            {
                Console.Error.WriteLine("Input file does not exist in DFS: {0}", dfsreader);
                SetFailure();
                return;
            }

            string tempfnpost = ".$" + Guid.NewGuid().ToString() + ".$" + System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
            string jobsfn = "gen" + hashname + "-jobs.xml" + tempfnpost;
            try
            {
                using (System.IO.StreamWriter sw = System.IO.File.CreateText(jobsfn))
                {
                    sw.Write((@"<SourceCode>
  <Jobs>
    <Job>
      <Narrative>
        <Name>Generate " + hashname + @"</Name>
        <Custodian></Custodian>
        <email></email>
      </Narrative>
<IOSettings>
        <JobType>remote</JobType>
    <DFS_IO>
          <DFSReader>" + dfsreader + @"</DFSReader>
          <DFSWriter></DFSWriter>
    </DFS_IO>
</IOSettings>
      <Remote>
        <![CDATA[

        public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
        {
            string hashname = @`" + hashname  + @"`;
            if(0 == string.Compare(`MD5`, hashname, true))
            {
                System.Security.Cryptography.MD5 md5 = new System.Security.Cryptography.MD5CryptoServiceProvider();
                byte[] hashresult = md5.ComputeHash(dfsinput);
                StringBuilder sbresult = new StringBuilder(32);
                sbresult.Append(hashname + ` of `);
                sbresult.Append(@`dfs://" + dfsreader + @"`);
                sbresult.Append(`:  `);
                foreach(byte hb in hashresult)
                {
                    sbresult.Append(hb.ToString(`x2`));
                }
                DSpace_Log(sbresult.ToString());

            }
            else if(0 == string.Compare(`Sum`, hashname, true))
            {
                ulong lsum = 0;
                for(;;)
                {
                    int ib = dfsinput.ReadByte();
                    if(ib < 0)
                    {
                        break;
                    }
                
                    lsum += (byte)ib;
                }
                StringBuilder sbresult = new StringBuilder(32);
                sbresult.Append(hashname + ` of `);
                sbresult.Append(@`dfs://" + dfsreader + @"`);
                sbresult.Append(`:  `);
                sbresult.Append(lsum);
                DSpace_Log(sbresult.ToString());

            }
            else if(0 == string.Compare(`Sum2`, hashname, true))
            {
                ulong lsum = 0;
                bool prevnl = true;
                for(;;)
                {
                    int ib = dfsinput.ReadByte();
                    if(ib < 0)
                    {
                        if(prevnl)
                        {
                            break;
                        }
                        prevnl = true;
                        ib = '\n';
                    }
                    if('\r' == ib || '\n' == ib)
                    {
                        if(prevnl)
                        {
                            continue;
                        }
                        prevnl = true;
                        ib = '\n';
                    }
                    else
                    {
                        prevnl = false;
                    }
                    lsum += (byte)ib;
                }
                StringBuilder sbresult = new StringBuilder(32);
                sbresult.Append(hashname + ` of `);
                sbresult.Append(@`dfs://" + dfsreader + @"`);
                sbresult.Append(`:  `);
                sbresult.Append(lsum);
                DSpace_Log(sbresult.ToString());

            }
        }

        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
").Replace("`", "\""));
                }
                Console.WriteLine("Generating {0}...", hashname);
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
            }
        }

        public static void CheckSortedmt(string dfsreader)
        {


        }

        
        public static void CheckSorted(string dfsreader)
        {
            dfs dc = LoadDfsConfig();
            if (null == DfsFindAny(dc, dfsreader))
            {
                Console.Error.WriteLine("Input file does not exist in DFS: {0}", dfsreader);
                SetFailure();
                return;
            }

            string tempfnpost = ".$" + Guid.NewGuid().ToString() + ".$" + System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
            string jobsfn = "checksorted-jobs.xml" + tempfnpost;
            try
            {
                using (System.IO.StreamWriter sw = System.IO.File.CreateText(jobsfn))
                {
                    sw.Write((@"<SourceCode>
  <Jobs>
    <Job>
      <Narrative>
        <Name>Check Sorted</Name>
        <Custodian></Custodian>
        <email></email>
      </Narrative>
<IOSettings>
        <JobType>remote</JobType>
    <DFS_IO>
          <DFSReader>" + dfsreader + @"</DFSReader>
          <DFSWriter></DFSWriter>
    </DFS_IO>
</IOSettings>
      <Remote>
        <![CDATA[

            static int CompareBytes(IList<byte> x, IList<byte> y)
            {
                for (int i = 0;; i++)
                {
                    if(i >= x.Count)
                    {
                        if(i >= y.Count)
                        {
                            return 0;
                        }
                        return -1;
                    }
                    if(i >= y.Count)
                    {
                        return 1;
                    }
                    int diff = x[i] - y[i];
                    if (0 != diff)
                    {
                        return diff;
                    }
                }
                return 0;
            }

        public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
        {
            List<byte> prevline = new List<byte>();
            List<byte> line = new List<byte>();
            List<byte> templist;

            //prevline.Clear();
            if(!dfsinput.ReadLineAppend(prevline))
            {
                DSpace_Log(`No input data`);
                return;
            }

            ulong linnum = 1;
            for(;;)
            {
                linnum++;
                
                line.Clear();
                dfsinput.ReadLineAppend(line);
                if(line.Count == 0 && dfsinput.EndOfStream)
                {
                    break;
                }
                
                if(CompareBytes(prevline, line) > 0)
                {
                    DSpace_Log(`Not sorted starting on line ` + linnum.ToString());
                    DSpace_Log(`  ` + (linnum - 1).ToString() + `: \`` + Encoding.UTF8.GetString(prevline.ToArray()) + `\`\n  ` + linnum.ToString() + `: \`` + Encoding.UTF8.GetString(line.ToArray()) + `\``);
                    return;
                }
                
                templist = prevline;
                prevline = line;
                line = templist;
                
            }
            DSpace_Log(`Sorted`);

        }

        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
").Replace("`", "\""));
                }
                Console.WriteLine("Checking if sorted...");
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
            }
        }


    }
}
