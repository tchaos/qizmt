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
    public class StressTests
    {
        #region SortTests
        public class SortTests
        {
            public static void GenerateTests()
            {
                string[] sizes = new string[] { "10GB", "1TB", "5TB" };
#if DEBUG
                sizes = new string[] { "1KB", "5KB" };
#endif
                List<string> alljobfiles = new List<string>();
                List<string> jobfilesverify = new List<string>();

                foreach (string size in sizes)
                {
                    GenerateGrouped(size, alljobfiles);
                    GenerateHashSorted(size, alljobfiles, jobfilesverify);
                    GenerateSorted(size, alljobfiles, jobfilesverify);
                    GenerateSortedGensort(size, alljobfiles, jobfilesverify);
                    GenerateSortedPOS(size, alljobfiles);
                }

                GeneratePrereq();
                GenerateSortTestsDriver(alljobfiles, jobfilesverify);
            }

            private static void GenerateGrouped(string size, List<string> alljobfiles)
            {
                string jobname = "grouped_" + size + "_Of_100_Byte_Rows";
                string jobfname = jobname + ".xml";
                string output = jobname + "_output.txt";
                string input = jobname + "_input.txt";

                alljobfiles.Add(jobfname);
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
    (@"<SourceCode>
  <Jobs>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace bingen " + input + @" " + size + @" 100B`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <!-- <Verbose>
            b   B   d   D   m   e   s   r   0
      </Verbose> -->
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://" + input + @"</DFSInput>
        <DFSOutput>dfs://" + output + @"</DFSOutput>
        <KeyMajor>2</KeyMajor>
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
        <ReduceInitialize>
          <![CDATA[
          public void ReduceInitialize(){}
        ]]>
        </ReduceInitialize>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Add(key);
              }
          }
       ]]>
        </Reduce>
        <ReduceFinalize>
          <![CDATA[
          public void ReduceFinalize(){}
        ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            }

            private static void GenerateHashSorted(string size, List<string> alljobfiles, List<string> jobfilesverify)
            {
                string jobname = "hashsorted_" + size + "_Of_100_Byte_Rows";
                string jobfname = jobname + ".xml";
                string output = jobname + "_output.txt";
                string input = jobname + "_input.txt";

                alljobfiles.Add(jobfname);
                jobfilesverify.Add(jobfname);
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
    (@"<SourceCode>
  <Jobs>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace bingen " + input + @" " + size + @" 100B`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <!-- <Verbose>
            b   B   d   D   m   e   s   r   0
      </Verbose> -->
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://" + input + @"</DFSInput>
        <DFSOutput>dfs://" + output + @"</DFSOutput>
        <KeyMajor>2</KeyMajor>
        <OutputMethod>hashsorted</OutputMethod>
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
        <ReduceInitialize>
          <![CDATA[
          public void ReduceInitialize(){}
        ]]>
        </ReduceInitialize>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Add(key);
              }
          }
       ]]>
        </Reduce>
        <ReduceFinalize>
          <![CDATA[
          public void ReduceFinalize(){}
        ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            }

            private static void GenerateHashFaceHaarSorted(string size, List<string> alljobfiles)
            {
                string jobname = "hashsorted_facehaar_" + size + "_Of_100_Byte_Rows";
                string jobfname = jobname + ".xml";
                string output = jobname + "_output.txt";
                string input = jobname + "_input.txt";

                alljobfiles.Add(jobfname);
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
    (@"<SourceCode>
  <Jobs>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace bingen " + input + @" " + size + @" 100B`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <OpenCVExtension/>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://" + input + @"</DFSInput>
        <DFSOutput>dfs://" + output + @"</DFSOutput>
        <KeyMajor>2</KeyMajor>
        <OutputMethod>hashsorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {              
              output.Add(line, ByteSlice.Prepare(@`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`));
          }
        ]]>
        </Map>
        <ReduceInitialize>
          <![CDATA[
          public void ReduceInitialize(){}
        ]]>
        </ReduceInitialize>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {                  
                  string x = values[i].Value.ToString();
                  byte[] data = Convert.FromBase64String(x);
                  System.IO.MemoryStream ms = new System.IO.MemoryStream(data);
                  System.Drawing.Bitmap srcBmp = new System.Drawing.Bitmap(ms);
                  ms.Close();
                  
                  byte[] faces = GetFaces(srcBmp);
                  srcBmp = null;
                  output.AddBinary(Blob.Prepare(Guid.NewGuid().ToString() + `.bmp`, faces));
              }
          }
          
         private byte[] GetFaces(System.Drawing.Bitmap srcBmp)
        {            
            openCV.IplImage srcImg = openCV.cvlib.ToIplImage(srcBmp, false);            

            // create gray scale image
			openCV.IplImage gray = openCV.cvlib.CvCreateImage(new openCV.CvSize(srcImg.width, srcImg.height), (int)openCV.cvlib.IPL_DEPTH_8U, 1);				
			// do color conversion
			openCV.cvlib.CvCvtColor(ref srcImg, ref gray, openCV.cvlib.CV_BGR2GRAY);

			//openCV.IplImage smImg = srcImg;
			openCV.CvMemStorage storage = openCV.cvlib.CvCreateMemStorage(0);
			openCV.CvSeq faces;
			int i, scale = 1;
            
            string xmlpath = @`.\haarcascade_frontalface_alt2.xml`;

			if (!System.IO.File.Exists(xmlpath))
			{                
				    openCV.cvlib.CvReleaseMemStorage(ref storage);
                openCV.cvlib.CvReleaseImage(ref gray);
                openCV.cvlib.CvReleaseImage(ref srcImg);
				    throw new Exception(`haarcascade_frontalface_alt2.xml is not found.`);
			}

			IntPtr p = openCV.cvlib.CvLoad(xmlpath);
			openCV.CvHaarClassifierCascade cascade = (openCV.CvHaarClassifierCascade)openCV.cvtools.ConvertPtrToStructure(p, typeof(openCV.CvHaarClassifierCascade));
			cascade.ptr = p;
			
			//use the fastest variant
            faces = openCV.cvlib.CvHaarDetectObjects(ref srcImg, ref cascade, ref storage, 1.2, 2, openCV.cvlib.CV_HAAR_DO_CANNY_PRUNING, new openCV.CvSize(0, 0));

			//Crop all the rectangles
			for (i = 0; i < faces.total; i++)
			{
				//extract the rectanlges
				openCV.CvRect face_rect = (openCV.CvRect)openCV.cvtools.ConvertPtrToStructure(openCV.cvlib.CvGetSeqElem(ref faces, i), typeof(openCV.CvRect));
                
            openCV.cvlib.CvRectangle(ref srcImg, new openCV.CvPoint(face_rect.x * scale, face_rect.y * scale),
                                                     new openCV.CvPoint((face_rect.x + face_rect.width) * scale, (face_rect.y + face_rect.height) * scale),
                                                     openCV.cvlib.CV_RGB(255, 0, 0), 3, 8, 0);
			}

            System.Drawing.Bitmap resultBmp = openCV.cvlib.ToBitmap(srcImg, false);
            System.IO.MemoryStream mss = new System.IO.MemoryStream();
            resultBmp.Save(mss, System.Drawing.Imaging.ImageFormat.Bmp);
            byte[] buf = mss.ToArray();
            mss.Close();
            
            openCV.cvlib.CvReleaseMemStorage(ref storage);
            openCV.cvlib.CvReleaseHaarClassifierCascade(ref cascade);
            if (srcImg.imageData != IntPtr.Zero)
            {
                openCV.cvlib.CvReleaseImage(ref srcImg);
            }
            if (gray.imageData != IntPtr.Zero)
            {
                openCV.cvlib.CvReleaseImage(ref gray);
            }

            return buf;
        }
       ]]>
        </Reduce>
        <ReduceFinalize>
          <![CDATA[
          public void ReduceFinalize(){}
        ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            }

            private static void GenerateSorted(string size, List<string> alljobfiles, List<string> jobfilesverify)
            {
                string jobname = "sorted_" + size + "_Of_100_Byte_Rows";
                string jobfname = jobname + ".xml";
                string output = jobname + "_output.txt";
                string input = jobname + "_input.txt";

                alljobfiles.Add(jobfname);
                jobfilesverify.Add(jobfname);
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
    (@"
<SourceCode>
  <Jobs>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace bingen " + input + @" " + size + @" 100B`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://" + input + @"</DFSInput>
        <DFSOutput>dfs://" + output + @"</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
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
        <ReduceInitialize>
          <![CDATA[
          public void ReduceInitialize(){}
        ]]>
        </ReduceInitialize>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Add(key);
              }
          }
       ]]>
        </Reduce>
        <ReduceFinalize>
          <![CDATA[
          public void ReduceFinalize(){}
        ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            }

            private static void GenerateSortedGensort(string size, List<string> alljobfiles, List<string> jobfilesverify)
            {
                string jobname = "sorted_gensort_" + size + "";
                string jobfname = jobname + ".xml";
                string output = jobname + "_output.gensort";
                string input = jobname + "_input.gensort";

#if DEBUG
                const long maxrowsatatime = 1024 / 100;
#else
                const long maxrowsatatime = 536870912 / 100;
#endif

                alljobfiles.Add(jobfname);
                jobfilesverify.Add(jobfname);
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
    (@"
<SourceCode>
  <Jobs>
    <Job Name=`" + jobname + @"_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            // Delete old files.
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + input + @"`);

            // Determine how many 100-byte rows to create in total.
            long lc = " + AELight.ParseLongCapacity(size) + @"; // ParseLongCapacity
            long rows = lc / 100;
            if(0 != (lc % 100))
            {
                rows++;
            }
            
            // Create the unsorted input data...
            const long maxrowsatatime = " + maxrowsatatime.ToString() + @";
            string guid = Guid.NewGuid().ToString();
            int counter = 0;
            long rowsdone = 0;
            while(rowsdone < rows)
            {
                // Only create about 512MB of rows at a time in separate MR.DFS files.
                long thisrows = (rows - rowsdone);
                if(thisrows > maxrowsatatime)
                {
                    thisrows = maxrowsatatime;
                }
                rowsdone += thisrows;
                // gensort to create rows of random data to a temporary location, then put it into MR.DFS.
                string fn = `" + input + @"` + guid + (counter++).ToString();
                string tempfn = IOUtils.GetTempDirectory() + @`\` + fn;
                Shell(@`gensort -b` + rowsdone.ToString() + @` ` + thisrows.ToString() + @` ` + tempfn, true);
                try
                {
                    Shell(@`DSpace put ` + tempfn + @` ` + fn + @`@100`);
                }
                finally
                {
                    System.IO.File.Delete(tempfn);
                }
            }
            
            // Combine all the separate MR.DFS files into one, containing all the input data.
            Shell(@`DSpace combine " + input + @"` + guid + @`* +" + input + @"`);

        }
        ]]>
      </Local>
    </Job>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>10</KeyLength>
        <DFSInput>dfs://" + input + @"@100</DFSInput>
        <DFSOutput>dfs://" + output + @"@100</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              // Prepare 10 byte keys and 90 byte values from the 100 bytes of the input record.
              output.Add(ByteSlice.Prepare(line, 0, 10), ByteSlice.Prepare(line, 10, 90));
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          List<byte> outrecord = new List<byte>(100);
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              // Move through all these sorted values and write records to the output.
              while(values.MoveNext())
              {
                  outrecord.Clear();
                  key.AppendTo(outrecord);
                  values.Current.AppendTo(outrecord);
                  output.WriteRecord(ByteSlice.Prepare(outrecord));
              }
          }
       ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`" + jobname + @"_Postprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            
            // Get the number of parts of this file.
            int numparts;
            {
                string line = Shell(@`dspace ls " + output + @"`).Split('\n')[0].Trim();
                int ilp = line.LastIndexOf('(');
                const string END = ` parts)`;
                if(!line.EndsWith(END) || -1 == ilp)
                {
                    throw new Exception(`ls output error: ` + line);
                }
                string s = line.Substring(ilp + 1, line.Length - (ilp + 1) - END.Length);
                numparts = int.Parse(s);
            }
            
            string tempfn = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
            try
            {
                // Get parts of the output file from MR.DFS a few at a time;
                // ensure it is sorted with valsort.
                const int inc = 8;
                for(int ipart = 0; ipart < numparts; ipart += inc)
                {
                    int x = numparts - ipart;
                    if(x > inc)
                    {
                        x = inc;
                    }
                    Shell(@`DSpace get parts=` + ipart.ToString() + @`-` + (ipart + x - 1).ToString() + @` " + output + @" ` + tempfn);
                    Shell(@`valsort ` + tempfn);
                    System.IO.File.Delete(tempfn);
                }
            }
            finally
            {
                try
                {
                    System.IO.File.Delete(tempfn);
                }
                catch
                {
                }
            }
            DSpace_Log(`Output is sorted`);

        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            }

            private static void GenerateSortedPOS(string size, List<string> alljobfiles)
            {
                string jobname = "sorted_POS_" + size + "_Of_100_Byte_Rows";
                string jobfname = jobname + ".xml";
                string output = jobname + "_output.txt";
                string input = jobname + "_input.txt";

                alljobfiles.Add(jobfname);
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
    (@"
<SourceCode>
  <Jobs>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace wordgen " + input + " " + size + @" 100B`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://" + input + @"</DFSInput>
        <DFSOutput>dfs://" + output + @"</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              output.Add(line, line);
          }
        ]]>
        </Map>
        <ReduceInitialize>
          <![CDATA[
          public void ReduceInitialize(){}
        ]]>
        </ReduceInitialize>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
          {                
              for(int i = 0; i < values.Length; i++)
              {
                  string pos = values[i].Value.ToPOSString();
                  output.Add(ByteSlice.Prepare(pos));
              }
          }
       ]]>
        </Reduce>
        <ReduceFinalize>
          <![CDATA[
          public void ReduceFinalize(){}
        ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            }

            private static void GenerateSortTestsDriver(List<string> alljobfiles, List<string> jobfilesverify)
            {
                string sort = "";
                foreach (string job in alljobfiles)
                {
                    sort += "sort.Add(`" + job + "`);" + Environment.NewLine;
                }

                string verify = "";
                foreach (string job in jobfilesverify)
                {
                    verify += "verify.Add(`" + job + "`);" + Environment.NewLine;
                }

                alljobfiles.Add("sortTestsDriver.xml");
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
    (@"
<SourceCode>
  <Jobs>
    <Job Name=`sortTestsDriver_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>test</JobType>
      </IOSettings>
      <Test>
        <![CDATA[
        public virtual void Test()
        {
            string mode = `s`;
            if(DSpace_ExecArgs.Length > 0)
            {
                mode = DSpace_ExecArgs[0].ToLower();
            }
            
            List<string> sort = new List<string>();
            " + sort + @"
            
            List<string> verify = new List<string>();
            " + verify + @"
            
            if(mode.StartsWith(`s`))
            {
                DSpace_Log(`Begin stress tests...`);
                DSpace_Log(`-`);
                foreach(string job in sort)
                {                    
                    DSpace_Log(`Qizmt exec ` + job);
                    DSpace_Log(Shell(`DSpace exec ` + job));
                    DSpace_Log(`-`);
                }
            }
            
            if(mode.EndsWith(`v`))
            {
                DSpace_Log(`Begin verifications...`);
                DSpace_Log(`-`);
                foreach(string job in verify)
                {
                    Shell(`DSpace exec regression_test_checkSorted.xml ` + job + ` ` + job.Substring(0, job.Length - 4) + `_output.txt`);
                    bool pass = false;
                    string[] info = Shell(@`DSpace info ` + job + `.PASSED`, true).Split('\n');

                    if(info.Length > 1)
                    {
                        if(info[1].IndexOf(`No such file`) == -1)
                        {
                            pass = true;
                        }
                    }
                    
                    if(pass)
                    {
                        DSpace_LogResult(`Qizmt exec ` + job, true);
                    }
                    else
                    {                        
                        DSpace_LogResult(`Qizmt exec ` + job, false);
                    } 
                    
                    Shell(@`DSpace del ` + job + `.FAILED`, true);
                    Shell(@`DSpace del ` + job + `.PASSED`, true);
                }
            }            
        }
        ]]>
      </Test>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0} [s = run tests]", alljobfiles[alljobfiles.Count - 1]);
                Console.WriteLine("    Qizmt exec {0} [sv = run tests and verify results]", alljobfiles[alljobfiles.Count - 1]);
                Console.WriteLine("    Qizmt exec {0} [v = verify results]", alljobfiles[alljobfiles.Count - 1]);
            }

            private static void GeneratePrereq()
            {
                AELight.DfsDelete("regression_test_checkSorted.xml", false);
                AELight.DfsPutJobsFileContent("regression_test_checkSorted.xml",
    (@"
<SourceCode>
  <Jobs>
    <Job Name=`regression_test_checkSorted` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            string mr = DSpace_ExecArgs[0];
            string fileToCheck = DSpace_ExecArgs[1];            
            string pass = mr+ `.PASSED`;
            string fail = mr + `.FAILED`;
            string temp = IOUtils.GetTempDirectory() + @`\` + Guid.NewGuid().ToString();
            System.IO.File.WriteAllText(temp, `x`);
            
            Shell(@`DSpace del ` + pass);
            Shell(@`DSpace del ` + fail);
            
            string[] rr = Shell(@`DSpace checksorted ` + fileToCheck).Split('\n');
            bool sorted = false;
            foreach(string rline in rr)
            {
                if(rline.StartsWith(`Sorted`))
                {
                    sorted = true;
                    break;
                }
            }           
            if(sorted)
            {
                Shell(@`DSpace put ` + temp + ` ` + pass);
            }
            else
            {
                Shell(@`DSpace put ` + temp + ` ` + fail);
            }            
            
            System.IO.File.Delete(temp);
        }
        ]]>
      </Local>
    </Job>    
  </Jobs>
</SourceCode>
").Replace('`', '"'));
            }
        }
        #endregion

        #region ValueSizeTests
        public class ValueSizeTests
        {
            public static void GenerateTests()
            {
                List<string> alljobfiles = new List<string>();

                #region Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows
                {
                    string inputfilesize = "10GB";
#if DEBUG
                    inputfilesize = "70MB";
#endif

                    alljobfiles.Add("Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows.xml");
                    AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                    AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
    (@"<SourceCode>
  <Jobs>
    <Job Name=`Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Input.txt`);
            Shell(@`DSpace del Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Output.txt`);
            Shell(@`DSpace gen Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Input.txt " + inputfilesize + @"`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Input.txt</DFSInput>
        <DFSOutput>dfs://Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          int cnt = 0;
          byte[] buf = new byte[200];
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                int bc = ++cnt % 201;
                if(bc == 0)
                {
                    bc = 1;
                }                
                ByteSlice bval = ByteSlice.Prepare(buf, 0, bc);
                output.Add(line, bval);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Add(ByteSlice.Prepare(values[i].Value.Length.ToString()));                  
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Verification` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            if(DSpace_ExecArgs.Length > 0 && DSpace_ExecArgs[0].ToLower() == `v`)
            {
                Shell(@`DSpace exec StressTests_verifyValueSize.xml Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows.xml Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Output.txt 200`);
            }
            
            Shell(@`DSpace del Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Input.txt`);
            Shell(@`DSpace del Mapper_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Output.txt`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                    Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
                }
                #endregion

                #region Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows
                {
                    string inputfilesize = "10GB";
#if DEBUG
                    inputfilesize = "70MB";
#endif
                   
                    alljobfiles.Add("Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows.xml");
                    AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                    AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                        (@"<SourceCode>
  <Jobs>
    <Job Name=`Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Input.txt`);
            Shell(@`DSpace del Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Output.txt`);
            Shell(@`DSpace del Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Cache`);
            Shell(@`DSpace gen Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Input.txt " + inputfilesize + @"`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows` Custodian=`` Email=``>
      <Delta>
        <Name>Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Cache</Name>
        <DFSInput>dfs://Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Input.txt</DFSInput>
      </Delta>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput></DFSInput>
        <DFSOutput>dfs://Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          int cnt = 0;
          byte[] buf = new byte[200];
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                int bc = ++cnt % 201;
                if(bc == 0)
                {
                    bc = 1;
                }
                
                ByteSlice bval = ByteSlice.Prepare(buf, 0, bc);
                output.Add(line, bval);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Cache(key, values[i].Value);
                  output.Add(ByteSlice.Prepare(values[i].Value.Length.ToString()));                 
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Verification` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            if(DSpace_ExecArgs.Length > 0 && DSpace_ExecArgs[0].ToLower() == `v`)
            {
                Shell(@`DSpace exec StressTests_verifyValueSize.xml Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows.xml Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Output.txt 200`);
            }
            
            Shell(@`DSpace del Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Input.txt`);
            Shell(@`DSpace del Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Output.txt`);
            Shell(@`DSpace del Cache_Value_Size_1-200_Byte_10GB_Of_100_Byte_Rows_Cache`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                    Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
                }
                #endregion               

                {
                    string rowsize = "1MB";
#if DEBUG
                    rowsize = "100B";
#endif
                   
                    GenerateMapperVariedValueSize(2000, rowsize, alljobfiles);
                    GenerateMapperVariedValueSize(20000, rowsize, alljobfiles);

                    GenerateCacheVariedValueSize(2000, rowsize, alljobfiles);
                    GenerateCacheVariedValueSize(20000, rowsize, alljobfiles);

                    GenerateMapperFixedValueSize(rowsize, 20000, alljobfiles);
                    GenerateCacheFixedValueSize(rowsize, 20000, alljobfiles);
                }

                GenerateValueSizeTestsDriver(alljobfiles);
                GeneratePrereq();
            }

            #region GenerateMapperVariedValueSize
            private static void GenerateMapperVariedValueSize(int maxValueSize, string rowSize, List<string> alljobfiles)
            {
                string jobname = string.Format("Mapper_Value_Size_1-{0}_Byte_{0}_Of_{1}_Rows", maxValueSize, rowSize);
                string jobfname = jobname + ".xml";
                string drivername = jobname + "_driver";
                string output = jobname + "_output.txt";
                string input = jobname + "_input.txt";
                long inputfilesize = (long)maxValueSize * (AELight.ParseLongCapacity(rowSize) + (long)Environment.NewLine.Length);

                alljobfiles.Add(drivername + ".xml");
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                    (@"<SourceCode>
  <Jobs>
    <Job Name=`" + drivername + @"_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del " + jobfname + @"`);
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace gen " + input + @" " + inputfilesize.ToString() + "B " + rowSize + @"`);
            
            string[] results = Shell(@`DSpace exec stressTests_lineCount.xml " + input + @"`).Split('\n');
            
            Dictionary<int, int> counts = new Dictionary<int, int>();
            
            foreach(string line in results)
            {
                if(line.StartsWith(`processID`))
                {
                    string[] parts = line.Split(':');
                    int processID = Int32.Parse(parts[1]);
                    int count = Int32.Parse(parts[2]);
                    counts[processID] = count;
                    DSpace_Log(processID.ToString());
                    DSpace_Log(count.ToString());
                }
            }
            
            string seeds = ``;
            int totalCount = 0;
            foreach(KeyValuePair<int, int> pair in counts)
            {
                seeds += `,{` + pair.Key.ToString() + `,` + totalCount.ToString() + `}`;
                totalCount += pair.Value;                
            }
            seeds = seeds.Trim(',');
            
            DSpace_Log(seeds);
            
            string mr = (@`<SourceCode>
  <Jobs>    
    <Job Name=^" + jobname + @"^ Custodian=^^ Email=^^>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://" + input + @"</DFSInput>
        <DFSOutput>dfs://" + output + @"</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          Dictionary<int, int> seeds = new Dictionary<int, int>() {` + seeds + @`};                 
          int cnt = 0;
          int maxbc = " + maxValueSize.ToString() + @";   
          byte[] buf = new byte[" + maxValueSize.ToString() + @"];
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                byte[] bline = line.ToBytes();
                ByteSlice key = ByteSlice.Prepare(bline, 0, 100);

                cnt++;
                int linenum = seeds[DSpace_ProcessID] + cnt;
                int bc = linenum % (maxbc+1);
                if(bc == 0)
                {
                    bc = 1;
                }
                
                ByteSlice bval = ByteSlice.Prepare(buf, 0, bc);
                output.Add(key, bval);
                DSpace_Log(bc.ToString());
          }
        ##>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Add(ByteSlice.Prepare(values[i].Value.Length.ToString()));                  
              }
          }
        ##>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>`).Replace('^', '`').Replace('#',']');

            string dir = IOUtils.GetTempDirectory() + @`\stressTestsMapperValueSize\`;
            if(System.IO.Directory.Exists(dir))
            {
                System.IO.Directory.Delete(dir, true);
            }
            System.IO.Directory.CreateDirectory(dir);
            
            System.IO.File.WriteAllText(dir + `" + jobfname + @"`, mr);
            
            Shell(@`DSpace importdir ` + dir);
            
            Shell(@`DSpace exec " + jobfname + @"`);

            if(DSpace_ExecArgs.Length > 0 && DSpace_ExecArgs[0].ToLower() == `v`)
            {
                Shell(@`DSpace exec StressTests_verifyValueSize.xml " + drivername + @".xml " + output + @" " + maxValueSize.ToString() + @"`);
            }

            System.IO.Directory.Delete(dir, true);
            Shell(@`DSpace del " + jobfname + @"`);
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + input + @"`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            }
            #endregion

            #region GenerateCacheVariedValueSize
            private static void GenerateCacheVariedValueSize(int maxValueSize, string rowSize, List<string> alljobfiles)
            {
                string jobname = string.Format("Cache_Value_Size_1-{0}_Byte_{0}_Of_{1}_Rows", maxValueSize, rowSize);
                string jobfname = jobname + ".xml";
                string drivername = jobname + "_driver";
                string cache = jobname + "_cache";
                string output = jobname + "_output.txt";
                string input = jobname + "_input.txt";
                long inputfilesize = (long)maxValueSize * (AELight.ParseLongCapacity(rowSize) + (long)Environment.NewLine.Length);

                alljobfiles.Add(drivername + ".xml");
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                    (@"<SourceCode>
  <Jobs>
    <Job Name=`" + drivername + @"_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del " + jobfname + @"`);
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + cache + @"`);
            Shell(@`DSpace gen " + input + @" " + inputfilesize.ToString() + "B " + rowSize + @"`);
            
            string[] results = Shell(@`DSpace exec stressTests_lineCount.xml " + input + @"`).Split('\n');
            
            Dictionary<int, int> counts = new Dictionary<int, int>();
            
            foreach(string line in results)
            {
                if(line.StartsWith(`processID`))
                {
                    string[] parts = line.Split(':');
                    int processID = Int32.Parse(parts[1]);
                    int count = Int32.Parse(parts[2]);
                    counts[processID] = count;
                    DSpace_Log(processID.ToString());
                    DSpace_Log(count.ToString());
                }
            }
            
            string seeds = ``;
            int totalCount = 0;
            foreach(KeyValuePair<int, int> pair in counts)
            {
                seeds += `,{` + pair.Key.ToString() + `,` + totalCount.ToString() + `}`;
                totalCount += pair.Value;                
            }
            seeds = seeds.Trim(',');
            
            DSpace_Log(seeds);
            
            string mr = (@`<SourceCode>
  <Jobs>    
    <Job Name=^" + jobname + @"^ Custodian=^^ Email=^^>
      <Delta>
        <Name>" + cache + @"</Name>
        <DFSInput>dfs://" + input + @"</DFSInput>
      </Delta>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput></DFSInput>
        <DFSOutput>dfs://" + output + @"</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          Dictionary<int, int> seeds = new Dictionary<int, int>() {` + seeds + @`};                 
          int cnt = 0;
          int maxbc = " + maxValueSize.ToString() + @";   
          byte[] buf = new byte[" + maxValueSize.ToString() + @"];
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                byte[] bline = line.ToBytes();
                ByteSlice key = ByteSlice.Prepare(bline, 0, 100);

                cnt++;
                int linenum = seeds[DSpace_ProcessID] + cnt;
                int bc = linenum % (maxbc+1);
                if(bc == 0)
                {
                    bc = 1;
                }
                
                ByteSlice bval = ByteSlice.Prepare(buf, 0, bc);
                output.Add(key, bval);
                DSpace_Log(bc.ToString());
          }
        ##>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Cache(key, values[i].Value);
                  output.Add(ByteSlice.Prepare(values[i].Value.Length.ToString()));                 
              }
          }
        ##>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>`).Replace('^', '`').Replace('#',']');

            string dir = IOUtils.GetTempDirectory() + @`\stressTestsCacheValueSize\`;
            if(System.IO.Directory.Exists(dir))
            {
                System.IO.Directory.Delete(dir, true);
            }
            System.IO.Directory.CreateDirectory(dir);
            
            System.IO.File.WriteAllText(dir + `" + jobfname + @"`, mr);
            
            Shell(@`DSpace importdir ` + dir);
            
            Shell(@`DSpace exec " + jobfname + @"`);     

            if(DSpace_ExecArgs.Length > 0 && DSpace_ExecArgs[0].ToLower() == `v`)
            {
                Shell(@`DSpace exec StressTests_verifyValueSize.xml " + drivername + @".xml " + output + @" " + maxValueSize.ToString() + @"`);
            }       
         
            System.IO.Directory.Delete(dir, true);
            Shell(@`DSpace del " + jobfname + @"`);
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + cache + @"`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            }
            #endregion

            #region GenerateMapperFixedValueSize
            private static void GenerateMapperFixedValueSize(string valueSize, int rowCount, List<string> alljobfiles)
            {
                string jobname = string.Format("Mapper_Value_Size_{0}_{1}_Of_{0}_Rows", valueSize, rowCount);
                string jobfname = jobname + ".xml";
                string output = jobname + "_output.txt";
                string input = jobname + "_input.txt";
                long inputfilesize = (long)rowCount * (AELight.ParseLongCapacity(valueSize) + (long)Environment.NewLine.Length);

                alljobfiles.Add(jobfname);
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                    (@"<SourceCode>
  <Jobs>
    <Job Name=`" + jobname + @"_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace gen " + input + @" " + inputfilesize.ToString() + @"B " + valueSize + @"`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://" + input + @"</DFSInput>
        <DFSOutput>dfs://" + output + @"</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                byte[] bline = line.ToBytes();
                ByteSlice key = ByteSlice.Prepare(bline, 0, 100);
                output.Add(key, line);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Add(ByteSlice.Prepare(values[i].Value.Length.ToString()));                  
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`" + jobname + @"_Verification` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            if(DSpace_ExecArgs.Length > 0 && DSpace_ExecArgs[0].ToLower() == `v`)
            {
                string r = Shell(@`DSpace head " + output + @" 1`).Trim();
                if(r == `" + AELight.ParseLongCapacity(valueSize).ToString() + @"`)
                {
                    Shell(`DSpace exec regression_test_writePASSFAIL.xml " + jobfname + @" 1`);
                }
                else
                {
                    Shell(`DSpace exec regression_test_writePASSFAIL.xml " + jobfname + @" 0`);
                }
            }
            
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace del " + output + @"`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            }
            #endregion

            #region GenerateCacheFixedValueSize
            private static void GenerateCacheFixedValueSize(string valueSize, int rowCount, List<string> alljobfiles)
            {
                string jobname = string.Format("Cache_Value_Size_{0}_{1}_Of_{0}_Rows", valueSize, rowCount);
                string jobfname = jobname + ".xml";
                string output = jobname + "_output.txt";
                string input = jobname + "_input.txt";
                string cache = jobname + "_cache";
                long inputfilesize = (long)rowCount * (AELight.ParseLongCapacity(valueSize) + (long)Environment.NewLine.Length);

                alljobfiles.Add(jobfname);
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                    (@"<SourceCode>
  <Jobs>
    <Job Name=`" + jobname + @"_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + cache + @"`);
            Shell(@`DSpace gen " + input + @" " + inputfilesize.ToString() + @"B " + valueSize + @"`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <Delta>
        <Name>" + cache + @"</Name>
        <DFSInput>dfs://" + input + @"</DFSInput>
      </Delta>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput></DFSInput>
        <DFSOutput>dfs://" + output + @"</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                byte[] bline = line.ToBytes();
                ByteSlice key = ByteSlice.Prepare(bline, 0, 100);
                output.Add(key, line);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  output.Cache(key, values[i].Value);
                  output.Add(ByteSlice.Prepare(values[i].Value.Length.ToString()));                  
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`" + jobname + @"_Verification` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            if(DSpace_ExecArgs.Length > 0 && DSpace_ExecArgs[0].ToLower() == `v`)
            {
                string r = Shell(@`DSpace head " + output + @" 1`).Trim();
                if(r == `" + AELight.ParseLongCapacity(valueSize).ToString() + @"`)
                {
                    Shell(`DSpace exec regression_test_writePASSFAIL.xml " + jobfname + @" 1`);
                }
                else
                {
                    Shell(`DSpace exec regression_test_writePASSFAIL.xml " + jobfname + @" 0`);
                }
            }
            
            Shell(@`DSpace del " + input + @"`);
            Shell(@`DSpace del " + output + @"`);
            Shell(@`DSpace del " + cache + @"`);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            }
            #endregion

            #region GenerateValueSizeTestsDriver
            private static void GenerateValueSizeTestsDriver(List<string> alljobfiles)
            {
                string jobs = "";
                foreach (string job in alljobfiles)
                {
                    jobs += "jobs.Add(`" + job + "`);" + Environment.NewLine;
                }

                alljobfiles.Add("valueSizeTestsDriver.xml");
                AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
                AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
    (@"
<SourceCode>
  <Jobs>
    <Job Name=`valueSizeTestsDriver_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>test</JobType>
      </IOSettings>
      <Test>
        <![CDATA[
        public virtual void Test()
        {
            string mode = ``;
            if(DSpace_ExecArgs.Length > 0)
            {
                mode = DSpace_ExecArgs[0].ToLower();
            }
            
            List<string> jobs = new List<string>();
            " + jobs + @"            
            
            DSpace_Log(`Begin stress tests...`);
            DSpace_Log(`-`);
            foreach(string job in jobs)
            {                    
                DSpace_Log(`Qizmt exec ` + job + ` ` + mode);
                DSpace_Log(Shell(`Qizmt exec ` + job + ` ` + mode));
                DSpace_Log(`-`);
            }

            if(mode.ToLower() == `v`)
            {                
                foreach(string job in jobs)
                {                    
                    bool pass = false;
                    string[] info = Shell(@`DSpace info ` + job + `.PASSED`, true).Split('\n');

                    if(info.Length > 1)
                    {
                        if(info[1].IndexOf(`No such file`) == -1)
                        {
                            pass = true;
                        }
                    }
                    
                    if(pass)
                    {
                        DSpace_LogResult(`Qizmt exec ` + job, true);
                    }
                    else
                    {                        
                        DSpace_LogResult(`Qizmt exec ` + job, false);
                    } 
                    
                    Shell(@`DSpace del ` + job + `.FAILED`, true);
                    Shell(@`DSpace del ` + job + `.PASSED`, true);
                }
            }        
        }
        ]]>
      </Test>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));
                Console.WriteLine("    Qizmt exec {0}  [v = verify results]", alljobfiles[alljobfiles.Count - 1]);
            }
            #endregion

            #region GeneratePrereq
            private static void GeneratePrereq()
            {
                #region StressTests_lineCount
                AELight.DfsDelete("StressTests_lineCount.xml", false);
                AELight.DfsPutJobsFileContent("StressTests_lineCount.xml",
                    @"<SourceCode>
  <Jobs>
    <Job Name=`stressTests_lineCount_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del stressTests_lineCount_*`);
            Shell(@`DSpace rename ` + DSpace_ExecArgs[0] + ` stressTests_lineCount_Input.txt`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`stressTests_lineCount` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://stressTests_lineCount_Input.txt</DFSInput>
        <DFSOutput></DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          byte[] buf = new byte[4];
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                Entry.ToBytes(DSpace_ProcessID, buf, 0);
                output.Add(ByteSlice.Prepare(buf), ByteSlice.Prepare());
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              int processID = Entry.BytesToInt(key.ToBytes());
              int linecount = values.Length;
              mstring line = mstring.Prepare(processID);
              line = line.AppendM(':').AppendM(linecount);
              DSpace_Log(`processID:` + processID.ToString() + `:` + linecount.ToString());             
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`stressTests_lineCount_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {   
            Shell(@`DSpace rename stressTests_lineCount_Input.txt ` + DSpace_ExecArgs[0]);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
                #endregion

                #region StressTests_verifyValueSize
                AELight.DfsDelete("StressTests_verifyValueSize.xml", false);
                AELight.DfsPutJobsFileContent("StressTests_verifyValueSize.xml",
                    @"<SourceCode>
  <Jobs>
    <Job Name=`stressTests_verifyValueSize_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {            
            Shell(@`DSpace del stressTests_verifyValueSize_Input.txt`);            
            Shell(@`DSpace rename ` + DSpace_ExecArgs[1] + ` stressTests_verifyValueSize_Input.txt`);            
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`stressTests_verifyValueSize` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://stressTests_verifyValueSize_Input.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                int maxbc = Int32.Parse(DSpace_ExecArgs[2]);
                Dictionary<int,int> dic = new Dictionary<int,int>();
                
                StringBuilder sb = new StringBuilder();
                while(dfsinput.ReadLineAppend(sb))
                {
                    int bc = Int32.Parse(sb.ToString());
                    dic[bc] = 0;
                    sb.Length = 0;                    
                }
                
                List<int> sorted = new List<int>(dic.Keys);
                sorted.Sort();                
                
                if(sorted.Count >= maxbc && sorted[0] == 1 && sorted[maxbc-1] == maxbc)
                {
                    Shell(@`DSpace exec regression_test_writePASSFAIL.xml ` + DSpace_ExecArgs[0] + ` 1`);
                }
                else
                {
                    Shell(@`DSpace exec regression_test_writePASSFAIL.xml ` + DSpace_ExecArgs[0] + ` 0`);
                }
           }
        ]]>
      </Remote>
    </Job><Job Name=`stressTests_verifyValueSize_Postprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace rename  stressTests_verifyValueSize_Input.txt ` + DSpace_ExecArgs[1]); 
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>".Replace('`', '"'));
                #endregion
            }
            #endregion
        }
        #endregion       

        #region CriticalSectionTests
        public class CriticalSectionTests
        {
            public static void GenerateTests()
            {
                string jobname = "critical_section_3GB_Of_100_Byte_Rows";
                string jobfname = jobname + ".xml";
                string hostname = System.Net.Dns.GetHostName();

                AELight.DfsDelete(jobfname, false);
                AELight.DfsPutJobsFileContent(jobfname,
    (@"<SourceCode>
  <Jobs>
    <Job Name=`" + jobname + @"Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`DSpace del " + jobname + @"_Input.txt`);
            Shell(@`DSpace del " + jobname + @"_Output.txt`);
            Shell(@`DSpace asciigen " + jobname + @"_Input.txt 3GB 100B`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`" + jobname + @"` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://" + jobname + @"_Input.txt</DFSInput>
        <DFSOutput>dfs://" + jobname + @"_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              output.Add(line, line);
          }
        ]]>
        </Map>
        <ReduceInitialize>
          <![CDATA[
          string xxx = Guid.NewGuid().ToString();
          public void ReduceInitialize()
          { 
                using(GlobalCriticalSection.GetLock())
                {                    
                    if(!System.IO.Directory.Exists(@`\\" + hostname + @"\c$\temp`))
                    {
                       System.IO.Directory.CreateDirectory(@`\\" + hostname + @"\c$\temp`);
                    }                    
                    if (!System.IO.File.Exists(@`\\" + hostname + @"\c$\temp\index.txt`))
                    {
                        System.IO.FileStream fs = System.IO.File.Create(@`\\" + hostname + @"\c$\temp\index.txt`);
                        fs.Close();
                    }

                    System.IO.FileStream w = new System.IO.FileStream(@`\\" + hostname + @"\c$\temp\index.txt`, System.IO.FileMode.Append, System.IO.FileAccess.Write, System.IO.FileShare.None);
                    string ss = `************start***********` + xxx + Environment.NewLine;
                    byte[] buf = System.Text.Encoding.UTF8.GetBytes(ss);
                    w.Write(buf, 0, buf.Length);
                    w.Close();
                }
        }
        
        ]]>
        </ReduceInitialize>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {             
              for(int i = 0; i < values.Length; i++)
              {
                  output.Add(key);            
              }
          }
        ]]>
        </Reduce>
        <ReduceFinalize>
          <![CDATA[
          public void ReduceFinalize()
          {
               using(GlobalCriticalSection.GetLock())                
                {
                    System.IO.FileStream w = new System.IO.FileStream(@`\\" + hostname + @"\c$\temp\index.txt`, System.IO.FileMode.Append, System.IO.FileAccess.Write, System.IO.FileShare.None);
                    string ss = `************end***********` + xxx + Environment.NewLine;
                    byte[] buf = System.Text.Encoding.UTF8.GetBytes(ss);
                    w.Write(buf, 0, buf.Length);
                    w.Close();
                }
        }       
        ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>").Replace('`', '"'));

                Console.WriteLine("Qizmt exec " + jobfname);
            }
        }
        #endregion
    }
}