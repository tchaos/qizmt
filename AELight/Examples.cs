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
    public partial class Examples
    {  
        public static void Generate()
        {
            try
            {
                System.IO.Directory.CreateDirectory(@"C:\temp");
            }
            catch
            {
            }

            List<string> alljobfiles = new List<string>();

            #region GroupBy
            alljobfiles.Add(@"Qizmt-GroupBy.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<?xml version=`1.0` encoding=`utf-8` ?>
<!--     Narrative:
            Get words from text document(s) and group the same ones together.
  -->
<SourceCode>
  <Jobs>
    <Job Name=`groupBy_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del groupBy_input.txt`,true);
            Shell(@`Qizmt del groupBy_output.txt`,true);            
        }
        ]]>
      </Local>
    </Job>
    <Job description=`Load sample data` Name=`groupBy_LoadData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://groupBy_input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
             dfsoutput.WriteLine(@`Create a community on MySpace and you can share photos, journals and interests with your growing network of mutual  friends!

See who knows who, or how you are connected. Find out if you really are six people away from Kevin Bacon.

MySpace is for everyone:
Friends who want to talk Online 
Single people who want to meet other Singles 
Matchmakers who want to connect their friends with other friends 
Families who want to keep in touch--map your Family Tree 
Business people and co-workers interested in networking 
Classmates and study partners 
Anyone looking for long lost friends!


MySpace makes it easy to express yourself, connect with friends and make new ones, but please remember that what you  post publicly can be read by anyone viewing your profile, so we suggest you consider the following guidelines when  using MySpace: 
Don't forget that your profile and MySpace forums are public spaces. Don't post anything you wouldn't want the world  to know (e.g., your phone number, address, IM screens name, or specific whereabouts). Avoid posting anything that  would make it easy for a stranger to find you, such as where you hang out every day after school. 
People aren't always who they say they are. Be careful about adding people you don't know in the physical world to  your friends list. It's fun to connect with new MySpace friends from all over the world, but avoid meeting people in  person whom you do not already know in the physical world. If you decide to meet someone you've met on MySpace, tell  your parents first, do it in a public place and bring a trusted adult. 
Harassment, hate speech and inappropriate content should be reported. If you feel someone's behavior is  inappropriate, react. Talk with a trusted adult, or report it to MySpace or the authorities.
Don't post anything that would embarrass you later. It's easy to think that only people you know are looking at your  MySpace page, but the truth is that everyone can see it. Think twice before posting a photo or information you  wouldn't want others to see, including potential employers or colleges!
Do not lie about your age.  Your profile may be deleted and your Membership may be terminated without warning if we  believe that you are under 14 years of age or if we believe you are 14 through 17 years of age and you represent  yourself as 18 or older.       
Don't get hooked by a phishing scam.  Phishing is a method used by fraudsters to try to get your personal  information, such as your username and password, by pretending to be a site you trust.`);
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`groupBy` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>16</KeyLength>
        <DFSInput>dfs://groupBy_Input.txt</DFSInput>
        <DFSOutput>dfs://groupBy_Output.txt</DFSOutput>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              mstring sLine = mstring.Prepare(line);
              mstringarray words = sLine.MSplit(' ');
              
              for(int i=0; i < words.Length; i++)
              {
                  mstring s = words[i];
                  
                  if(s.Length > 0 && s.Length <= 16)
                  {
                      output.Add(s, mstring.Prepare());
                  }
              }
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                for(int i = 0; i < values.Length; i++)
                {
                    output.Add(mstring.Prepare(UnpadKey(key)));
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region WordCountByPartialReduce
            alljobfiles.Add(@"Qizmt-WordCountByPartialReduce.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<?xml version=`1.0` encoding=`utf-8`?>
<SourceCode>
  <Jobs>
    <Job Name=`Cleanup` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del WordCountByPartialReduce_*`,true); // Clean previous run.            
        }
        ]]>
      </Local>
    </Job>
    <Job description=`Load sample data` Name=`` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://WordCountByPartialReduce_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
             dfsoutput.WriteLine(@`Create a community on MySpace and you can share photos, journals and interests with your growing network of mutual  friends!

See who knows who, or how you are connected. Find out if you really are six people away from Kevin Bacon.

MySpace is for everyone:
Friends who want to talk Online 
Single people who want to meet other Singles 
Matchmakers who want to connect their friends with other friends 
Families who want to keep in touch--map your Family Tree 
Business people and co-workers interested in networking 
Classmates and study partners 
Anyone looking for long lost friends!


MySpace makes it easy to express yourself, connect with friends and make new ones, but please remember that what you  post publicly can be read by anyone viewing your profile, so we suggest you consider the following guidelines when  using MySpace: 
Don't forget that your profile and MySpace forums are public spaces. Don't post anything you wouldn't want the world  to know (e.g., your phone number, address, IM screens name, or specific whereabouts). Avoid posting anything that  would make it easy for a stranger to find you, such as where you hang out every day after school. 
People aren't always who they say they are. Be careful about adding people you don't know in the physical world to  your friends list. It's fun to connect with new MySpace friends from all over the world, but avoid meeting people in  person whom you do not already know in the physical world. If you decide to meet someone you've met on MySpace, tell  your parents first, do it in a public place and bring a trusted adult. 
Harassment, hate speech and inappropriate content should be reported. If you feel someone's behavior is  inappropriate, react. Talk with a trusted adult, or report it to MySpace or the authorities.
Don't post anything that would embarrass you later. It's easy to think that only people you know are looking at your  MySpace page, but the truth is that everyone can see it. Think twice before posting a photo or information you  wouldn't want others to see, including potential employers or colleges!
Do not lie about your age.  Your profile may be deleted and your Membership may be terminated without warning if we  believe that you are under 14 years of age or if we believe you are 14 through 17 years of age and you represent  yourself as 18 or older.       
Don't get hooked by a phishing scam.  Phishing is a method used by fraudsters to try to get your personal  information, such as your username and password, by pretending to be a site you trust.`);
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`WordCountByPartialReduce` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>16</KeyLength>
        <DFSInput>dfs://WordCountByPartialReduce_Input.txt</DFSInput>
        <DFSOutput>dfs://WordCountByPartialReduce_Output.txt</DFSOutput>
      </IOSettings>
      <PartialReduce/>        
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              mstring sLine= mstring.Prepare(line);
              mstringarray parts = sLine.SplitM(' ');
              
             for(int i=0; i < parts.Length; i++)
             {
                    mstring word = parts[i];
                    
                    if(word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
                    {                        
                        recordset rs = recordset.Prepare();
                        rs.PutInt(1);
                        output.Add(word.ToLowerM(), rs); 
                    }                                 
             }
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[         
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              int totalCount = 0;
              
              for(int i = 0; i < values.Length; i++)
              {
                  recordset rs = recordset.Prepare(values[i].Value);
                  int count = rs.GetInt();
                  totalCount += count;
              }
              
              if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
              {
                  recordset rs = recordset.Prepare();
                  rs.PutInt(totalCount);
                  output.Add(key, rs.ToByteSlice());
              }
              else
              {
                  mstring outline = mstring.Prepare(UnpadKey(key));
                  outline = outline.AppendM(',').AppendM(totalCount);
                  output.Add(outline);
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region WordCountByOther
            alljobfiles.Add(@"Qizmt-WordCountByOther.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<?xml version=`1.0` encoding=`utf-8`?>
<!--
    Narrative:
        Word Count parses out all the words from the input files and writes out how many times the same words occur.
-->
<SourceCode>
  <Jobs>
    <Job Name=`PrepJob` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del WordCount_*`,true); // Clean previous run.            
        }
        ]]>
      </Local>
    </Job>
    <Job description=`Load sample data` Name=`wordCount_LoadData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://WordCount_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
             dfsoutput.WriteLine(@`Create a community on MySpace and you can share photos, journals and interests with your growing network of mutual  friends!

See who knows who, or how you are connected. Find out if you really are six people away from Kevin Bacon.

MySpace is for everyone:
Friends who want to talk Online 
Single people who want to meet other Singles 
Matchmakers who want to connect their friends with other friends 
Families who want to keep in touch--map your Family Tree 
Business people and co-workers interested in networking 
Classmates and study partners 
Anyone looking for long lost friends!


MySpace makes it easy to express yourself, connect with friends and make new ones, but please remember that what you  post publicly can be read by anyone viewing your profile, so we suggest you consider the following guidelines when  using MySpace: 
Don't forget that your profile and MySpace forums are public spaces. Don't post anything you wouldn't want the world  to know (e.g., your phone number, address, IM screens name, or specific whereabouts). Avoid posting anything that  would make it easy for a stranger to find you, such as where you hang out every day after school. 
People aren't always who they say they are. Be careful about adding people you don't know in the physical world to  your friends list. It's fun to connect with new MySpace friends from all over the world, but avoid meeting people in  person whom you do not already know in the physical world. If you decide to meet someone you've met on MySpace, tell  your parents first, do it in a public place and bring a trusted adult. 
Harassment, hate speech and inappropriate content should be reported. If you feel someone's behavior is  inappropriate, react. Talk with a trusted adult, or report it to MySpace or the authorities.
Don't post anything that would embarrass you later. It's easy to think that only people you know are looking at your  MySpace page, but the truth is that everyone can see it. Think twice before posting a photo or information you  wouldn't want others to see, including potential employers or colleges!
Do not lie about your age.  Your profile may be deleted and your Membership may be terminated without warning if we  believe that you are under 14 years of age or if we believe you are 14 through 17 years of age and you represent  yourself as 18 or older.       
Don't get hooked by a phishing scam.  Phishing is a method used by fraudsters to try to get your personal  information, such as your username and password, by pretending to be a site you trust.`);
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`WordCount` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>16</KeyLength>
        <DFSInput>dfs://WordCount_Input.txt</DFSInput>
        <DFSOutput>dfs://WordCount_Output.txt</DFSOutput>
      </IOSettings>
      <FaultTolerantExecution>
        <Mode>enabled</Mode> <!-- enabled or disabled -->
      </FaultTolerantExecution>
      <!--
          No limit to intermediate data collisions and allows up to 1GB of keys and values per reduce.
      -->
      <IntermediateDataAddressing>64</IntermediateDataAddressing>
        
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              mstring sLine= mstring.Prepare(line);
              mstringarray parts = sLine.SplitM(' ');
              
              for(int i=0; i < parts.Length; i++)
             {
                    mstring word = parts[i];
                    
                    if(word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
                    {                        
                        output.Add(word.ToLowerM(), mstring.Prepare(1)); 
                    }                                 
             }
          }
        ]]>
        </Map>

        <Reduce>
          <![CDATA[
          
          bool HasSaved = false;
          List<byte> SavedKey = new List<byte>();
          int SavedCount = 0; // Init.
          
          void HandleSaved(ReduceOutput output)
          {
                if(HasSaved)
                {
                    mstring sLine = mstring.Prepare(ByteSlice.Prepare(SavedKey));
                    sLine = sLine.AppendM(',').AppendM(SavedCount);              
                    output.Add(sLine);
                    HasSaved = false;
                    SavedCount = 0; // Reset.
                }
          }
          
          [KeyRepeatedEnabled]
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              if(!Qizmt_KeyRepeated)
              {
                  HandleSaved(output);
              }
              
              SavedKey.Clear();
              UnpadKey(key).AppendTo(SavedKey);
              SavedCount += values.Length;
              HasSaved = true;
              
              if(StaticGlobals.Qizmt_Last)
              {
                  HandleSaved(output);
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion            

            #region ExecXPath
            alljobfiles.Add(@"Qizmt-ExecXPath.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`ExecXPath_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Qizmt_Log(`Execute other example Qizmt-WordCount.xml`);
            Qizmt_Log(`    Sets new final output file name`);
            Qizmt_Log(`    Sets larger KeyLength`);
            Qizmt_Log(`    Adds a KeyMajor`);
            string output = Shell(@`Qizmt exec ``//Job[@Name='WordCount']/IOSettings/DFSOutput=WordCount_New-Output-Name.txt`` ``//Job[@Name='WordCount']/IOSettings/KeyLength=32`` ``//Job[@Name='WordCount']/IOSettings/KeyMajor=8`` Qizmt-WordCount.xml`);
            Qizmt_Log(`Output from WordCount example with XPath updates:`);
            Qizmt_Log(output);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region CellularKMeans
            alljobfiles.Add(@"Qizmt-CellularKMeans.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`CellularKMeans_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del CellularKMeans_*`, true);
        }
        ]]>
      </Local>
    </Job>
    <Job description=`Create sample data` Name=`CellularKMeans_CreateSampleData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSWriter>dfs://CellularKMeans_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {              
              int sampleSize = 200;
              //Number of centroids.
              int k = 50;
              Random rnd = new Random();  
              
              //Random data.
              for(int i=0; i < sampleSize; i++)                
                  dfsoutput.WriteLine(`V:` + rnd.NextDouble().ToString() + `,` + rnd.NextDouble().ToString());              
                           
              //Initial centroids.
              for(int i=0; i < k; i++)            
                  dfsoutput.WriteLine(`C:` + rnd.NextDouble().ToString() + `,` + rnd.NextDouble().ToString());  
                  
             //Clustered data.
             //Number of clusters to create.
             int clusterCount = 100;    
             double xCenter = 0;
             double yCenter = 0;
             double radius = 0.03;
             int density = 40;
             int clusterSize = 25;
             double x = 0;
             double y = 0;
             
             for(int i=0; i < clusterCount; i++)
             {
                 //Cluster center point.
                 xCenter = rnd.NextDouble();
                 yCenter = rnd.NextDouble();
                 
                 for(int j=0; j < clusterSize; j++)
                 {
                     x = xCenter + (rnd.Next(0-density, density) * radius) / (double)density;
                     y = yCenter + (rnd.Next(0-density, density) * radius) / (double)density;
                     
                     if(x >= 0 && x <= 1.0 && y >= 0 && y<= 1.0)
                         dfsoutput.WriteLine(`V:` + x.ToString() + `,` + y.ToString());              
                 }                
             }   
          }
        ]]>
      </Remote>
    </Job>
    <Job Name=`CellularKMeans` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>double,double</KeyLength>
        <DFSInput>dfs://CellularKMeans_Input.txt</DFSInput>
        <DFSOutput>dfs://CellularKMeans_Output.txt</DFSOutput>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              mstring sLine = mstring.Prepare(line);
              mstring sCoords = sLine.MSubstring(2);
              
              double x = sCoords.NextItemToDouble(',');
              double y = sCoords.NextItemToDouble(',');
              
              double kx = x * (double)5;
              double ky = y * (double)5; 
              
              kx = Math.Truncate(kx);
              ky = Math.Truncate(ky);
              
              recordset rKey = recordset.Prepare();
              rKey.PutDouble(kx);
              rKey.PutDouble(ky);
              
              output.Add(rKey, sLine);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              recordset rKey = recordset.Prepare(key);
              mstring sKey = mstring.Prepare(`K:`);
              sKey = sKey.AppendM(rKey.GetDouble()).AppendM(',').AppendM(rKey.GetDouble());
              
              output.Add(sKey);                        
            
              for(int i = 0; i < values.Length; i++)   
              {                 
                  mstring sValue = mstring.Prepare(values.Items[i]);
                  output.Add(sValue);
              }
              
              List<Point> data = new List<Point>();
              List<Point> centroids = new List<Point>();
              
              mstring V = mstring.Prepare(`V:`);
              
              for(int i = 0; i < values.Length; i++)
              {
                  mstring sValue = mstring.Prepare(values.Items[i]);
                  mstring label = sValue.MSubstring(0,2);
                  mstring coord = sValue.MSubstring(2);                      
                  
                  double x = coord.NextItemToDouble(',');
                  double y = coord.NextItemToDouble(',');
                  Point pt = Point.Create(x, y);
                  
                  if(label == V)
                  {
                      data.Add(pt);
                  }
                  else
                  {
                      centroids.Add(pt);
                  }                    
              }

              //If there is any centroid in this partition
              if(centroids.Count > 0)
              {
                  foreach(Point c in centroids)
                  {
                      mstring ms = mstring.Prepare(`I:`);
                      ms.MAppend(c.ToMString());
                      output.Add(ms);
                  }                     
                     
                  Point[] newCentroids = centroids.ToArray();
                     
                  CalculateKMeans(newCentroids, data.ToArray());
                   
                  foreach(Point c in newCentroids)
                  {
                      mstring ms = mstring.Prepare(`F:`);
                      ms.MAppend(c.ToMString());
                      output.Add(ms);
                  }
              }
           }
          
            public struct Point
            {
                public double X;
                public double Y;

                public static Point Create(double x, double y)
                {
                    Point p;
                    p.X = x;
                    p.Y = y;
                    return p;
                }

                public static Point Create(string s)
                {
                    string[] coords = s.Split(new Char[] { ',' });
                    Point p;
                    p.X = Convert.ToDouble(coords[0]);
                    p.Y = Convert.ToDouble(coords[1]);
                    return p;
                }			

                public static double GetDistance(Point p1, Point p2)
                {
                    return Math.Sqrt(Math.Pow((p1.X - p2.X), 2) + Math.Pow((p1.Y - p2.Y), 2));
                }

                public override string ToString()
                {
                    return string.Format(`{0},{1}`, X, Y);
                }

		public mstring ToMString()
                {
                    mstring ms = mstring.Prepare(X);
                    ms.MAppend(',');
                    ms.MAppend(Y);
                    return ms;                    
                }
            }


            public void CalculateKMeans(Point[] centroids, Point[] data)
            {
                int k = centroids.Length;			
                bool running = true;

                while (running)
                {
                    double[] sumX = new double[k];
                    double[] sumY = new double[k];
                    int[] count = new int[k];

                    foreach (Point d in data)
                    {
                        double min = 0;
                        int cID = 0;

                         //Find the centroid nearest to the data.
                        for (int i = 0; i < k; i++)
                        {                           
                            Point c = centroids[i];
                            double e = Point.GetDistance(d, c);
						
                            if (i == 0)
                            {
                                min = e;
                            }
                            else
                                if (e < min)
	                      {
                                    min = e;
		                 cID = i;
                                }
                        }

                        sumX[cID] += d.X;
                        sumY[cID] += d.Y;
                        count[cID]++;
                    }

                    running = false;
                    Point newCentroid;

                    for (int i = 0; i < k; i++)
                    {
                        if (count[i] > 0)
                        {
                            //re-calculate centroids.
                            newCentroid = Point.Create(sumX[i] / (double)count[i], sumY[i] / (double)count[i]);

                            //If centroid has moved, keep running.
                            if (newCentroid.X != centroids[i].X || newCentroid.Y != centroids[i].Y)
                            {
                                centroids[i] = newCentroid;
                                running = true;
                            }
                        }    
                    }
                }
            }
		
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region SortInt
            alljobfiles.Add(@"Qizmt-SortInt.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`sortInt_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <DynamicFoil/>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del sortInt_*`,true);
        }
        ]]>
      </Local>
    </Job>
    <Job description=`Load sample data` Name=`sortInt_LoadData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://sortInt_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
             dfsoutput.WriteLine(`200`);
             dfsoutput.WriteLine(`-200`);
             dfsoutput.WriteLine(`3`);
             dfsoutput.WriteLine(`-99`);
            dfsoutput.WriteLine(`-1`);
             dfsoutput.WriteLine(`-300`);
             dfsoutput.WriteLine(`20`);
             dfsoutput.WriteLine(`6`);
             dfsoutput.WriteLine(int.MaxValue.ToString());
             dfsoutput.WriteLine(int.MinValue.ToString());
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`sortInt` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>4</KeyLength>
        <OutputMethod>sorted</OutputMethod>
        <DFSInput>dfs://sortInt_Input.txt</DFSInput>
        <DFSOutput>dfs://sortInt_Output.txt</DFSOutput>
      </IOSettings>
      <DynamicFoil/>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              mstring sLine = mstring.Prepare(line);             
              uint i = Entry.ToUInt32(sLine.ToInt());
              byte[] buffer = Entry.UInt32ToBytes(i);
              output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
          }                
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
             int num = Entry.ToInt32(Entry.BytesToUInt32(key.ToBytes()));
              
             output.Add(mstring.Prepare(num));
          }          
            
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region piEstimator
            alljobfiles.Add(@"Qizmt-piEstimator.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"
<SourceCode>
  <Jobs>
    <Job Name=`piEst_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {   
            Shell(@`Qizmt del piEst_input.txt`,true);
            Shell(@`Qizmt del piEst_output.txt`,true);
            Shell(@`Qizmt del piEst_output2.txt`,true);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`piEst_LoadData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://piEst_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
             Random rnd = new Random();
            int size = 10000;
            double x = 0;
            double y = 0;
            for (int i = 0; i < size; i++)
            {
                x = rnd.NextDouble();
                y = rnd.NextDouble();
                dfsoutput.WriteLine(x.ToString() + `,` + y.ToString());
            }
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`piEst` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://piEst_Input.txt</DFSInput>
        <DFSOutput>dfs://piEst_Output.txt</DFSOutput>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                mstring sLine = mstring.Prepare(line);
              
                double x = sLine.NextItemToDouble(',');
                double y = sLine.NextItemToDouble(',');                
                double d = Math.Sqrt(x * x + y * y);

                recordset rKey = recordset.Prepare();
                recordset rVal = recordset.Prepare();
                
                if(d <= 1)
                {
                    rKey.PutInt(1);                        
                }
                else
                {
                    rKey.PutInt(0);
                }
                
                output.Add(rKey, rVal); 
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
                public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
                {
                    recordset rKey = recordset.Prepare(key);
                    int x = rKey.GetInt();

                    if(x == 1)
                    {
                        int hitCount = values.Length;
                        mstring line = mstring.Prepare(`h=`);
                        line.AppendM(hitCount);
                        output.Add(line);
                    }
                    else
                    {
                        int missedCount = values.Length;
                        mstring line = mstring.Prepare(`m=`);
                        line.AppendM(missedCount);                    
                        output.Add(line);
                    }                    
                }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`piEst_LoadData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://piEst_Output.txt</DFSReader>
          <DFSWriter>dfs://piEst_Output2.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                StringBuilder sb = new StringBuilder();
                int hit = 0;
                int missed = 0;
                
                while(dfsinput.ReadLineAppend(sb))
                {
                    string line = sb.ToString();
                    sb.Length = 0;
                    
                    if(line.StartsWith(`h`))
                    {
                        hit = Int32.Parse(line.Substring(2));
                    }
                    else if(line.StartsWith(`m`))
                    {
                        missed = Int32.Parse(line.Substring(2));   
                    } 
                }
                
                double pi = (double)hit * 4.0 / (double)(hit+missed);
                double diff = Math.Abs(Math.PI - pi);
                              
                dfsoutput.WriteLine(`Estimated PI = ` + pi.ToString()); // PI estimate.
                dfsoutput.WriteLine(`HitCount = ` + hit.ToString());
                dfsoutput.WriteLine(`MissedCount = ` + missed.ToString());

                if(diff > 0.01)
                {
                    dfsoutput.WriteLine(`Your estimated PI is too far off.  Difference = ` + diff.ToString() + `.  Please increase your sample size.`);
                }
                
                Qizmt_Log(`Estimated PI = ` + pi.ToString());
           }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            
            #region Pointers
            alljobfiles.Add(@"Qizmt-Pointers.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`PointerTest_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del PointerTest_Input.txt`);
            Shell(@`Qizmt del PointerTest_Output.txt`);
        }
        ]]>
      </Local>
    </Job>
    <Job description=`Create sample data` Name=`PointerTest_CreateSampleData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://PointerTest_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`The quick brown fox jumped over the lazy dog.`);   
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`PointerTest_CopyReverse` Custodian=`` email=``>
      <Unsafe/>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>1</KeyLength>
        <DFSInput>dfs://PointerTest_Input.txt</DFSInput>
        <DFSOutput>dfs://PointerTest_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          List<byte> mapbuf = new List<byte>();
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                unsafe
                {
                    const int BUF_MAX_LENGTH = 1024;
                    
                    if(line.Length <= BUF_MAX_LENGTH)
                    {
                        int buflen = line.Length;
                        byte* buf = stackalloc byte[buflen];
                        
                        for(int i = 0; i < buflen; i++)
                        {
                            buf[i] = line[line.Length - i - 1]; // Reverse.
                        }
                        
                        mapbuf.Clear();
                        for(int i = 0; i < buflen; i++)
                        {
                            mapbuf.Add(buf[i]);
                        }
                        
                        output.Add(ByteSlice.Prepare(line, 0, 1), line); // Output normal...
                        output.Add(ByteSlice.Prepare(line, 0, 1), ByteSlice.Prepare(mapbuf)); // ... and output reversed.
                    }
                }
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          List<byte> reducebuf = new List<byte>();
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              while(values.MoveNext())
              {
                 unsafe
                 {
                     const int BUF_MAX_LENGTH = 1024;
                     if(values.Current.Length <= BUF_MAX_LENGTH)
                     {
                         int buflen = values.Current.Length;
                         byte* buf = stackalloc byte[buflen];
                        
                         for(int i = 0; i < buflen; i++)
                         {
                            buf[i] = values.Current[i];
                         }
                         
                         reducebuf.Clear();
                         for(int i = 0; i < buflen; i++)
                         {
                             reducebuf.Add(buf[i]);
                         }
                         
                         output.Add(ByteSlice.Prepare(reducebuf));
                     }
                 }
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job description=`Display output data` Name=`PointerTest_DisplayOutputData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://PointerTest_Output.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {    
                //Display output.
                byte[] buf = new byte[500];
                dfsinput.Read(buf, 0, 500);
                Qizmt_Log(`Output:`);
                Qizmt_Log(System.Text.Encoding.UTF8.GetString(buf));            
           }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region Linq
            alljobfiles.Add(@"Qizmt-Linq.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`Qizmt-Linq_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del Qizmt-Linq_Input.txt`);
            Shell(@`Qizmt del Qizmt-Linq_Output.txt`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Qizmt-Linq_CreateSampleData` Custodian=`` email=``>      
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-Linq_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                Random rnd = new Random();
                
                for(int i= 0; i < 60; i++)
                {
                    string line = ``;
                    
                    for(int j = 0; j < 60; j++)
                    {
                        line += rnd.Next(1, 100).ToString() + ` `;                        
                    }   
                    
                    dfsoutput.WriteLine(line.Trim());
                } 
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-Linq` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://Qizmt-Linq_Input.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-Linq_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <DynamicFoil/>
      <MapReduce>
        <Map>
          <![CDATA[
          List<int> nums = new List<int>();          
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                mstring sLine = mstring.Prepare(line);
                nums.Clear();
                
                while(sLine.HasNextItem())
                {
                        nums.Add(sLine.NextItemToInt32(' '));                    
                }
                               
                IEnumerable<int> evenNums =
                from n in nums
                where (n % 2) == 0
                select n;                
                
                recordset key = recordset.Prepare();
                key.PutInt(evenNums.Count());
                
                recordset value = recordset.Prepare();
                
                foreach(int n in evenNums)
                {
                    value.PutInt(n);
                }
                
                output.Add(key, value);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          List<int> nums = new List<int>();
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              recordset rKey = recordset.Prepare(key);
              int numCount = rKey.GetInt();
              
              for(int i = 0; i < values.Length; i++)
              {
                  nums.Clear();
                  recordset value = recordset.Prepare(values.Items[i]);
                  
                  for(int j = 0; j < numCount; j++)
                  {
                        nums.Add(value.GetInt());                      
                  }
                  
                  IEnumerable<int> distinctNums = nums.Distinct();
                  
                  int min = distinctNums.Min();
                  int max = distinctNums.Max();
                  double avg = distinctNums.Average();
                  int sum = distinctNums.Aggregate(0, (total, next) => next > 50 ? total + next : total);
                  
                  mstring line = mstring.Prepare(`Min: `);
                  line = line.AppendM(min)
                  .AppendM(`  Max: `)
                  .AppendM(max)
                  .AppendM(`  Sum over 50: `)
                  .AppendM(sum)
                  .AppendM(`  Avg: `)
                  .AppendM(avg);
                  
                  output.Add(line);                 
              }              
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>    
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region Geometry
            alljobfiles.Add(@"Qizmt-Geometry.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`Geometry_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt dfs del dfs://Qizmt-Geometry_Input.txt`, true);
            Shell(@`Qizmt dfs del dfs://Qizmt-Geometry_Output.txt`, true);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Geometry_LoadData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-Geometry_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                dfsoutput.WriteLine(`Triangle,2,3,3,0`);
                dfsoutput.WriteLine(`Square,2,4,4,4`);
                dfsoutput.WriteLine(`Cube,3,12,8,24`);
                dfsoutput.WriteLine(`Tesseract,4,32,16,64`);
                dfsoutput.WriteLine(`Line,1,1,0,0`);
                dfsoutput.WriteLine(`Circle,2,0,0,0`);
                dfsoutput.WriteLine(`Sphere,3,0,0,0`);
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Geometry` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int,long,int</KeyLength>
        <DFSInput>dfs://Qizmt-Geometry_Input.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-Geometry_Output.txt</DFSOutput>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              // Parse row.
              mstring sline = mstring.Prepare(line);
              mstring shape = sline.NextItemToString(',');
              shape = shape.MToUpper();
              int dimensionality = sline.NextItemToInt32(',');
              int edges = sline.NextItemToInt32(',');
              int corners = sline.NextItemToInt32(',');
              long rightangles = sline.NextItemToInt64(',');
              
              // Identify intermediate key/value pairs.
              recordset key = recordset.Prepare();
              key.PutInt32(dimensionality);
              key.PutInt64(rightangles);
              key.PutInt32(edges);
              recordset value = recordset.Prepare();
              value.PutString(shape);
              value.PutInt32(corners);
              output.Add(key, value);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              recordset rKey = recordset.Prepare(key);
              int dimensionality = rKey.GetInt32();
              long rightangles = rKey.GetInt64();
              int edges = rKey.GetInt32();
              for(int i = 0; i < values.Length; i++)
              {
                  recordset rValue = recordset.Prepare(values.Items[i]);
                  mstring shape = rValue.GetString();
                  int corners = rValue.GetInt32();
                  mstring line = mstring.Prepare();
                  line = line.MAppend(dimensionality)
                      .MAppend(',')
                      .MAppend(rightangles)
                      .MAppend(',')
                      .MAppend(edges)
                      .MAppend(',')
                      .MAppend(shape)
                      .MAppend(',')
                      .MAppend(corners);
                  output.Add(line);
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region CollaborativeFiltering
            alljobfiles.Add(@"Qizmt-CollaborativeFiltering.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
            @"<SourceCode>
  <Jobs>
    <Job Name=`CollaborativeFiltering_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(`Qizmt del dfs://CollaborativeFiltering_*`, true); // Suppress errors.
            
            {
                const string fn = `CollaborativeFiltering_Input001.txt`;
                string fpath = IOUtils.GetTempDirectory() + @`\` + fn;
                using(System.IO.StreamWriter sr = new System.IO.StreamWriter(System.IO.File.Open(fpath, System.IO.FileMode.Create)))
                {
                    sr.WriteLine(`2,33`);
                    sr.WriteLine(`2,4`);
                    sr.WriteLine(`2,66`);
                    sr.WriteLine(`2,77`);
                    sr.WriteLine(`2,55`);
                    sr.WriteLine(`3,55`);
                    sr.WriteLine(`1,11`);
                    sr.WriteLine(`1,2`);
                    sr.WriteLine(`1,3`);
                    sr.WriteLine(`1,33`);
                    sr.WriteLine(`1,4`);
                    sr.WriteLine(`1,6`);
                    sr.WriteLine(`1,8`);
                    sr.WriteLine(`2,11`);
                    sr.WriteLine(`2,2`);
                    sr.WriteLine(`6,42`);
                    sr.WriteLine(`3,6`);
                    sr.WriteLine(`3,66`);
                    sr.WriteLine(`3,77`);
                    sr.WriteLine(`3,8`);
                    sr.WriteLine(`4,55`);
                    sr.WriteLine(`6,55`);
                }
                Shell(`Qizmt.exe -dfs put \`` + fpath + `\``);
            }
            
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`CollaborativeFiltering` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://CollaborativeFiltering_Input001.txt</DFSReader>
          <DFSWriter>dfs://CollaborativeFiltering_Output_Part1.txt</DFSWriter>
        </DFS_IO>
        <DFS_IO>
          <DFSReader>dfs://CollaborativeFiltering_Input001.txt</DFSReader>
          <DFSWriter>dfs://CollaborativeFiltering_Output_Part2.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {
              List<byte> buffer = new List<byte>();
              
              MySpace.DataMining.CollaborativeFilteringObjects3.RectangularFriendsLists rfl = new MySpace.DataMining.CollaborativeFilteringObjects3.RectangularFriendsLists();
              rfl.AddUserIDRange(0, 20);
              rfl.AddUserIDRange(21, 9999);
              rfl.SetMaxUserCount(200);
              rfl.MaxFriends = 28;
              rfl.MinimumScores = 1; // Minimum scores cutoff; inclusive (1 means scores >= 1).
              rfl.Load(0x400 * 0x400 * 20, dfsinput);
              rfl.AddOutputRange(0, 2147480000, dfsoutput);
              rfl.ToFofScores(Qizmt_ProcessCount, new int[] { Qizmt_ProcessID }, null);
           }
        ]]>
      </Remote>
    </Job>
    <!--<Job Name=`CollaborativeFiltering_Postprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            //Shell(@`Qizmt ls`);
        }
        ]]>
      </Local>
    </Job>-->
  </Jobs>
</SourceCode>
            ".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region Grep
            alljobfiles.Add(@"Qizmt-Grep.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<?xml version=`1.0` encoding=`utf-8`?>
<!--
    Narrative:
        Grep parses out all the sentences from the input files and writes out which ones contain a certain string.
-->
<SourceCode>
  <Jobs>
    <Job Name=`TestPrepJob` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del Grep*put.txt`, true); 
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Grep_LoadData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://GrepInput.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
             dfsoutput.WriteLine(@`Create a community on MySpace and you can share photos, journals and interests with your growing network of mutual  friends!

See who knows who, or how you are connected. Find out if you really are six people away from Kevin Bacon.

MySpace is for everyone:
Friends who want to talk Online 
Single people who want to meet other Singles 
Matchmakers who want to connect their friends with other friends 
Families who want to keep in touch--map your Family Tree 
Business people and co-workers interested in networking 
Classmates and study partners 
Anyone looking for long lost friends!


MySpace makes it easy to express yourself, connect with friends and make new ones, but please remember that what you  post publicly can be read by anyone viewing your profile, so we suggest you consider the following guidelines when  using MySpace: 
Don't forget that your profile and MySpace forums are public spaces. Don't post anything you wouldn't want the world  to know (e.g., your phone number, address, IM screens name, or specific whereabouts). Avoid posting anything that  would make it easy for a stranger to find you, such as where you hang out every day after school. 
People aren't always who they say they are. Be careful about adding people you don't know in the physical world to  your friends list. It's fun to connect with new MySpace friends from all over the world, but avoid meeting people in  person whom you do not already know in the physical world. If you decide to meet someone you've met on MySpace, tell  your parents first, do it in a public place and bring a trusted adult. 
Harassment, hate speech and inappropriate content should be reported. If you feel someone's behavior is  inappropriate, react. Talk with a trusted adult, or report it to MySpace or the authorities.
Don't post anything that would embarrass you later. It's easy to think that only people you know are looking at your  MySpace page, but the truth is that everyone can see it. Think twice before posting a photo or information you  wouldn't want others to see, including potential employers or colleges!
Do not lie about your age.  Your profile may be deleted and your Membership may be terminated without warning if we  believe that you are under 14 years of age or if we believe you are 14 through 17 years of age and you represent  yourself as 18 or older.       
Don't get hooked by a phishing scam.  Phishing is a method used by fraudsters to try to get your personal  information, such as your username and password, by pretending to be a site you trust.`);
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Grep` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>4</KeyLength>
        <DFSInput>dfs://GrepInput.txt</DFSInput>
        <DFSOutput>dfs://GrepOutput.txt</DFSOutput>
      </IOSettings>      
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              while(line.Length > 0)
              {                 
                  ByteSlice sentence = ExtractSentence(ref line);
                  mstring st = mstring.Prepare(sentence);
                  
                  if(st.Length > 0)
                  {
                      mstring key = st.SubstringM(0, 4);                      
                      output.Add(key, st);
                  }                  
              }
          }
          ]]>
        </Map>       
        <ReduceInitialize>
          <![CDATA[
        string srex = @`\bfriends?\b`; // Default string to search for.
        System.Text.RegularExpressions.Regex rex;
        public void ReduceInitialize()
        {
            if(Qizmt_ExecArgs.Length > 0)
            {
                // Allow user to specify search string on command-line.
                srex = Qizmt_ExecArgs[0];
            }
            rex = new System.Text.RegularExpressions.Regex(srex);
        }
    ]]>
        </ReduceInitialize>
        <Reduce>
          <![CDATA[
        public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
        {
            for(int iv = 0; iv < values.Length; iv++)
            {
                string s = values.Items[iv].ToString(); // Sentence as a string.
                System.Text.RegularExpressions.Match m = rex.Match(s);
                if(m.Success)
                {
                    // Match! output this sentence.
                    output.Add(values.Items[iv]);
                }
            }
        }
    ]]>
        </Reduce>
        <ReduceFinalize>
          <![CDATA[
        public void ReduceFinalize() { }
    ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region mstring
            alljobfiles.Add(@"Qizmt-mstring.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`mstring_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {            
            Shell(@`Qizmt del Qizmt-mstring_Input.txt`, true);
            Shell(@`Qizmt del Qizmt-mstring_Output.txt`, true);            
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`mstring_Remote` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-mstring_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                dfsoutput.WriteLine(`1,2,33,Los Angeles,55`);
                dfsoutput.WriteLine(`45,7,99,San Francisco,78`);
                dfsoutput.WriteLine(`1,2,33,San Jose,550`);
                dfsoutput.WriteLine(`11,29,339,Mountain View,99`);
                dfsoutput.WriteLine(`1,2,33,Washington,595`);          
          }
        ]]>
      </Remote>
    </Job>
    <Job Name=`ms` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int,int</KeyLength>
        <DFSInput>dfs://Qizmt-mstring_Input.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-mstring_Output.txt</DFSOutput>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
        public virtual void Map(ByteSlice line, MapOutput output)
        {
            //i,i,l,s,i
            //61,92,383,Washington,595
            mstring ms = mstring.Prepare(line);
            
            int i1 = ms.NextItemToInt32(',');
            int i2 = ms.NextItemToInt32(',');
            Int64 i3 = ms.NextItemToInt64(',');
            mstring s = ms.NextItemToString(',');  
            int i4 = ms.NextItemToInt32(',');
            
            recordset rKey = recordset.Prepare();
            recordset rValue = recordset.Prepare();
            
            rKey.PutInt32(i1);
            rKey.PutInt32(i2);
            
            rValue.PutInt64(i3);
            rValue.PutString(s.MToUpper());
            rValue.PutInt32(i4);
                        
            output.Add(rKey, rValue);
        }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
            recordset rKey = recordset.Prepare(key);
            
            int i1 = rKey.GetInt32();
            int i2 = rKey.GetInt32();       
            
            mstring ms = mstring.Prepare();
            
            for(int i=0; i < values.Length; i++)
            {
                recordset rValue = recordset.Prepare(values.Items[i]);
                
                Int64 i3 = rValue.GetInt64();
                mstring s = rValue.GetString();
                int i4 = rValue.GetInt32();
                mstring delimiter = mstring.Prepare(`,`);
                
                ms.Consume(ref s);
                ms.Consume(ref delimiter);                
            }
                        
            output.Add(ms);
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>    
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            GenerateImageProcessKMeans(alljobfiles);

            #region putBinary
            alljobfiles.Add(@"Qizmt-putBinary.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`putBinary_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del putBinary_Input.blob`);
            Shell(@`Qizmt del putBinary_Output.blob`);           
           
            string dir = IOUtils.GetTempDirectory() + @`\putBinaryTestPics\`;
            
            if(System.IO.Directory.Exists(dir))
            {
                System.IO.Directory.Delete(dir, true);
            }
            System.IO.Directory.CreateDirectory(dir);
            
            //Prepare binary data.
            string[] sImages = new string[3]{@`R0lGODlhDwAPALMOAP/qAEVFRQAAAP/OAP/JAP+0AP6dAP/+k//9E///////xzMzM///6//lAAAAAAAAACH5BAEAAA4ALAAAAAAPAA8AAARb0EkZap3YVabOGRcWcAgCnIMRTEEnCCfwpqt2mHEOagoOnz+CKnADxoKFyiHHBBCSAdOiCVg8KwPZa7sVrgJZQWI8FhB2msGgwTXTWGqCXP4WBQr4wjDDstQmEQA7`, 
@`R0lGODlhIAAgAPMIAAAAAAAAmQAA//8AAP//AJmZmci0yMzMzP///8i0yAAAAAAAAAAAAAAAAAAAAAAAACH5BAEAAAkAIf5/SWNvbm9ncmFwaGVyIDogTWljcm9zb2Z0IFdpbmRvd3MKVGhlIEljb25vbG9nIDogVGhlIEFydCBvZiBWaXJ0dWFsIExpdHRlciAKICAgICAgICAgICAgICAgaHR0cDovL3d3dy5vemVtYWlsLmNvbS5hdS9+YWZhY3Rvci8gCgAsAAAAACAAIADDAAAAAACZAAD//wAA//8AmZmZyLTIzMzM////yLTIAAAAAAAAAAAAAAAAAAAAAAAABOUwyUlpuTiXyntBYCgeF8BdZHYcCBAAaxxjpicixVx3fD/lsiCN9zmYPiHjLVniBQRQaGAaKMCCq6FNlLNaZh8CQkze+YDYrNd3SuuMZ3MCnbbCK0U0QHPFak83AC5TfUJNHU9RAlNZhTJrRDdokHNjlmVtj3JfWQgpm2dujWyASyF/pBJ0flagZxp8JK49q4aCtC2qsLswlD+eOXsjjmC+cyRgLCCCbqg/SCEwg8RqxhKJUYOEzYcc2FKMdjIDA58934s6guvsbNAgk3K73aqSoz/FvPVclOx7ybIo+LvFY2C7CREAADs=`, 
@`R0lGODlhRQAfAIcAAAAAABciIyImJiApMiYxKSgxMjI2NS04QzpDPS9JVEpOTUVXalJgVlVmb2FgYm1yb09ukEh3sHN1iGV2qyuQtATe/VSJi1WGtnSGjGOFq2aYpmSUvHuLonyUpnWWuG+lvWeax2Sv24qLdaW1QpnRbJGUjoWbt5SknZCot6Geoq2vr4qpxYm1yZety7q3xcTGxcPOzs7Ow87OzsXG0cnaysnX2dvZ297e793n7urs3Ojn6u3v8/H38Pf39/f3//f////38v/3////9////wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACH5BAEAAEQALAAAAABFAB8AAAj/AIkIHEiwoMGDLFSQQHGwocOHECM6RMFQosWLGAViaGCCCIqORFZgGAgyo0mTChxAwMCBJQcODVoQMSGBIYaSJ3MaREFixQUGESxYMNEAgoMFLBs0IILhwYoHDSrq1MkTRQgNFihouABBgVcFDBaAVSoBQoMFHUxInZqRBAkWIUJs0LCBAQMFYr96DduAQQMOHU6QUMH24FqDJEbAjVvhgV68X/P6ZbniBIvDBdVipIkgwciDbhdrwOBV7IEDCB5HRvEhxAcODhug/gxRwgIDCDiIyG2YxAmskMUaeFACAerHYiFg1aChYQcDKjAgwHmwAYIHKnQMGbJgqWGiFu56/zUgYbsM1AcSPPZLAQTsgxwQ9FBxgLpBBBi27yjxwICBBSYQhcACtqWmQGp5HUCDDioM4QBqBpx2GgIDQrAAAhJshIFIpSGgQwq4edeQBAo8oIMMBRTQQAoOUMhACRIYwAAHChjQwIEHMjCECg0MUYKN16mgQAnSWeeAdAe0eJsE8SGwQwr4KSCiQSs0UIANNRTwQBA96NCZDj04+MAQGGAQhAsHKHDAmCUw8KEEQqjggwwM9OCCA0MQ56AKP6QwgAjbFbeDCzcMkcIBHTjEwQE2oAgDDHgO2YObJeBZQpheImBAfhgUgICW27lQgAI+iKBjAyX8UMJ2LxygAwwy6P+ggHY2ZIehQ89h6aQIKtxgwAsuwOnXECfasMOBBwAaY1NjwuDfAkNIgOeRQwy6gwwKGHqCCwrg4KCwUxaUqw0K7KCDDgwQ8MIOQGAgwJh5rkvhAeVJN8QMYzpQgAHZSpAvoD04cCKe0jlQrg2bDhGWQyb86uWJCrCowqQYiNCjlzIc66KhYzaQb4QK9EDkEApwMMQLTmIb7Qk+MLCDCgFcKqVDEnjoAwIqDDqEx0K4YCieKhQgg8LzytBDDzEUUF6Mar7Qww42FLDqjzvsgECsPRzKQwoEZIxAuARhkIAL5OL8wgPGlQBDcQvkUEKSNaeW2gsvKFCAAzI8kOYBDMj1YIO+Jur9gN4MuHCoAS6cMBzaE0zEAGrqTdidpmkaOGBnA6p5gH/GRWggapomILdXCdxoHF4Uks4RRBw8cFMHLHVkAksddKDUeyTqlbpXBorV2QIPSFBURx+t0JIHs0vgwQoUFWYQTDeGpRpkeAn/l33OX6QhTJHhdVde3XGAQgMSrJD9ScJzYEIHKEwQ1gLgKyCBX2mhsAL752ckgQRpgdRBSxzwi/VgIoGSmC9/2oNAgNbSug4wKQMFJIr4EGiSliywIChAXv9uQoQMHZCCFjGB+DSzE5F0hHlEaMkHQRgR+6GQZhz4oAhZiECWSOUjmJlKQAAAOw==` };
            
            for(int i=0; i < sImages.Length; i++)
            {
                string fname = dir + `image` + i.ToString() + `.gif`;
                byte[] buf = Convert.FromBase64String(sImages[i]);
                System.IO.File.WriteAllBytes(fname, buf);
            }
            
            Shell(`Qizmt putbinary \`` + dir + `*.gif\` putBinary_Input.blob`);
        }
        ]]>
      </Local>
    </Job>    
    <Job Name=`putBinary` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>260</KeyLength>
        <DFSInput>dfs://putBinary_Input.blob</DFSInput>
        <DFSOutput>dfs://putBinary_Output.blob</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <DynamicFoil/>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {                
                Blob b = line.ReadBinary();                        
                
                //Can process binary data here...
                byte[] data = b.data;
                
                mstring key = mstring.Prepare(b.name);        
                recordset value = recordset.Prepare();
                value.PutBytes(data, 0, data.Length);
                
                output.Add(key, value);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {              
              string blobName = UnpadKey(key).ToString();                    
             
              for(int i=0; i < values.Length; i++)
              {
                  //Can process binary data here...
                  byte[] data = values.Items[i].ToBytes();
                  
                  Blob b = Blob.Prepare(blobName, data);
                  
                  output.AddBinary(b);
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>    
    <Job Name=`putBinary_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {            
            string tdir = IOUtils.GetTempDirectory() + @`\putBinaryTestPics2`;
            
            if(System.IO.Directory.Exists(tdir))
            {
                System.IO.Directory.Delete(tdir, true);
            }
            System.IO.Directory.CreateDirectory(tdir);
            
            Shell(`Qizmt getbinary putBinary_Output.blob \`` + tdir + `\``);            
        }        
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>

".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region ExplicitCacheWordCount
            alljobfiles.Add(@"Qizmt-ExplicitCacheWordCount.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`ExplicitCacheWordCount_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {            
            Shell(@`Qizmt del ExplicitCacheWordCount_Temp*.txt`);
            Shell(@`Qizmt del ExplicitCacheWordCount_Input*.txt`);
            Shell(@`Qizmt del ExplicitCacheWordCount_Output.txt`);
        }
        ]]>
      </Local>
    </Job>
    <Job description=`Create sample data` Name=`ExplicitCacheWordCount_CreateSampleData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://ExplicitCacheWordCount_Temp01.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(@`Create a community on MySpace and you can share photos, journals and interests with your growing network of mutual  friends!
See who knows who, or how you are connected. Find out if you really are six people away from Kevin Bacon.`);
           }
        ]]>
      </Remote>
    </Job>
    <Job description=`Create sample data` Name=`ExplicitCacheWordCount_CreateSampleData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://ExplicitCacheWordCount_Temp02.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.               
                dfsoutput.WriteLine(@`MySpace is for everyone:
Friends who want to talk Online 
Single people who want to meet other Singles 
Matchmakers who want to connect their friends with other friends 
Families who want to keep in touch--map your Family Tree `); 
           }
        ]]>
      </Remote>
    </Job>
    <Job description=`Create sample data` Name=`ExplicitCacheWordCount_CreateSampleData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://ExplicitCacheWordCount_Temp03.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.               
                dfsoutput.WriteLine(@`Business people and co-workers interested in networking 
Classmates and study partners 
Anyone looking for long lost friends!`);             
           }
        ]]>
      </Remote>
    </Job>
    <Job description=`Create sample data` Name=`ExplicitCacheWordCount_CreateSampleData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://ExplicitCacheWordCount_Temp04.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(@`MySpace makes it easy to express yourself, connect with friends and make new ones, but please remember that what you  post publicly can be read by anyone viewing your profile, so we suggest you consider the following guidelines when  using MySpace: 
Don't forget that your profile and MySpace forums are public spaces. Don't post anything you wouldn't want the world  to know (e.g., your phone number, address, IM screens name, or specific whereabouts). Avoid posting anything that  would make it easy for a stranger to find you, such as where you hang out every day after school. 
People aren't always who they say they are. Be careful about adding people you don't know in the physical world to  your friends list. It's fun to connect with new MySpace friends from all over the world, but avoid meeting people in  person whom you do not already know in the physical world. If you decide to meet someone you've met on MySpace, tell  your parents first, do it in a public place and bring a trusted adult. `);        
           }
        ]]>
      </Remote>
    </Job>
    <Job description=`Create sample data` Name=`ExplicitCacheWordCount_CreateSampleData` Custodian=`` email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://ExplicitCacheWordCount_Temp05.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.             
                dfsoutput.WriteLine(@`Harassment, hate speech and inappropriate content should be reported. If you feel someone's behavior is  inappropriate, react. Talk with a trusted adult, or report it to MySpace or the authorities.
Don't post anything that would embarrass you later. It's easy to think that only people you know are looking at your  MySpace page, but the truth is that everyone can see it. Think twice before posting a photo or information you  wouldn't want others to see, including potential employers or colleges!
Do not lie about your age.  Your profile may be deleted and your Membership may be terminated without warning if we  believe that you are under 14 years of age or if we believe you are 14 through 17 years of age and you represent  yourself as 18 or older.       
Don't get hooked by a phishing scam.  Phishing is a method used by fraudsters to try to get your personal  information, such as your username and password, by pretending to be a site you trust.`);                
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`ExplicitCacheWordCount_Preprocessing` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {            
            //Iteration
            int i = 1;
            
            if(Qizmt_ExecArgs.Length > 0)
            {
                i = Int32.Parse(Qizmt_ExecArgs[0]);
            }
                
            if(i > 5)
            {
                i = 1;
            }
                            
            //Clean out previous files
            if(i == 1)
            {
                Shell(@`Qizmt del ExplicitCacheWordCount_Cache`);
            }
            
           //Put input files
            for(int k = 1; k <= i; k++)
            {
                Shell(@`Qizmt rename ExplicitCacheWordCount_Temp0` + k.ToString() + `.txt ExplicitCacheWordCount_Input0` + k.ToString() + `.txt` );
            }
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`ExplicitCacheWordCount` Custodian=`` email=``>
      <Delta>
        <Name>ExplicitCacheWordCount_Cache</Name>
        <DFSInput>dfs://ExplicitCacheWordCount_Input*.txt</DFSInput>
      </Delta>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>16</KeyLength>
        <DFSInput></DFSInput>
        <DFSOutput>dfs://ExplicitCacheWordCount_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <DynamicFoil/>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                Qizmt_Log(`**************************counting words****************************`);
                mstring sLine = mstring.Prepare(line);
                mstringarray words = sLine.SplitM(' ');
                mstring value = mstring.Prepare();
                
                for(int i=0; i < words.Length; i++)
                {
                    mstring word = words[i].TrimM('.').TrimM(',').TrimM('!').TrimM('?').TrimM(':').TrimM(';').TrimM('(').TrimM(')').ToLowerM();
                    
                    if(word.Length > 0 && word.Length <= Qizmt_KeyLength) 
                    {                        
                        output.Add(word, value); 
                    }
                }
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          [ExplicitCache]
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              mstring word = mstring.Prepare(UnpadKey(key));
              int count = 0;
              
              while(values.MoveNext())
              {               
                  mstring value = mstring.Prepare(values.Current);
                  
                  if(value.Length > 0)
                  {
                      count += value.ToInt();
                  }
                  else
                  {
                      ++count;
                  }
              }
                           
              mstring sCount = mstring.Prepare(count);
              
              output.Cache(word, sCount);           
             
              output.Add(word.AppendM(',').AppendM(sCount));  
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region WildCardNumericRanges
            alljobfiles.Add(@"Qizmt-WildCardNumericRanges.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`Qizmt-WildCardNumericRanges_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del Qizmt-WildCardNumericRanges_Input*.txt`);
            Shell(@`Qizmt del Qizmt-WildCardNumericRanges_Output*.txt`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Qizmt-WildCardNumericRanges_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WildCardNumericRanges_Input11x.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`11a`);
                dfsoutput.WriteLine(`11b`);
                dfsoutput.WriteLine(`11c`);      
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-WildCardNumericRanges_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WildCardNumericRanges_Input12y.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`12a`);
                dfsoutput.WriteLine(`12b`);
                dfsoutput.WriteLine(`12c`);               
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-WildCardNumericRanges_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WildCardNumericRanges_Input13z.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`13a`);
                dfsoutput.WriteLine(`13b`);
                dfsoutput.WriteLine(`13c`);       
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-WildCardNumericRanges_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WildCardNumericRanges_Input14p.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`14a`);
                dfsoutput.WriteLine(`14b`);
                dfsoutput.WriteLine(`14c`);                   
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-WildCardNumericRanges_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WildCardNumericRanges_Input15f.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`15a`);
                dfsoutput.WriteLine(`15b`);
                dfsoutput.WriteLine(`15c`);      
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-WildCardNumericRanges_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WildCardNumericRanges_Input20090514x.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`20090514 a`);
                dfsoutput.WriteLine(`20090514 b`);
                dfsoutput.WriteLine(`20090514 c`);      
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-WildCardNumericRanges_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WildCardNumericRanges_Input20090615x.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`20090615 a`);
                dfsoutput.WriteLine(`20090615 b`);
                dfsoutput.WriteLine(`20090615 c`);      
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-WildCardNumericRanges_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WildCardNumericRanges_Input20091201x.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`20091201 a`);
                dfsoutput.WriteLine(`20091201 b`);
                dfsoutput.WriteLine(`20091201 c`);      
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-WildCardNumericRanges` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>10</KeyLength>
        <DFSInput>dfs://Qizmt-WildCardNumericRanges_Input|11-12|*.txt;dfs://Qizmt-WildCardNumericRanges_Input15f.txt;dfs://Qizmt-WildCardNumericRanges_Input|20090601-20100101|*.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-WildCardNumericRanges_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <DynamicFoil/>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                mstring sline = mstring.Prepare(line);
                output.Add(sline, mstring.Prepare());
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              mstring skey = mstring.Prepare(UnpadKey(key));
              
              for(int i = 0; i < values.Length; i++)
              {                  
                    output.Add(skey);                    
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region Qizmt-NumericStringKeys.xml
            alljobfiles.Add(@"Qizmt-NumericStringKeys.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`Qizmt-NumericStringKeys_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del Qizmt-NumericStringKeys_Input*.txt`);
            Shell(@`Qizmt del Qizmt-NumericStringKeys_Output.txt`);
            
            Shell(@`Qizmt wordgen Qizmt-NumericStringKeys_Input.txt 10KB 20B`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Qizmt-NumericStringKeys_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>Qizmt-NumericStringKeys_Input.txt</DFSReader>
          <DFSWriter>dfs://Qizmt-NumericStringKeys_Input2.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                Random rnd = new Random();                
                StringBuilder sb = new StringBuilder();
                
                while(dfsinput.ReadLineAppend(sb))
                {
                    string line = sb.ToString();
                    int num = rnd.Next();
                    if(num % 2 == 0)
                    {
                        num = num * (-1);
                    }
                    sb.Append(`,` + num.ToString());
                    dfsoutput.WriteLine(sb);     
                    sb.Length = 0;
                }
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-NumericStringKeys` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>12</KeyLength>
        <DFSInput>dfs://Qizmt-NumericStringKeys_Input2.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-NumericStringKeys_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <DynamicFoil/>
      <MapReduce>
        <Map>
          <![CDATA[
          int stringLen = 2;
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                mstring sLine = mstring.Prepare(line);
                mstringarray parts = sLine.SplitM(',');
                uint num = Entry.ToUInt32(parts[1].ToInt32());
                mstring snum = mstring.Prepare(num).PadLeftM(10, '0');  
                
                parts = sLine.SplitM(' ');                
                mstring word = parts[0];                
                if(word.Length > stringLen)
                {
                    word = word.SubstringM(0, stringLen);
                }
                else if(word.Length < stringLen)
                {
                    word = word.PadRightM(stringLen, ' ');
                }          
                
                mstring key = mstring.Prepare();
                key = key.AppendM(word).AppendM(snum);
                output.Add(key, mstring.Prepare());
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {   
              mstring skey = mstring.Prepare(UnpadKey(key));
              mstring word = skey.SubstringM(0, 2);
              string snum = skey.SubstringM(2).ToString();
              int num = Entry.ToInt32(UInt32.Parse(snum));
              
              mstring line = word.AppendM(',').AppendM(num);
              output.Add(line);
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

       
            #region SharedMemory
            alljobfiles.Add(@"Qizmt-SharedMemory.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"
<!--

    -  If shared memory is too large for 1 machine, should use distributed
        memory / RINDEX so that there is scalable direct access lookups.
        
    -  If a lookup table is ultra-small, should have redundant copy in every
        process so that it can be stored as a local dictionary.
        
    -  This is an example useful for storing data which is redundant across
        machines, but shared by all workers on each machine.

-->

<SourceCode>
  <Jobs>
    <Job Name=`SharedMem_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del SharedMem_Input.txt`);
                Shell(@`Qizmt del SharedMem_Table.txt`);
                Shell(@`Qizmt del SharedMem_Output.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`SharedMem_CreateInputData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://SharedMem_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                dfsoutput.WriteLine(`1498,The Last Supper,100.45,374000000`);
                dfsoutput.WriteLine(`1503,Mona Lisa,4.75,600000000`);
                dfsoutput.WriteLine(`1990,Bill Murray,4.4,112000000`);
                dfsoutput.WriteLine(`1501,Study for a portrait of Isabella d'Este,1.5,100000000`);
                dfsoutput.WriteLine(`1501,Study of horse,1.5,100000000`);
                dfsoutput.WriteLine(`2003,Josh Holloway,1.1,500000000`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`SharedMem_CreateTableData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://SharedMem_Table.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                dfsoutput.WriteLine(`1501`);
                dfsoutput.WriteLine(`1498`);
                dfsoutput.WriteLine(`1503`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`SharedMem` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int,double</KeyLength>
        <DFSInput>dfs://SharedMem_Input.txt</DFSInput>
        <DFSOutput>dfs://SharedMem_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <Using>System.Runtime.InteropServices</Using>
      <Unsafe/>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                mstring sLine = mstring.Prepare(line);
                int year = sLine.NextItemToInt(',');
                mstring title = sLine.NextItemToString(',');
                double size = sLine.NextItemToDouble(',');
                long pixel = sLine.NextItemToLong(',');
                
                recordset rKey = recordset.Prepare();
                rKey.PutInt(year);
                rKey.PutDouble(size);
                
                recordset rValue = recordset.Prepare();
                rValue.PutString(title);
                rValue.PutLong(pixel);
                
                output.Add(rKey, rValue);
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            
            const int sharedbytes = 0x400 * 0x400; // Number of bytes to reserve.
            unsafe int* pyears;
            int pyearslength; // Will be updated with the number of valid elements in pyears.
            unsafe byte* pview = null;
            IntPtr hmap = IntPtr.Zero;
            
            public unsafe void ReduceInitialize()
            {
                System.Threading.Mutex mutex = new System.Threading.Mutex(false, `SharedMem_Table{B98FC90B-D396-4e2d-A08F-90F7E955D703}`);
                mutex.WaitOne();
                try
                {
                    hmap = CreateFileMapping(INVALID_HANDLE_VALUE, IntPtr.Zero, PAGE_READWRITE, 0, sharedbytes, `SharedMem_Table{E2E20082-8DCE-44ec-AFBA-5E6F1AB1A07B}`);
                    int lasterror = Marshal.GetLastWin32Error();
                    if(IntPtr.Zero == hmap)
                    {
                        throw new Exception(`CreateFileMapping failed; Marshal.GetLastWin32Error() returned error code ` + lasterror);
                    }
                    pview = (byte*)MapViewOfFile(hmap, FILE_MAP_ALL_ACCESS, 0, 0, 0);
                    pyears = (int*)pview + 1; // First int of pview is number of following elements.
                    if(lasterror != ERROR_ALREADY_EXISTS)
                    {
                        *(int*)pview = 0; // Number of elements init.
                        DfsStream ds = new DfsStream(`dfs://SharedMem_Table.txt`);
                        System.IO.StreamReader sr = new System.IO.StreamReader(ds);
                        int* pcurrent = (int*)pview + 1;
                        for(;;)
                        {
                            string line = sr.ReadLine();
                            if(line == null)
                            {
                                break;
                            }
                            if((byte*)pcurrent >= pview + sharedbytes)
                            {
                                throw new Exception(`Read too many values into shared memory map`);
                            }
                            *pcurrent = int.Parse(line);
                            pcurrent++;
                            pyearslength++;
                        }
                        *(int*)pview = pyearslength; // Number of elements.
                        sr.Close();
                        ds.Close();
                    }
                    pyearslength = *(int*)pview;
                }
                finally
                {
                    mutex.ReleaseMutex();
                    mutex.Close();
                }
            }
            
            public unsafe override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rKey = recordset.Prepare(key);
                int year = rKey.GetInt();
                double size = rKey.GetDouble();
                
                {
                    // Use pyears table to see if this year is wanted.
                    bool wantyear = false;
                    for(int i = 0; i < pyearslength; i++)
                    {
                        if(year == pyears[i])
                        {
                            wantyear = true;
                            break;
                        }
                    }
                    if(!wantyear)
                    {
                        return;
                    }
                }
                
                for(int i = 0; i < values.Length; i++)
                {
                    recordset rValue = recordset.Prepare(values.Items[i]);
                    mstring title = rValue.GetString();
                    long pixel = rValue.GetLong();
                    
                    mstring sLine = mstring.Prepare(year);
                    sLine = sLine.AppendM(',')
                        .AppendM(title)
                        .AppendM(',')
                        .AppendM(size)
                        .AppendM(',')
                        .AppendM(pixel);
                    
                    output.Add(sLine);
                }
            }
            
            public unsafe void ReduceFinalize()
            {
                UnmapViewOfFile(new IntPtr(pview));
                CloseHandle(hmap);
            }
            
            
            public unsafe static void pmemcpy(byte* dest, byte* src, int length)
            {
                uint numints = (uint)length >> 2;
                int* idest = (int*)dest;
                int* isrc = (int*)src;
                for (int i = 0; i < numints; i++)
                {
                    idest[i] = isrc[i];
                }
                for (uint remainpos = numints << 2; remainpos < (uint)length; remainpos++)
                {
                    dest[remainpos] = src[remainpos];
                }
            }

            public unsafe static void pmemcpy(byte* dest, byte[] src, int srcoffset, int length)
            {
                fixed (byte* psrc = src)
                {
                    pmemcpy(dest, psrc + srcoffset, length);
                }
            }

            public unsafe static void pmemcpy(byte[] dest, int destoffset, byte* src, int length)
            {
                fixed (byte* pdest = dest)
                {
                    pmemcpy(pdest + destoffset, src, length);
                }
            }


            [DllImport(`kernel32`, SetLastError = true, CharSet = CharSet.Auto)]
            public static extern IntPtr CreateFile(
               String lpFileName, int dwDesiredAccess, int dwShareMode,
               IntPtr lpSecurityAttributes, int dwCreationDisposition,
               int dwFlagsAndAttributes, IntPtr hTemplateFile);

            [DllImport(`kernel32`, SetLastError = true, CharSet = CharSet.Auto)]
            public static extern IntPtr CreateFileMapping(
               IntPtr hFile, IntPtr lpAttributes, int flProtect,
               int dwMaximumSizeHigh, int dwMaximumSizeLow,
               String lpName);

            [DllImport(`kernel32`, SetLastError = true)]
            public static extern bool FlushViewOfFile(
               IntPtr lpBaseAddress, int dwNumBytesToFlush);

            [DllImport(`kernel32`, SetLastError = true)]
            public static extern IntPtr MapViewOfFile(
               IntPtr hFileMappingObject, int dwDesiredAccess, int dwFileOffsetHigh,
               int dwFileOffsetLow, int dwNumBytesToMap);

            [DllImport(`kernel32`, SetLastError = true, CharSet = CharSet.Auto)]
            public static extern IntPtr OpenFileMapping(
               int dwDesiredAccess, bool bInheritHandle, String lpName);

            [DllImport(`kernel32`, SetLastError = true)]
            public static extern bool UnmapViewOfFile(IntPtr lpBaseAddress);

            [DllImport(`kernel32`, SetLastError = true)]
            public static extern bool CloseHandle(IntPtr handle);

            public const int ERROR_ALREADY_EXISTS = 183 ;

            public static IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);

            public const int PAGE_READWRITE = 0x4;

            public const int FILE_MAP_WRITE = 0x2;
            public const int FILE_MAP_READ = 0x4;
            public const int FILE_MAP_ALL_ACCESS = 0xF001F;
            
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`SharedMem_Display` Custodian=`` Email=`` Description=`Display output data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://SharedMem_Output.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display output.
                Qizmt_Log(`Output:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region RemoteMultiIO
            /*
            alljobfiles.Add(@"Qizmt-RemoteMultiIO.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            string thishost = System.Net.Dns.GetHostName();
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                (@"<SourceCode>
  <Jobs>
    <Job Name=`Qizmt-RemoteMultiIO_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del Qizmt-RemoteMultiIO_*.txt`);
            Shell(@`Qizmt wordgen Qizmt-RemoteMultiIO_Input.txt 100KB`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Qizmt-RemoteMultiIO_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>        
        <DFS_IO_Multi>
          <DFSReader>dfs://Qizmt-RemoteMultiIO_Input.txt</DFSReader>
          <DFSWriter>dfs://Qizmt-RemoteMultiIO_Output1####.txt</DFSWriter>
          <Mode>ALL MACHINES</Mode>
        </DFS_IO_Multi>
        <DFS_IO_Multi>
          <DFSReader>dfs://Qizmt-RemoteMultiIO_Input.txt</DFSReader>
          <DFSWriter>dfs://Qizmt-RemoteMultiIO_Output2####.txt</DFSWriter>
          <Mode>ALL CORES</Mode>
        </DFS_IO_Multi>
        <DFS_IO>
          <DFSReader>dfs://Qizmt-RemoteMultiIO_Input.txt</DFSReader>
          <DFSWriter>dfs://Qizmt-RemoteMultiIO_Output3.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {                           
                int processID = Qizmt_ProcessID;
                int processCount = Qizmt_ProcessCount ;
                
                StringBuilder sb = new StringBuilder();
                int i = 0;
                
                while(dfsinput.ReadLineAppend(sb))
                {
                    if(i % processCount == processID)
                    {
                        dfsoutput.WriteLine(sb.ToString());
                    }                    
                    sb.Length = 0;
                    i++;
                }          
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-RemoteMultiIO_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://Qizmt-RemoteMultiIO_Input.txt</DFSReader>
          <DFSWriter>dfs://Qizmt-RemoteMultiIO__Output.txt</DFSWriter>
          <Host>" + thishost + @"</Host>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {   
                StringBuilder sb = new StringBuilder();
                
                while(dfsinput.ReadLineAppend(sb))
                {                    
                    dfsoutput.WriteLine(sb.ToString());                                       
                    sb.Length = 0;
                }          
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-RemoteMultiIO_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt combine Qizmt-RemoteMultiIO_Output*.txt +Qizmt-RemoteMultiIO_Result.txt`);            
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            */
            #endregion

            #region Table
            alljobfiles.Add(@"Qizmt-Table.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`Qizmt-Table_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del Qizmt-Table_*`);
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Qizmt-Table_CreateSampleData` Custodian=`` Email=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-Table_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
          public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
          {             
                //Create sample data.
                dfsoutput.WriteLine(`1001,John Doe,5.5`);
                dfsoutput.WriteLine(`203,Mary Chap,4`);
                dfsoutput.WriteLine(`203,Mary Chap,5`);
                dfsoutput.WriteLine(`990,Kim Joh,3.2`);
                dfsoutput.WriteLine(`89,Yang Yi,8.9`);
                dfsoutput.WriteLine(`89,Yang Yi,1`);
           }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-Table` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://Qizmt-Table_Input.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-Table_Input.tbl@64</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <DynamicFoil/>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                mstring sLine = mstring.Prepare(line);
                int uid = sLine.NextItemToInt(',');
                mstring uname = sLine.NextItemToString(',');
                double grade = sLine.NextItemToDouble(',');
                
                recordset rKey = recordset.Prepare();
                rKey.PutInt(uid);
                
                recordset rValue = recordset.Prepare();
                rValue.PutInt(uid);
                rValue.PutString(uname);
                rValue.PutDouble(grade);
                
                output.Add(rKey, rValue);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              for(int i = 0; i < values.Length; i++)
              {
                  recordset rs = recordset.Prepare(values[i].Value);
                  output.Add(rs);
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`Qizmt-Table` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://Qizmt-Table_Input.tbl@64</DFSInput>
        <DFSOutput>dfs://Qizmt-Table_Output.tbl@64</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <DynamicFoil/>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
                recordset rline = recordset.PrepareRow(line);
                int uid = rline.GetInt();
                mstring uname = rline.GetString();
                double grade = rline.GetDouble();
                
                recordset rkey = recordset.Prepare();
                rkey.PutInt(uid);
                
                recordset rvalue = recordset.Prepare();
                rvalue.PutString(uname);
                rvalue.PutDouble(grade);
                
                output.Add(rkey, rvalue);
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              recordset rkey = recordset.Prepare(key);
              int uid = rkey.GetInt();
              double total = 0;
              
              for(int i = 0; i < values.Length; i++)
              {                  
                  recordset rs = recordset.Prepare(values[i].Value);
                  mstring uname = rs.GetString();
                  double grade = rs.GetDouble();
                  total += grade;
              }
              
              recordset rout = recordset.Prepare();
              rout.PutInt(uid);
              rout.PutDouble(total);
              output.Add(rout);
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`Qizmt-Table_Postprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            string tempfp = IOUtils.GetTempDirectory() + @`\Qizmt-Table_Output.tbl`;
            Shell(`Qizmt get Qizmt-Table_Output.tbl \`` + tempfp + `\``);
            Shell(`Qizmt put \`` + tempfp + `\` Qizmt-Table_Result.tbl@64`);
            System.IO.File.Delete(tempfp);
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region ClusterLock
            /*
            alljobfiles.Add(@"Qizmt-ClusterLock.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
<!--
To clean previous leaked data before running job, use -c switch:
Qizmt exec Qizmt-ClusterLock.xml -c
-->
<Jobs>
    <Job Name=`Qizmt-ClusterLock_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            //Delete DFS files from previous run.
            Shell(@`Qizmt del Qizmt-ClusterLock_Input.txt`);
            Shell(@`Qizmt del Qizmt-ClusterLock_Output.txt`);
            Shell(@`Qizmt wordgen Qizmt-ClusterLock_Input.txt 1GB 100B`);
                     
            //Delete master index file from previous run.
            string dir = @`\\` + StaticGlobals.Qizmt_Hosts[0] + @`\c$\temp\Qizmt\masterindex\`;
            if(System.IO.Directory.Exists(dir))
            {
                System.IO.File.Delete(dir + `masterindex.txt`);
            }
            
            //Delete subindex files from previous run if switch -c is specified.
            if(Qizmt_ExecArgs.Length > 0)
            {
                string[] allhosts = StaticGlobals.Qizmt_Hosts;
                foreach(string host in allhosts)
                {
                    string localdir = @`\\` + host + @`\c$\temp\Qizmt\subindexes\`;
                    if(System.IO.Directory.Exists(localdir))
                    {
                        System.IO.Directory.Delete(localdir, true);
                    }
                }
            }            
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`Qizmt-ClusterLock` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>100</KeyLength>
        <DFSInput>dfs://Qizmt-ClusterLock_Input.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-ClusterLock_Output.txt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <DynamicFoil/>
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
          string thisindexdir = @`\\` + System.Net.Dns.GetHostName() + @`\c$\temp\Qizmt\subindexes\`;
          string thisindexfn = @`\\` + System.Net.Dns.GetHostName() + @`\c$\temp\Qizmt\subindexes\subindex_` + Qizmt_ProcessID + `_` + Guid.NewGuid().ToString() + `.txt`;
          System.IO.StreamWriter writer = null;
          
          public void ReduceInitialize()
          { 
              //Create subindex file.
              if(!System.IO.Directory.Exists(thisindexdir))
              {
                  System.IO.Directory.CreateDirectory(thisindexdir);
              }
              writer = new System.IO.StreamWriter(thisindexfn);             
          }        
        ]]>
        </ReduceInitialize>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {                            
              for(int i = 0; i < values.Length; i++)
              {                  
                    //write to subindex file.
                    writer.WriteLine(key.ToString());                    
              }
          }
        ]]>
        </Reduce>
        <ReduceFinalize>
          <![CDATA[
          public void ReduceFinalize()
          {
                writer.Close();
                writer = null;

                {
                    //If there is no content, get out of here.
                    System.IO.FileInfo fi = new System.IO.FileInfo(thisindexfn);
                    if(fi.Length == 0)
                    {
                        return;
                    }
                }  
                
                //Write the first line of this subindex and subindex file name to the master index.
                using(GlobalCriticalSection.GetLock())
                {  
                    //Create master index file directory if it doesn't exist.
                    string dir = @`\\` + StaticGlobals.Qizmt_Hosts[0] + @`\c$\temp\Qizmt\masterindex\`;
                    if(!System.IO.Directory.Exists(dir))
                    {
                        System.IO.Directory.CreateDirectory(dir);
                    }
                    string masterfn = dir + `masterindex.txt`;

                    //Read all lines from master index if it exists.
                    List<string> list = new List<string>();
                    
                    if (System.IO.File.Exists(masterfn))
                    {
                        string[] lines = System.IO.File.ReadAllLines(masterfn);
                        list.AddRange(lines);
                    }

                    string firstline = ``;
                    {
                        //Get the first line from subindex.
                        System.IO.StreamReader reader = new System.IO.StreamReader(thisindexfn);
                        firstline = reader.ReadLine();
                        reader.Close();
                    }

                    //Insert into master index as sort.       
                    int wheretoinsert = -1;

                    for (int i = 0; i < list.Count; i+=2)
                    {
                        if (string.Compare(firstline, list[i + 1]) <= 0)
                        {
                            wheretoinsert = i;
                            break;
                        }
                    }
                    if (wheretoinsert == -1)
                    {
                        wheretoinsert = list.Count;
                    }

                    list.Insert(wheretoinsert, thisindexfn);
                    list.Insert(wheretoinsert + 1, firstline);
                    
                    //Write all back to master index file.
                    System.IO.StreamWriter masterwriter = new System.IO.StreamWriter(masterfn);
                    foreach (string line in list)
                    {
                        masterwriter.WriteLine(line);
                    }
                    masterwriter.Close();       
                }
          }       
        ]]>
        </ReduceFinalize>
      </MapReduce>
    </Job>
    <Job Name=`Qizmt-ClusterLock_post-processing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            //Show master index content.
            string masterfn = @`\\` + StaticGlobals.Qizmt_Hosts[0] + @`\c$\temp\Qizmt\masterindex\masterindex.txt`;
            string content = System.IO.File.ReadAllText(masterfn);
            Qizmt_Log(content);
            Qizmt_Log(`The master index is created at: ` + masterfn);
            Qizmt_Log(`To clear example data during the next run: Qizmt exec Qizmt-ClusterLock.xml -c`); 
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            */
            #endregion

            #region MultipleInputOutputFiles
            alljobfiles.Add(@"Qizmt_MultipleInputOutputFiles.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`Qizmt_MultipleInputOutputFiles_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del Qizmt_MultipleInputOutputFiles_Paintings*.txt`);
                Shell(@`Qizmt del Qizmt_MultipleInputOutputFiles_Output*.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`Qizmt_MultipleInputOutputFiles_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt_MultipleInputOutputFiles_Paintings001.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.                
                dfsoutput.WriteLine(`1499,The Sunset,10.0,39000000`);
                dfsoutput.WriteLine(`1498,The Last Supper,100.45,374000000`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt_MultipleInputOutputFiles_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt_MultipleInputOutputFiles_Paintings002.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.
                dfsoutput.WriteLine(`1503,Mona Lisa,4.75,600000000`);
                dfsoutput.WriteLine(`1501,Study for a portrait of Isabella d'Este,1.5,100000000`);
                dfsoutput.WriteLine(`1501,Study of horse,1.5,100000000`);
            }
        ]]>
      </Remote>
    </Job>    
    <Job Name=`Qizmt_MultipleInputOutputFiles_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt_MultipleInputOutputFiles_Paintings003.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.                
                dfsoutput.WriteLine(`1605,Dog and Master,60.0,500000`);
                dfsoutput.WriteLine(`1655,The Garden,35,689000000`);
                dfsoutput.WriteLine(`1689,Sunrise,58,1000000`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt_MultipleInputOutputFiles` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int,double</KeyLength>
        <DFSInput>dfs://Qizmt_MultipleInputOutputFiles_Paintings*.txt</DFSInput>
        <DFSOutput>dfs://Qizmt_MultipleInputOutputFiles_Output1.txt;dfs://Qizmt_MultipleInputOutputFiles_Output2.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                string inputFileName = StaticGlobals.Qizmt_InputFileName;  
                mstring ms = mstring.Prepare(inputFileName);
                mstring strSetID = ms.SubstringM(ms.Length - 7, 3);
                int setID = strSetID.ToInt();
                
                mstring sline = mstring.Prepare(line);
                int paintingID = sline.NextItemToInt(',');
                
                recordset key = recordset.Prepare();
                key.PutInt(paintingID);
                
                recordset val = recordset.Prepare();
                val.PutInt(setID);
                
                output.Add(key, val);
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rKey = recordset.Prepare(key);
                int paintingID = rKey.GetInt();     
                
                for(int i = 0; i < values.Length; i++)
                {
                    recordset rval = recordset.Prepare(values[i].Value);
                    int setID = rval.GetInt();
                    
                    ReduceOutput thisoutput = null;
                    if(setID > 1)
                    {
                        thisoutput = output.GetOutputByIndex(1);
                    }
                    else
                    {
                        thisoutput = output.GetOutputByIndex(0);
                    }
                    mstring outline = mstring.Prepare(paintingID);
                    outline.AppendM(':');
                    outline.AppendM(setID);
                    thisoutput.Add(outline);
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region HeteroInputOutputFiles
            alljobfiles.Add(@"Qizmt-HeteroInputOutputFiles.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`HeteroInputOutputFiles_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del HeteroInputOutputFiles_Input*`);
                Shell(@`Qizmt del HeteroInputOutputFiles_Output*`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`HeteroInputOutputFiles_CreateSampleData` Custodian=`` Email=`` Description=`Create sample text data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://HeteroInputOutputFiles_Input1;HeteroInputOutputFiles_Input2@4</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample text data.
                dfsoutput.GetOutputByIndex(0).WriteLine(`1498,The Last Supper,100.45,374000000`);
                dfsoutput.GetOutputByIndex(0).WriteLine(`1503,Mona Lisa,4.75,600000000`);
                dfsoutput.GetOutputByIndex(0).WriteLine(`1501,Study for a portrait of Isabella d'Este,1.5,100000000`);
                dfsoutput.GetOutputByIndex(0).WriteLine(`1501,Study of horse,1.5,100000000`);

                //Create sample binary data.
                List<byte> buf = new List<byte>();
                for(int i = 1500; i <= 1504; i++)
                {
                    recordset rs = recordset.Prepare();
                    rs.PutInt(i);
                    buf.Clear();
                    rs.ToByteSlice().AppendTo(buf);
                    int buflen = buf.Count;
                    dfsoutput.GetOutputByIndex(1).WriteRecord(buf);
                }
            }
        ]]>
      </Remote>
    </Job>    
    <Job Name=`HeteroInputOutputFiles` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://HeteroInputOutputFiles_Input1;dfs://HeteroInputOutputFiles_Input2@4</DFSInput>
        <DFSOutput>dfs://HeteroInputOutputFiles_Output1;dfs://HeteroInputOutputFiles_Output2@4</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <DynamicFoil/>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                if(string.Compare(StaticGlobals.Qizmt_InputFileName, `HeteroInputOutputFiles_Input2`, true) == 0)
                {
                    recordset rs = recordset.Prepare(line);
                    int i = rs.GetInt();
                    mstring ms = mstring.Prepare(i);
                    ms.AppendM(`,N/A,N/A,N/A`);
                    output.Add(rs, ms);
                }
                else if(string.Compare(StaticGlobals.Qizmt_InputFileName, `HeteroInputOutputFiles_Input1`, true) == 0) // Text.
                {
                    recordset rs = recordset.Prepare();
                    mstring ms = mstring.Prepare(line);
                    int i = ms.CsvNextItemToInt32();
                    rs.PutInt(i);
                    output.Add(rs, ms);
                }
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rs = recordset.Prepare(key);
                while(values.MoveNext())
                {
                    mstring ms = mstring.Prepare(values.Current);
                    output.GetOutputByIndex(0).WriteLine(ms);
                    
                    output.GetOutputByIndex(1).Add(rs);
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`HeteroInputOutputFiles_Display` Custodian=`` Email=`` Description=`Display output`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://HeteroInputOutputFiles_Output1;dfs://HeteroInputOutputFiles_Output2@4</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                List<byte> buf = new List<byte>();
                for(;;)
                {
                    if(string.Compare(StaticGlobals.Qizmt_InputFileName, `HeteroInputOutputFiles_Output2`, true) == 0)
                    {
                        buf.Clear();
                        if(!dfsinput.ReadRecordAppend(buf))
                        {
                            break;
                        }
                        ByteSlice line = ByteSlice.Prepare(buf);
                        recordset rs = recordset.Prepare(line);
                        int i = rs.GetInt();
                        Qizmt_Log(`int : ` + i);
                    }
                    else if(string.Compare(StaticGlobals.Qizmt_InputFileName, `HeteroInputOutputFiles_Output1`, true) == 0) // Text.
                    {
                        buf.Clear();
                        if(!dfsinput.ReadLineAppend(buf))
                        {
                            break;
                        }
                        ByteSlice line = ByteSlice.Prepare(buf);
                        mstring ms = mstring.Prepare(line);
                        Qizmt_Log(`text: ` + ms);
                    }
                }
            }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region MemCache
            alljobfiles.Add(@"Qizmt-MemCache.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<!--

The following jobs operate on a MemCache with these steps:
Local job MemCache_Create:
    1. Create a new MemCache,
    2. Generate input data to populate into the MemCache,
        this uses the gen command to generate random binary records,
MapReduce job MemCache_Insert:
    3. Map over the input data for Reduce to insert into the MemCache,
        this needs to be done in Reduce so all keys hash to the same worker,
        the keys are sorted on the last reduce iteration and inserted in order,
Local job MemCache_Commit:
    4. Commit the MemCache to DFS,
        this allows transactions, can either roll back or commit future changes,
        commit can be postponed and queued every X hours via a schedule,
Local job MemCache_CreateDelta:
    5. Generate delta input data to merge into the MemCache,
        this uses the gen command again,
MapReduce job MemCache_MergeDelta:
    6. Map over the delta for Reduce to merge into the MemCache,
        business logic can be applied, inserting, updating, deleting, etc,
        the keys are sorted on the last reduce iteration and merged in order,
Local job MemCache_Rollback:
    7. Roll back all the changes made since commit,
        this essentially discards all the delta that was merged,
    8. Load from DFS the previously committed MemCache,
        this essentially will return to the state of the last commit

Exec usage:
    [<MemCacheSize>] [<DeltaSize>]

-->


<SourceCode>
  <Jobs>
    
    <Job Name=`MemCache_Create` Custodian=`` Email=`` Description=`Create the MemCache and generate input data for the next job`>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            
            string schema =
                `int,int,int,int,int,int,int,int,int,int`
                + `,int,int,int,int,int,int,int,int,int,int`
                + `,int,int,int,int,int,int,int,int,int,int`
                + `,int`; // Default.
            const string DefaultMemCacheSize = `10MB`;
            
            public virtual void Local()
            {
                // Delete old data if any:
                Shell(@`Qizmt del dfs://Qizmt-TestMemCacheInputData*`);
                Shell(@`Qizmt del dfs://Qizmt-TestMemCacheChanges`);
                Shell(@`Qizmt memcache delete name=Qizmt-TestMemCache`, true);
                
                // Create the empty MemCache:
                Shell(@`Qizmt memcache create name=Qizmt-TestMemCache schema=` + schema);
                Qizmt_Log(` Created MemCache named 'Qizmt-TestMemCache' `);
                
                // Get argument:
                string sMemCacheSize = DefaultMemCacheSize;
                if(Qizmt_ExecArgs.Length > 0)
                {
                    sMemCacheSize = Qizmt_ExecArgs[0];
                }
                long MemCacheSize = CommandUtils.ParseLongByteSize(sMemCacheSize);
                
                // Generate input data for the next job to insert into the MemCache to populate it:
                Shell(@`Qizmt gen dfs://Qizmt-TestMemCacheInputDataPopulate@124 ` + MemCacheSize + ` rand=frand`);
                Qizmt_Log(` Generated ` + sMemCacheSize + ` of input data for the next job to insert into the MemCache to populate it `);
                
            }
        ]]>
      </Local>
    </Job>
    
    <Job Name=`MemCache_Insert` Custodian=`` Email=`` Description=`Insert the input data into the MemCache - populate memcache with some data`>
      <MemCache />
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int,int</KeyLength>
        <DFSInput>dfs://Qizmt-TestMemCacheInputDataPopulate@124</DFSInput>
        <DFSOutput></DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                // Set the output key to the first 8 bytes of the input record,
                // and the value to the whole input record.
                output.Add(ByteSlice.Prepare(line, 0, 8), line);
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            
            const string DefaultMemCacheSize = `10MB`;
            
            List<int[]> Inputs = new List<int[]>(2000);
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                while(values.MoveNext())
                {
                    // Just gather up the input for now so it can be sorted on last reduce iteration.
                    recordset rs = recordset.Prepare(values.Current);
                    int[] input = new int[31];
                    for(int i = 0; i < input.Length; i++)
                    {
                        input[i] = rs.GetInt();
                    }
                    Inputs.Add(input);
                    break; // Remove if you want duplicate keys in the MemCache.
                }
                
                if(StaticGlobals.Qizmt_Last)
                {
                    // On last reduce iteration, so sort the input and insert into the MemCache.
                    
                    // Need to sort the input keys since this is a grouped job.
                    Inputs.Sort(MyCompareKeys);
                    
                    string MemCacheName = `Qizmt-TestMemCache`;
                    int keyoffset = 0, keylen = 8;
                    int rowlen = 124;
                    
                    // Attach to this worker's view of the MemCache in a using-statement.
                    using(MemCache mc = MemCache.Attach(MemCacheName, Qizmt_ProcessID))
                    {
                        mc.MoveFirst(); // Start forward-only iterator for the following inserts.
                        
                        // Loop through all the input data and add to the MemCache in order.
                        foreach(int[] input in Inputs)
                        {
                            MemCache.Tuple t;
                            t = mc.InsertRow();
                            for(int i = 0; i < input.Length; i++)
                            {
                                t[i].SetInt(input[i]);
                            }
                        }
                        
                    }
                    
                    if(0 == Qizmt_ProcessID)
                    {
                        string sMemCacheSize = DefaultMemCacheSize;
                        if(Qizmt_ExecArgs.Length > 0)
                        {
                            sMemCacheSize = Qizmt_ExecArgs[0];
                        }
                        Qizmt_Log(` Loaded ` + sMemCacheSize + ` into MemCache `);
                    }
                    
                }
                
            }
            
            // Only comparing first 2 ints since they are the key.
            int MyCompareKeys(int[] x, int[] y)
            {
                int r = x[0].CompareTo(y[0]);
                if(0 == r)
                {
                    r = x[1].CompareTo(y[1]);
                }
                return r;
            }
            
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    
    <Job Name=`MemCache_Commit` Custodian=`` Email=`` Description=`Commit the MemCache changes`>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                // Commit the MemCache changes to DFS.
                // Alternatively, could execute commit every X hours from the Qizmt scheduler.
                {
                    DateTime lt = DateTime.Now;
                    Shell(@`Qizmt memcache commit name=Qizmt-TestMemCache`, true);
                    Qizmt_Log(`Committed MemCache; duration: ` + (DateTime.Now - lt));
                }
                
            }
        ]]>
      </Local>
    </Job>
    
    <Job Name=`MemCache_CreateDelta` Custodian=`` Email=`` Description=`Generate delta input data for the next job`>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            
            const string DefaultDeltaMemCacheSize = `10MB`;
            
            public virtual void Local()
            {
                // Delete old data if any:
                Shell(@`Qizmt del dfs://Qizmt-TestMemCacheInputData*`);
                
                // Get argument:
                string sDeltaMemCacheSize = DefaultDeltaMemCacheSize;
                if(Qizmt_ExecArgs.Length > 1)
                {
                    sDeltaMemCacheSize = Qizmt_ExecArgs[1];
                }
                long DeltaMemCacheSize = CommandUtils.ParseLongByteSize(sDeltaMemCacheSize);
                
                // Generate input data for the next job to insert into the MemCache to populate it:
                Shell(@`Qizmt gen dfs://Qizmt-TestMemCacheInputDataDelta@124 ` + DeltaMemCacheSize + ` rand=frand`);
                Qizmt_Log(` Generated ` + sDeltaMemCacheSize + ` of delta input data for the next job to merge into the MemCache `);
                
            }
        ]]>
      </Local>
    </Job>
    
    <Job Name=`MemCache_MergeDelta` Custodian=`` Email=`` Description=`Merge delta data into the MemCache (inserts, updates, deletes, custom business logic, etc.)`>
      <MemCache />
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int,int</KeyLength>
        <DFSInput>dfs://Qizmt-TestMemCacheInputDataDelta@124</DFSInput>
        <DFSOutput>dfs://Qizmt-TestMemCacheChanges@124</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                // Set the output key to the first 8 bytes of the input record,
                // and the value to the whole input record.
                output.Add(ByteSlice.Prepare(line, 0, 8), line);
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            
            List<int[]> Inputs = new List<int[]>(400); // Inputs to be merged into the MemCache.
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                while(values.MoveNext())
                {
                    // Just gather up the input for now so it can be sorted on last reduce iteration.
                    recordset rs = recordset.Prepare(values.Current);
                    int[] input = new int[31];
                    for(int i = 0; i < input.Length; i++)
                    {
                        input[i] = rs.GetInt();
                    }
                    Inputs.Add(input);
                    break; // Remove if you want duplicate keys in the MemCache.
                }
                
                if(StaticGlobals.Qizmt_Last)
                {
                    // On last reduce iteration, so sort the input and insert into the MemCache.
                    
                    // Need to sort the input keys since this is a grouped job.
                    Inputs.Sort(MyCompareKeys);
                    
                    string MemCacheName = `Qizmt-TestMemCache`;
                    int keyoffset = 0, keylen = 8;
                    int rowlen = 124;
                    
                    // Attach to this worker's view of the MemCache in a using-statement.
                    using(MemCache mc = MemCache.Attach(MemCacheName, Qizmt_ProcessID))
                    {
                        int CurrentMergeKeyIndex = 0;
                        for(mc.MoveFirst(); !mc.EOF; mc.MoveNext()) // Iterate through existing values in MemCache.
                        {
                            recordset.ResetBuffers(); // Reset recordset buffers in tight loop.
                            
                            MemCache.Tuple t;
                            t = mc.PeekRow(); // Peek at the current row, but don't read past it yet.
                            int k0 = t[0].GetInt(); // First part of existing row.
                            int k1 = t[1].GetInt(); // Second part of existing row.
                            
                            {
                                // If the first int of an existing row is evenly divisible by 8, delete it (for demo).
                                if(0 == (k0 % 8))
                                {
                                    mc.DeleteRow(); // Delete the row before it was read.
                                    continue; // We're done with this one so go to the next one.
                                }
                            }
                            
                            bool updated = false;
                            while(CurrentMergeKeyIndex < Inputs.Count)
                            {
                                // See if there are new keys to insert or keys to change.
                                int[] input = Inputs[CurrentMergeKeyIndex];
                                int kdiff = MyCompareKeys(input, new int[] {k0, k1 });
                                if(kdiff < 0) // Merge key is less than current one, so insert.
                                {
                                    if(updated)
                                    {
                                        throw new Exception(`Keys out of order (2662)`);
                                    }
                                    // Insert here.
                                    t = mc.InsertRow();
                                    for(int i = 0; i < input.Length; i++)
                                    {
                                        t[i].SetInt(input[i]);
                                    }
                                    {
                                        // Outputting changes to DFS: (new ones sorted in)
                                        recordset rs = recordset.Prepare();
                                        for(int i = 0; i < input.Length; i++)
                                        {
                                            rs.PutInt(t[i].GetInt());
                                        }
                                        output.Add(rs);
                                    }
                                    CurrentMergeKeyIndex++;
                                    continue;
                                }
                                else if(kdiff == 0) // Merge key is the same as the current one, so update.
                                {
                                    // They're the same key, so update it.
                                    t = mc.ReadRow(); // Read it so it's included, and then update it.
                                    // Leave first 2 ints.
                                    // Only take the highest value of the rest of the ints.
                                    for(int i = 2; i < input.Length; i++)
                                    {
                                        if(input[i] > t[i].GetInt())
                                        {
                                            t[i].SetInt(input[i]);
                                        }
                                    }
                                    CurrentMergeKeyIndex++;
                                    {
                                        // Outputting changes to DFS: (updated ones)
                                        recordset rs = recordset.Prepare();
                                        for(int i = 0; i < input.Length; i++)
                                        {
                                            rs.PutInt(t[i].GetInt());
                                        }
                                        output.Add(rs);
                                    }
                                    updated = true;
                                    continue; // Let it keep updating the same one if same keys.
                                }
                                break;
                            }
                            if(updated)
                            {
                                continue;
                            }
                        }
                        // Add extra, higher keys after the last one.
                        while(CurrentMergeKeyIndex < Inputs.Count)
                        {
                            recordset.ResetBuffers(); // Reset recordset buffers in tight loop.
                            
                            int[] input = Inputs[CurrentMergeKeyIndex];
                            MemCache.Tuple t;
                            t = mc.InsertRow();
                            for(int i = 0; i < input.Length; i++)
                            {
                                t[i].SetInt(input[i]);
                            }
                            CurrentMergeKeyIndex++;
                            {
                                // Outputting changes to DFS: (new ones at end)
                                recordset rs = recordset.Prepare();
                                for(int i = 0; i < input.Length; i++)
                                {
                                    rs.PutInt(t[i].GetInt());
                                }
                                output.Add(rs);
                            }
                            
                        }
                    }
                    
                    if(0 == Qizmt_ProcessID)
                    {
                        Qizmt_Log(` Merged data into the MemCache `);
                    }
                    
                }
                
            }
            
            // Only comparing first 2 ints since they are the key.
            int MyCompareKeys(int[] x, int[] y)
            {
                int r = x[0].CompareTo(y[0]);
                if(0 == r)
                {
                    r = x[1].CompareTo(y[1]);
                }
                return r;
            }
            
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    
    <Job Name=`MemCache_Rollback` Custodian=`` Email=`` Description=`Rollback the MemCache changes and then load the previously committed changes`>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            
            string schema =
                `int,int,int,int,int,int,int,int,int,int`
                + `,int,int,int,int,int,int,int,int,int,int`
                + `,int,int,int,int,int,int,int,int,int,int`
                + `,int`; // Default.
            
            public virtual void Local()
            {
                // Roll back all the changes made after commit (merging in delta)
                {
                    DateTime lt = DateTime.Now;
                    Shell(@`Qizmt memcache rollback name=Qizmt-TestMemCache`, true);
                    Qizmt_Log(`MemCache rollback; duration: ` + (DateTime.Now - lt));
                }
                
                // Clean up:
                Shell(@`Qizmt del dfs://Qizmt-TestMemCacheInputData*`);
                
                // Load from DFS the previously committed MemCache (return to state of last commit)
                {
                    DateTime lt = DateTime.Now;
                    Shell(@`Qizmt memcache load name=Qizmt-TestMemCache`, true);
                    Qizmt_Log(`MemCache load; duration: ` + (DateTime.Now - lt));
                }
                
                Qizmt_Log(``); // Blank line.
                // Output first few records of changes:
                Qizmt_Log(Shell(@`Qizmt head dfs://Qizmt-TestMemCacheChanges@` + schema));
                
            }
        ]]>
      </Local>
    </Job>
    
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region FTE
            alljobfiles.Add(@"Qizmt-FTE.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`CleanUp` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del Qizmt-FTE_Input.txt`);
                Shell(@`Qizmt del Qizmt-FTE_Output.txt`);
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-FTE_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.
                dfsoutput.WriteLine(`200,John Smith,1`);
                dfsoutput.WriteLine(`300,Ashley West,3`);
                dfsoutput.WriteLine(`300,Ashley West,9`);
                dfsoutput.WriteLine(`200,John Smith,10`);
                dfsoutput.WriteLine(`200,John Smith,12`);
                dfsoutput.WriteLine(`201,Mary Smith,4`);
                dfsoutput.WriteLine(`202,Jane Ash,2`);
                dfsoutput.WriteLine(`100,Joe Green,1`);
                dfsoutput.WriteLine(`201,Mary Smith,85`);
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-FTE` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://Qizmt-FTE_Input.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-FTE_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <FaultTolerantExecution>
        <Mode>enabled</Mode> <!-- enabled or disabled -->
        <MapInputOrder>shuffle</MapInputOrder>   <!-- next or shuffle map input files -->
        <HeartBeatTimeout>30000</HeartBeatTimeout> <!-- how often in milliseconds a worker will report a heartbeat -->
        <HeartBeatRetries>10</HeartBeatRetries> <!-- how many times a worker tries to write to the local drive -->
        <HeartBeatExpired>120000</HeartBeatExpired> <!-- how many milliseconds take it take for a heartbeat to expire -->
        <TattleTimeout>3000</TattleTimeout> <!-- how often in milliseconds a worker inquires about rogue hosts -->
      </FaultTolerantExecution>
        <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                mstring sLine = mstring.Prepare(line);
                int userid = sLine.NextItemToInt(',');
                mstring name = sLine.NextItemToString(',');
                int pageid = sLine.NextItemToInt(',');
                
                recordset rkey = recordset.Prepare();               
                rkey.PutInt(userid);
                
                recordset rvalue = recordset.Prepare();
                rvalue.PutInt(pageid);
                
                output.Add(rkey, rvalue);
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rkey = recordset.Prepare(key);
                int userid = rkey.GetInt();
                 
                mstring sline = mstring.Prepare(userid);
                sline = sline.AppendM(':');
                
                for(int i = 0; i < values.Length; i++)
                {
                    recordset rvalue = recordset.Prepare(values.Items[i]);
                    int pageid = rvalue.GetInt();
                    
                    if(i > 0)
                    {
                        sline.AppendM(',');
                    }
                    
                    sline.AppendM(pageid);                    
                }
                
                output.Add(sline);
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region LargeWordCount
            alljobfiles.Add(@"Qizmt-LargeWordCount.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    
    <!-- Check arguments: -->
    <Job Name=`checkargs` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                // Ensure expected arguments.
                if(Qizmt_ExecArgs.Length <= 0)
                {
                    throw new Exception(`   ***   Expected argument: <expand-size>   `);
                }
                CommandUtils.ParseLongByteSize(Qizmt_ExecArgs[0]); // Test it.
            }
        ]]>
      </Local>
    </Job>
    
    <!-- Generate initial words: -->
    <Job Name=`genwords` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                try
                {
                    Qizmt_Log(Shell(`Qizmt wordgen Qizmt-LargeWordCount-1MB.txt 1MB rand=frand`, false));
                }
                catch
                {
                }
            }
        ]]>
      </Local>
    </Job>
    
    <!-- Expand words: -->
    <Job Name=`expandwords_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                try
                {
                    Shell(`Qizmt del Qizmt-LargeWordCount-AllWords.txt`, true);
                }
                catch
                {
                }
            }
        ]]>
      </Local>
    </Job>
    <!--
        Output size should be roughly the size requested.
        However, if [0B] is shown to output, it may be much less.
    -->
    <Job Name=`expandwords` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>dfs://Qizmt-LargeWordCount-1MB.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-LargeWordCount-AllWords.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            List<byte> keybytes = new List<byte>(); // Reused buffer for key.
            bool first = true; // Keep track of first map iteration.
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                if(first)
                {
                    // Initialize random on first map iteration.
                    first = false;
                    FRandom.Seed(unchecked(Qizmt_ProcessID * 5 + 2231));
                }
                keybytes.Clear(); // Clear to prepare for key.
                Entry.ToBytesAppend(FRandom.Next(), keybytes); // Random number key.
                output.Add(ByteSlice.Prepare(keybytes), line); // Map all input as value.
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            List<byte[]> rows = new List<byte[]>(); // Keep track of all values from input file.
            long totalBytesThisProcess = 0;
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                while(values.MoveNext())
                {
                    rows.Add(values.Current.ToBytes()); // Keep a copy of all values from input file.
                    totalBytesThisProcess += values.Current.Length; // Keep track of total size.
                }
                
                // Use all values on last reduce iteration to expand the data.
                if(StaticGlobals.Qizmt_Last)
                {
                    FRandom.Seed(unchecked((Qizmt_ProcessID * 5 + 828) * DateTime.Now.Minute)); // Initialize random.
                    long expandsize = CommandUtils.ParseLongByteSize(Qizmt_ExecArgs[0]); // Expand size provided by user argument.
                    long thisexpand = expandsize / Qizmt_ProcessCount; // Expand size for this process (total divided by process count).
                    int avgreclen = checked((int)(totalBytesThisProcess / rows.Count)); // Average record length.
                    long thisoutputrecords = thisexpand / avgreclen; // Roughly the number of records this process should output.
                    for(long thisoutputsofar = 0; thisoutputsofar < thisoutputrecords; thisoutputsofar++)
                    {
                        // Randomly select values to output.
                        int upper = rows.Count;
                        int i = FRandom.Next(0, upper);
                        output.WriteLine(ByteSlice.Prepare(rows[i]));
                    }
                }
                
                
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    
    <!-- Word count: -->
    <Job Name=`Cleanup` Custodian=`` email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            Shell(@`Qizmt del Qizmt-LargeWordCount-WordCounts.txt`, true); // Clean previous run.            
        }
        ]]>
      </Local>
    </Job>
    <Job Name=`WordCountByPartialReduce` Custodian=`` email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>16</KeyLength>
        <DFSInput>dfs://Qizmt-LargeWordCount-AllWords.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-LargeWordCount-WordCounts.txt</DFSOutput>
      </IOSettings>
      <PartialReduce/>        
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {
              mstring sLine = mstring.Prepare(line); // Input line as mstring.
              mstringarray parts = sLine.SplitM(' '); // Split input by space.
              
             for(int i = 0; i < parts.Length; i++) // For each word...
             {
                    mstring word = parts[i]; // Current word.
                    
                    if(word.Length > 0 && word.Length <= 16) // If appropriate word length.
                    {                        
                        // Lowercase word as key; and 1 as value to sum.
                        recordset rs = recordset.Prepare();
                        rs.PutInt(1);
                        output.Add(word.ToLowerM(), rs); 
                    }                                 
             }
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[         
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              int totalCount = 0;
              
              if(StaticGlobals.ExecutionContext == ExecutionContextType.PARTIALREDUCE)
              {
                  // Partial reduce to combine counts.
                  totalCount = values.Length;
                 
                  recordset rs = recordset.Prepare();
                  rs.PutInt(totalCount); // Update value with intermediate count.
                  output.Add(key, rs.ToByteSlice());
              }
              else
              {
			      // Add values together from partial reduce.
                  for(int i = 0; i < values.Length; i++)
                  {
                      recordset rs = recordset.Prepare(values[i].Value);
                      int count = rs.GetInt();
                      totalCount += count;
                  }
                  
                  // Final output with count of occurrences of this word.
                  mstring outline = mstring.Prepare(UnpadKey(key));
                  outline = outline.AppendM(',').AppendM(totalCount);
                  output.Add(outline);
              }
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region Qizmt-AddUsingReferences
            alljobfiles.Add(@"Qizmt-AddUsingReferences.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`ExCustomDLL_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del custom.dll`);                // deleting dll from CAC
                Shell(@`Qizmt del ExCustomDLL_Input.txt`);     // deleting input file from DFS in order to regenerate new one
                Shell(@`Qizmt del ExCustomDLL_Output.txt`);    // deleting old output file from DFS in order to produce new one
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
        {            /*
            This example puts a simple `Any CPU` DLL into CAC (Cluster Assembly Cache in MR.DFS).
            CAC has the same  idea as The Global Assembly Cache or GAC which is a machine-wide .NET assemblies cache for Microsoft's CLR platform,
            whith the difference that is made for DFS(Distributed File System) platform.
            For example , two jobs can reference the same dll that is uploaded to the MR.DFS, at the same time.
            Select `Any CPU` when building a DLL for CAC.
            DLL`s put into the CAC are replicated on every machine of the cluster before the put command returns.
            Once DLL is in CAC, it may be referenced from mapreducer jobs, local jobs and remote jobs.
            CAC DLL`s may be written in any .net language, e.g. C#, VB.NET, C++ CLI, etc.
            */
            string localdir = @`\\` + System.Net.Dns.GetHostName() + @`\c$\temp\Qizmt`;
            if(!System.IO.Directory.Exists(localdir))
            {
                System.IO.Directory.CreateDirectory(localdir);
            }
            
            string fn = `custom.dll`;
            string localfn = localdir + @`\` + Guid.NewGuid().ToString() + fn;
            
  /*
            // Code for custom.dll:
namespace custom
{
    public class Custom
    {
        public static int x;
        public static string ReverseString(string x)
        {
            int len = x.Length;
            char[] arr = new char[len];

            for (int i = 0; i < len; i++)
            {
                arr[i] = x[len - 1 - i];
            }

            return new string(arr);

        }
        public static string StringFoo()
        {
            return `Testing custom DLL`;
        }
    }
}

            */
            
            /* In the next step,  string testdlldataAnyCPU is assigned binary data converted to base64.
            
                How this is done?
                
                -We can hardcode string base64 in to the job's xml file, then we can convert string to bynary using  System.Convert.FromBase64String()
                which returns byte array. Byte array can be writen to the file using  System.IO.File.WriteAllBytes(localfn, testdlldata). File can be put in the CAC usig `put` command and
                net path.
                
                 What is the purpose?
                 
                 -This is made in order to recreate custom.dll in 
                each subsequent run and make example self-contained. */
            string testdlldataAnyCPU = `TVqQAAMAAAAEAAAA//8AALgAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAA4fug4AtAnNIbgBTM0hVGhpcyBwcm9ncmFtIGNhbm5vdCBiZSBydW4gaW4gRE9TIG1vZGUuDQ0KJAAAAAAAAABQRQAATAEDADD+H0wAAAAAAAAAAOAAAiELAQgAAAgAAAAGAAAAAAAAbicAAAAgAAAAQAAAAABAAAAgAAAAAgAABAAAAAAAAAAEAAAAAAAAAACAAAAAAgAAAAAAAAMAQIUAABAAABAAAAAAEAAAEAAAAAAAABAAAAAAAAAAAAAAABwnAABPAAAAAEAAAPgCAAAAAAAAAAAAAAAAAAAAAAAAAGAAAAwAAACEJgAAHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAACAAAAAAAAAAAAAAACCAAAEgAAAAAAAAAAAAAAC50ZXh0AAAAdAcAAAAgAAAACAAAAAIAAAAAAAAAAAAAAAAAACAAAGAucnNyYwAAAPgCAAAAQAAAAAQAAAAKAAAAAAAAAAAAAAAAAABAAABALnJlbG9jAAAMAAAAAGAAAAACAAAADgAAAAAAAAAAAAAAAAAAQAAAQgAAAAAAAAAAAAAAAAAAAABQJwAAAAAAAEgAAAACAAUAnCAAAOgFAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABMwBQAvAAAAAQAAEQJvEAAACgoGjRMAAAELFgwrEgcIAgYXWQhZbxEAAAqdCBdYDAgGMuoHcxIAAAoqGnIBAABwKh4CKBMAAAoqAABCU0pCAQABAAAAAAAMAAAAdjIuMC41MDcyNwAAAAAFAGwAAAAAAgAAI34AAGwCAABsAgAAI1N0cmluZ3MAAAAA2AQAACgAAAAjVVMAAAUAABAAAAAjR1VJRAAAABAFAADYAAAAI0Jsb2IAAAAAAAAAAgAAAVcVAgAJAAAAAPoBMwAWAAABAAAAEwAAAAIAAAABAAAAAwAAAAEAAAATAAAADQAAAAEAAAABAAAAAQAAAAAACgABAAAAAAAGADMALAAGAGwAWgAGAIMAWgAGAKAAWgAGAL8AWgAGANgAWgAGAPEAWgAGAAwBWgAGACcBWgAGAF8BQAEGAHMBQAEGAIEBWgAGAJoBWgAGAMoBtwE7AN4BAAAGAA0C7QEGAC0C7QEGAEsCLAAGAF0CLAAAAAAAAQAAAAAAAQABAAEAEAAVABwABQABAAEAFgA6AAoAUCAAAAAAlgA8AA0AAQCLIAAAAACWAEoAEgACAJIgAAAAAIYYVAAWAAIAAAABADoAEQBUABoAGQBUABoAIQBUABoAKQBUABoAMQBUABoAOQBUABoAQQBUABoASQBUABoAUQBUAB8AWQBUABoAYQBUABoAaQBUABoAcQBUACQAgQBUACoAiQBUABYAkQBSAi8AkQBiAjMAkQBUADgACQBUABYALgALAEUALgATAFEALgAbAFEALgAjAFEALgArAEUALgAzAFcALgA7AFEALgBLAFEALgBTAG8ALgBjAJkALgBrAKYALgBzAK8ALgB7ALgAPgAEgAAAAQAAAAAAAAAAAAAAAAAcAAAAAgAAAAAAAAAAAAAAAQAjAAAAAAAAAAA8TW9kdWxlPgBjdXN0b20uZGxsAEN1c3RvbQBjdXN0b20AbXNjb3JsaWIAU3lzdGVtAE9iamVjdAB4AFJldmVyc2VTdHJpbmcAU3RyaW5nRm9vAC5jdG9yAFN5c3RlbS5SZWZsZWN0aW9uAEFzc2VtYmx5VGl0bGVBdHRyaWJ1dGUAQXNzZW1ibHlEZXNjcmlwdGlvbkF0dHJpYnV0ZQBBc3NlbWJseUNvbmZpZ3VyYXRpb25BdHRyaWJ1dGUAQXNzZW1ibHlDb21wYW55QXR0cmlidXRlAEFzc2VtYmx5UHJvZHVjdEF0dHJpYnV0ZQBBc3NlbWJseUNvcHlyaWdodEF0dHJpYnV0ZQBBc3NlbWJseVRyYWRlbWFya0F0dHJpYnV0ZQBBc3NlbWJseUN1bHR1cmVBdHRyaWJ1dGUAU3lzdGVtLlJ1bnRpbWUuSW50ZXJvcFNlcnZpY2VzAENvbVZpc2libGVBdHRyaWJ1dGUAR3VpZEF0dHJpYnV0ZQBBc3NlbWJseVZlcnNpb25BdHRyaWJ1dGUAQXNzZW1ibHlGaWxlVmVyc2lvbkF0dHJpYnV0ZQBTeXN0ZW0uRGlhZ25vc3RpY3MARGVidWdnYWJsZUF0dHJpYnV0ZQBEZWJ1Z2dpbmdNb2RlcwBTeXN0ZW0uUnVudGltZS5Db21waWxlclNlcnZpY2VzAENvbXBpbGF0aW9uUmVsYXhhdGlvbnNBdHRyaWJ1dGUAUnVudGltZUNvbXBhdGliaWxpdHlBdHRyaWJ1dGUAU3RyaW5nAGdldF9MZW5ndGgAQ2hhcgBnZXRfQ2hhcnMAACVUAGUAcwB0AGkAbgBnACAAYwB1AHMAdABvAG0AIABEAEwATAAAACRGPWtDtNtNmxIqQyaX+MEACLd6XFYZNOCJAgYIBAABDg4DAAAOAyAAAQQgAQEOBCABAQIFIAEBET0EIAEBCAMgAAgEIAEDCAUgAQEdAwYHAwgdAwgLAQAGY3VzdG9tAAAFAQAAAAAXAQASQ29weXJpZ2h0IMKpICAyMDEwAAApAQAkMDA4MjI5YjYtNjJlZC00NWI2LWJkOTAtOWY5MTAxNWU3YTE1AAAMAQAHMS4wLjAuMAAACAEAAgAAAAAACAEACAAAAAAAHgEAAQBUAhZXcmFwTm9uRXhjZXB0aW9uVGhyb3dzAQAAAAAAMP4fTAAAAAACAAAAeQAAAKAmAACgCAAAUlNEUyzGo95U+ElPm8JUo30clfQEAAAAQzpcVXNlcnNcdm1hcnRzaW5jaHlrXERvY3VtZW50c1xWaXN1YWwgU3R1ZGlvIDIwMDhcUHJvamVjdHNcY3VzdG9tXGN1c3RvbVxvYmpcUmVsZWFzZVxjdXN0b20ucGRiAAAAAEQnAAAAAAAAAAAAAF4nAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAABQJwAAAAAAAAAAAAAAAF9Db3JEbGxNYWluAG1zY29yZWUuZGxsAAAAAAD/JQAgQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAQAAAAGAAAgAAAAAAAAAAAAAAAAAAAAQABAAAAMAAAgAAAAAAAAAAAAAAAAAAAAQAAAAAASAAAAFhAAACgAgAAAAAAAAAAAACgAjQAAABWAFMAXwBWAEUAUgBTAEkATwBOAF8ASQBOAEYATwAAAAAAvQTv/gAAAQAAAAEAAAAAAAAAAQAAAAAAPwAAAAAAAAAEAAAAAgAAAAAAAAAAAAAAAAAAAEQAAAABAFYAYQByAEYAaQBsAGUASQBuAGYAbwAAAAAAJAAEAAAAVAByAGEAbgBzAGwAYQB0AGkAbwBuAAAAAAAAALAEAAIAAAEAUwB0AHIAaQBuAGcARgBpAGwAZQBJAG4AZgBvAAAA3AEAAAEAMAAwADAAMAAwADQAYgAwAAAAOAAHAAEARgBpAGwAZQBEAGUAcwBjAHIAaQBwAHQAaQBvAG4AAAAAAGMAdQBzAHQAbwBtAAAAAAAwAAgAAQBGAGkAbABlAFYAZQByAHMAaQBvAG4AAAAAADEALgAwAC4AMAAuADAAAAA4AAsAAQBJAG4AdABlAHIAbgBhAGwATgBhAG0AZQAAAGMAdQBzAHQAbwBtAC4AZABsAGwAAAAAAEgAEgABAEwAZQBnAGEAbABDAG8AcAB5AHIAaQBnAGgAdAAAAEMAbwBwAHkAcgBpAGcAaAB0ACAAqQAgACAAMgAwADEAMAAAAEAACwABAE8AcgBpAGcAaQBuAGEAbABGAGkAbABlAG4AYQBtAGUAAABjAHUAcwB0AG8AbQAuAGQAbABsAAAAAAAwAAcAAQBQAHIAbwBkAHUAYwB0AE4AYQBtAGUAAAAAAGMAdQBzAHQAbwBtAAAAAAA0AAgAAQBQAHIAbwBkAHUAYwB0AFYAZQByAHMAaQBvAG4AAAAxAC4AMAAuADAALgAwAAAAOAAIAAEAQQBzAHMAZQBtAGIAbAB5ACAAVgBlAHIAcwBpAG8AbgAAADEALgAwAC4AMAAuADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAADAAAAHA3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==`;
    /*  CODE THAT CONVERTS  binary data from custom.dll in to base64 string and writes it to the file
            
            
            System.IO.FileStream inFile;
            byte[] binaryData;


            inFile = new System.IO.FileStream(inputFile(some custom dll), System.IO.FileMode.Open, System.IO.FileAccess.Read);
            binaryData = new Byte[inFile.Length];
            long bytesRead = inFile.Read(binaryData, 0, (int)inFile.Length);
            inFile.Close();
            string base64String = System.Convert.ToBase64String(binaryData, 0, binaryData.Length);

          System.IO.StreamWriter outFile;
			try
			{
				outFile = new System.IO.StreamWriter(outputFile(text file), false, System.Text.Encoding.ASCII);
				outFile.Write(base64String);
				outFile.Close();
			}
			catch (System.Exception exp)
			{
				System.Console.WriteLine(`{0}`, exp.Message);
			}
            
            
            */
            byte[] testdlldata = System.Convert.FromBase64String(testdlldataAnyCPU);
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
    <Job Name=`ExCustomDLL_Preprocessing` Custodian=`` Email=``>
    <Add Reference='custom.dll' Type='dfs' />
    <Add Reference=`Microsoft.VisualBasic.dll` Type=`system` />
      <Using>custom</Using>
      <Using>Microsoft.VisualBasic</Using>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                  if(32 != Strings.Asc(' '))
            {
                throw new Exception(`Local: (32 != Microsoft.VisualBasic.Strings.Asc(' '))`);
            }
            

                Qizmt_Log(Custom.StringFoo());
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`ExCustomDLL_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
   <Add Reference='custom.dll' Type='dfs' />
    <Add Reference=`Microsoft.VisualBasic.dll` Type=`system` />
      <Using>custom</Using>
      <Using>Microsoft.VisualBasic</Using>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://ExCustomDLL_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {

              if(32 != Strings.Asc(' '))
                {
                   throw new Exception(`Local: (32 != Microsoft.VisualBasic.Strings.Asc(' '))`);
                }
            
                //Create sample data.
                dfsoutput.WriteLine(Custom.StringFoo());
                dfsoutput.WriteLine(`This is PolindromFinder example with use of the custom made DLL`);
                 dfsoutput.WriteLine(`Polindromes are words that can be read the same way in either direction`);
                 dfsoutput.WriteLine(`Such as aibohphobia alula cammac kayak level radar refer`);
                dfsoutput.WriteLine(`Use of custom made DLLs together with powerful API of Qizmt makes life easier`);
                dfsoutput.WriteLine(`What other features we can implement in the future?`);
                dfsoutput.WriteLine(`What else is needed to improve performance?`);
                
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`ExCustomDLL` Custodian=`` Email=``>
    <Add Reference='custom.dll' Type='dfs' />
    <Add Reference=`Microsoft.VisualBasic.dll` Type=`system` />
      <Using>custom</Using>
      <Using>Microsoft.VisualBasic</Using>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>40</KeyLength>
        <DFSInput>dfs://ExCustomDLL_Input.txt</DFSInput>
        <DFSOutput>dfs://ExCustomDLL_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(' ');
               
               for(int i=0; i<parts.Length;i++)
               {
                   string word ;
                   word = parts[i].ToString();
                   string rword ;
                    rword = Custom.ReverseString(word);
                    
                    if ( rword == word )
                    {
                        recordset count = recordset.Prepare();
                        count.PutInt(1);
                        output.Add(parts[i],count);
                    }
                   
                   
               }
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {

                if(32 != Strings.Asc(' '))
                  {
                     throw new Exception(`Local: (32 != Microsoft.VisualBasic.Strings.Asc(' '))`);
                  }
            
                mstring Polindrome = mstring.Prepare(UnpadKey(key));
                int count = 0 ;
                for(int i = 0; i< values.Length; i++)
                {
                    
                    count++;
                    
                    
                }
                
                Polindrome.AppendM(` - `);
                Polindrome.AppendM(count);
                
                output.Add(Polindrome);
                
               
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`ExCustomDLL_DisplayInputData` Custodian=`` Email=`` Description=`Display input data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://ExCustomDLL_Input.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display input.
                Qizmt_Log(`Input:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`ExCustomDLL_DisplayOutputData` Custodian=`` Email=`` Description=`Display output data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://ExCustomDLL_Output.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display output.
                Qizmt_Log(`Output:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>

".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region Qizmt-WordCountMemCache.xml
            alljobfiles.Add(@"Qizmt-WordCountMemCache.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`Qizmt-WordCountMemCache_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del Qizmt-WordCountMemCache1_Input.txt`);
                Shell(@`Qizmt del Qizmt-WordCountMemCache2_Input.txt`);
                Shell(@`Qizmt del Qizmt-WordCountMemCache3_Input.txt`);
                Shell(@`Qizmt del Qizmt-WordCountMemCache_Output.txt`);
              
               
           
            }
        ]]>
      </Local>
    </Job>
   
     <Job Name=`Qizmt-WordCountMemCache_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WordCountMemCache1_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.
                dfsoutput.WriteLine(`In computer science a cache pronounced kækash is a component that improves performance by transparently storing data such that future requests for that data can be served faster`);
                dfsoutput.WriteLine(`The data that is stored within a cache might be values that have been computed earlier or duplicates of original values that are stored elsewhere`);
                dfsoutput.WriteLine(`regression test`);
              
            }
        ]]>
      </Remote>
    </Job>
     <Job Name=`Qizmt-WordCountMemCache_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WordCountMemCache2_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.
                dfsoutput.WriteLine(`As opposed to a buffer which is managed explicitly by a client a cache stores data transparently`);
                dfsoutput.WriteLine(` Nevertheless caches have proven extremely effective in many areas of computing because `);
                              
            }
        ]]>
      </Remote>
    </Job>
     <Job Name=`Qizmt-WordCountMemCache_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WordCountMemCache3_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.
                dfsoutput.WriteLine(`Nevertheless caches have proven extremely effective in many areas of computing because`);
                dfsoutput.WriteLine(` if data is requested that is physically stored close to data that has been requested already`);
              
            }
    
      ]]>
      </Remote>
    </Job>
    
    <Job Name=`Qizmt-WordCountMemCache` Custodian=`` Email=``>
     <MemCache />
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>16</KeyLength>
        <DFSInput>dfs://Qizmt-WordCountMemCache1_Input.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-WordCountMemCache_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                
                 mstring sLine= mstring.Prepare(line);
                 mstringarray parts = sLine.SplitM(' ');
              
                 for(int i=0; i < parts.Length; i++)
                     {
                        mstring word = parts[i];
                    
                        if(word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
                          {                        
                            output.Add(word.ToLowerM(), mstring.Prepare(1)); 
                           }   
                     }
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          struct SKeyVal
            {
             public   string ikey;
             public   int ival;
            }
          
            List<SKeyVal> Inputs = new List<SKeyVal>(); 
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
               
                            
                    mstring rs = mstring.Prepare(key);
                    SKeyVal s = new SKeyVal();
                    s.ikey = rs.ToString();
                    
                    for (int i=0; i < values.Length; i ++)
                    {
                        s.ival++;
                        
                    }
                   
                      
                   
                    Inputs.Add(s);
                                
                
                if(StaticGlobals.Qizmt_Last)
                {
                    // On last reduce iteration, so sort the input and insert into the MemCache.
                    
                                 Inputs.Sort(MyCompareKeys1);  
                    
                    string MemCacheName = `Qizmt-WordCountMemCache`;
                    int keyoffset = 0, keylen = 16;
                    int rowlen = 36;
                    
                    // Attach to this worker's view of the MemCache in a using-statement.
                    using(MemCache mc = MemCache.Attach(MemCacheName, Qizmt_ProcessID))
                    {
                        int CurrentMergeKeyIndex = 0;
                        for(mc.MoveFirst(); !mc.EOF; mc.MoveNext()) // Iterate through existing values in MemCache.
                        {
                            MemCache.Tuple t;
                            t = mc.PeekRow(); // Peek at the current row, but don't read past it yet.
                            string k0 = t[0].GetString(); // First part of existing row.
                                                       
                            
                                
                            bool updated = false;
                            while(CurrentMergeKeyIndex < Inputs.Count)
                            {
                                // See if there are new keys to insert or keys to change.
                                int kdiff = MyCompareKeys((Inputs[CurrentMergeKeyIndex].ikey).ToString(),
                                    k0.ToString() );
                                if(kdiff < 0) // Merge key is less than current one, so insert.
                                {
                                    if(updated)
                                    {
                                        throw new Exception(`Keys out of order (2662)`);
                                    }
                                    // Insert here.
                                    t = mc.InsertRow();
                                    t[0].SetString(Inputs[CurrentMergeKeyIndex].ikey); // First part of input.
                                    t[1].SetInt(Inputs[CurrentMergeKeyIndex].ival);//Second part of input.
                                    mstring sLine = mstring.Prepare(Inputs[CurrentMergeKeyIndex].ikey);
                                    sLine.AppendM(`->`);
                                    sLine.AppendM(Inputs[CurrentMergeKeyIndex].ival);
                                    output.Add(sLine);
                                    
                                    
                                    CurrentMergeKeyIndex++;
                                    continue;
                                }
                                else if(kdiff == 0) // Merge key is the same as the current one, so update.
                                {
                                    // They're the same, so update it.
                                    t = mc.ReadRow(); // Read it so it's included, and then update it.
                                    
                                    t[0].SetString(Inputs[CurrentMergeKeyIndex].ikey);
                                    
                                    int k1 = t[1].GetInt();
                                    
                                    t[1].SetInt(Inputs[CurrentMergeKeyIndex].ival+k1);
                                    
                                    // Creating output:
                                    mstring sLine = mstring.Prepare(Inputs[CurrentMergeKeyIndex].ikey);
                                    sLine.AppendM(`->`);
                                    sLine.AppendM(Inputs[CurrentMergeKeyIndex].ival+k1);
                                    output.Add(sLine);
                                    CurrentMergeKeyIndex++;
                                    updated = true;
                                    continue; // Let it keep updating the same one if same keys.
                                }
                                break;
                            }
                            if(updated)
                            {
                                continue;
                            }
                        }
                        // Add extra, higher keys after the last one.
                        while(CurrentMergeKeyIndex < Inputs.Count)
                        {
                            MemCache.Tuple t;
                            t = mc.InsertRow();
                            t[0].SetString(Inputs[CurrentMergeKeyIndex].ikey);
                            t[1].SetInt(Inputs[CurrentMergeKeyIndex].ival);
                            // Creating output:
                            mstring sLine = mstring.Prepare(Inputs[CurrentMergeKeyIndex].ikey);
                                    sLine.AppendM(`->`);
                                    sLine.AppendM(Inputs[CurrentMergeKeyIndex].ival);
                                    output.Add(sLine);
                            
                            CurrentMergeKeyIndex++;
                        }
                    }
                    
                    if(0 == Qizmt_ProcessID)
                    {
                        Qizmt_Log(` Merged data into the MemCache `);
                    }
                    
                }
                
   
                
                
            }
            
            int MyCompareKeys1(SKeyVal x,SKeyVal y)
            {
                int r = (x.ikey).CompareTo(y.ikey);
                
                return r;
                
            }
            
              int MyCompareKeys(string x,string  y)
            {
                int r = x.CompareTo(y);
              
                return r;
            }
                       
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    
      <Job Name=`MemCache_Commit` Custodian=`` Email=`` Description=`Commit the MemCache changes`>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                // Commit the MemCache changes to DFS.
                // Alternatively, could execute commit every X hours from the Qizmt scheduler.
                {
                    DateTime lt = DateTime.Now;
                    Shell(@`Qizmt memcache commit name=Qizmt-TestMemCache`, true);
                    Qizmt_Log(`Committed MemCache; duration: ` + (DateTime.Now - lt));
                }
                
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`Qizmt-WordCountMemCache_DisplayInputData` Custodian=`` Email=`` Description=`Display input data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://Qizmt-WordCountMemCache1_Input.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display input.
                Qizmt_Log(`Input:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-WordCountMemCache_DisplayOutputData` Custodian=`` Email=`` Description=`Display output data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://Qizmt-WordCountMemCache_Output.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display output.
                Qizmt_Log(`Output:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>

".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region Qizmt-Wrapper-WordCountMemCache.xml
            alljobfiles.Add(@"Qizmt-Wrapper-WordCountMemCache.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`ExecXPath_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            if(Qizmt_ExecArgs.Length > 0)
            {
               
                
                if (Qizmt_ExecArgs[0] == `1`)
                {
  string output = Shell(@`Qizmt exec  ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSInput=Qizmt-WordCountMemCache1_Input.txt`` ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSOutput=Qizmt-WordCountMemCache_Output.txt``  Qizmt-WordCountMemCache.xml`);
                 Qizmt_Log(output);
                }
            
             if (Qizmt_ExecArgs[0] == `2`)
                {
   string output = Shell(@`Qizmt exec  ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSInput=Qizmt-WordCountMemCache2_Input.txt``   ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSOutput=Qizmt-WordCountMemCache_Output.txt``  Qizmt-WordCountMemCache.xml`);
                Qizmt_Log(output);
                }
             if (Qizmt_ExecArgs[0] == `3`)
             {
      string output = Shell(@`Qizmt exec  ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSInput=Qizmt-WordCountMemCache3_Input.txt`` ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSOutput=Qizmt-WordCountMemCache_Output.txt``  Qizmt-WordCountMemCache.xml`);
             Qizmt_Log(output);
             }
            
            }
            else
            {
              
                 string schema =
                `string(16),int`; // Default.
            const string DefaultMemCacheSize = `10KB`;
            
                  // Delete old data if any:
            //  Shell(@`Qizmt del dfs://Qizmt-WordCountMemCache*`);
              Shell(@`Qizmt memcache delete name=Qizmt-WordCountMemCache`, true);
                
                // Create the empty MemCache:
              Shell(@`Qizmt memcache create name=Qizmt-WordCountMemCache schema=` + schema);
              Qizmt_Log(` Created MemCache named 'Qizmt-WordCountMemCache' `);
              
       string output = Shell(@`Qizmt exec  ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSInput=Qizmt-WordCountMemCache1_Input.txt`` ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSOutput=Qizmt-WordCountMemCache_Output.txt``  Qizmt-WordCountMemCache.xml`);
            Qizmt_Log(output);

                
          }
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>


".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion


            #region Qizmt-AddUsingReferences
            alljobfiles.Add(@"Qizmt-AddUsingReferences.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`ExCustomDLL_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del custom.dll`);                // deleting dll from CAC
                Shell(@`Qizmt del ExCustomDLL_Input.txt`);     // deleting input file from DFS in order to regenerate new one
                Shell(@`Qizmt del ExCustomDLL_Output.txt`);    // deleting old output file from DFS in order to produce new one
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
        {            /*
            This example puts a simple `Any CPU` DLL into CAC (Cluster Assembly Cache in MR.DFS).
            CAC has the same  idea as The Global Assembly Cache or GAC which is a machine-wide .NET assemblies cache for Microsoft's CLR platform,
            whith the difference that is made for DFS(Distributed File System) platform.
            For example , two jobs can reference the same dll that is uploaded to the MR.DFS, at the same time.
            Select `Any CPU` when building a DLL for CAC.
            DLL`s put into the CAC are replicated on every machine of the cluster before the put command returns.
            Once DLL is in CAC, it may be referenced from mapreducer jobs, local jobs and remote jobs.
            CAC DLL`s may be written in any .net language, e.g. C#, VB.NET, C++ CLI, etc.
            */
            string localdir = @`\\` + System.Net.Dns.GetHostName() + @`\c$\temp\Qizmt`;
            if(!System.IO.Directory.Exists(localdir))
            {
                System.IO.Directory.CreateDirectory(localdir);
            }
            
            string fn = `custom.dll`;
            string localfn = localdir + @`\` + Guid.NewGuid().ToString() + fn;
            
  /*
            // Code for custom.dll:
namespace custom
{
    public class Custom
    {
        public static int x;
        public static string ReverseString(string x)
        {
            int len = x.Length;
            char[] arr = new char[len];

            for (int i = 0; i < len; i++)
            {
                arr[i] = x[len - 1 - i];
            }

            return new string(arr);

        }
        public static string StringFoo()
        {
            return `Testing custom DLL`;
        }
    }
}

            */
            
            /* In the next step,  string testdlldataAnyCPU is assigned binary data converted to base64.
            
                How this is done?
                
                -We can hardcode string base64 in to the job's xml file, then we can convert string to bynary using  System.Convert.FromBase64String()
                which returns byte array. Byte array can be writen to the file using  System.IO.File.WriteAllBytes(localfn, testdlldata). File can be put in the CAC usig `put` command and
                net path.
                
                 What is the purpose?
                 
                 -This is made in order to recreate custom.dll in 
                each subsequent run and make example self-contained. */
            string testdlldataAnyCPU = `TVqQAAMAAAAEAAAA//8AALgAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAA4fug4AtAnNIbgBTM0hVGhpcyBwcm9ncmFtIGNhbm5vdCBiZSBydW4gaW4gRE9TIG1vZGUuDQ0KJAAAAAAAAABQRQAATAEDADD+H0wAAAAAAAAAAOAAAiELAQgAAAgAAAAGAAAAAAAAbicAAAAgAAAAQAAAAABAAAAgAAAAAgAABAAAAAAAAAAEAAAAAAAAAACAAAAAAgAAAAAAAAMAQIUAABAAABAAAAAAEAAAEAAAAAAAABAAAAAAAAAAAAAAABwnAABPAAAAAEAAAPgCAAAAAAAAAAAAAAAAAAAAAAAAAGAAAAwAAACEJgAAHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAACAAAAAAAAAAAAAAACCAAAEgAAAAAAAAAAAAAAC50ZXh0AAAAdAcAAAAgAAAACAAAAAIAAAAAAAAAAAAAAAAAACAAAGAucnNyYwAAAPgCAAAAQAAAAAQAAAAKAAAAAAAAAAAAAAAAAABAAABALnJlbG9jAAAMAAAAAGAAAAACAAAADgAAAAAAAAAAAAAAAAAAQAAAQgAAAAAAAAAAAAAAAAAAAABQJwAAAAAAAEgAAAACAAUAnCAAAOgFAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABMwBQAvAAAAAQAAEQJvEAAACgoGjRMAAAELFgwrEgcIAgYXWQhZbxEAAAqdCBdYDAgGMuoHcxIAAAoqGnIBAABwKh4CKBMAAAoqAABCU0pCAQABAAAAAAAMAAAAdjIuMC41MDcyNwAAAAAFAGwAAAAAAgAAI34AAGwCAABsAgAAI1N0cmluZ3MAAAAA2AQAACgAAAAjVVMAAAUAABAAAAAjR1VJRAAAABAFAADYAAAAI0Jsb2IAAAAAAAAAAgAAAVcVAgAJAAAAAPoBMwAWAAABAAAAEwAAAAIAAAABAAAAAwAAAAEAAAATAAAADQAAAAEAAAABAAAAAQAAAAAACgABAAAAAAAGADMALAAGAGwAWgAGAIMAWgAGAKAAWgAGAL8AWgAGANgAWgAGAPEAWgAGAAwBWgAGACcBWgAGAF8BQAEGAHMBQAEGAIEBWgAGAJoBWgAGAMoBtwE7AN4BAAAGAA0C7QEGAC0C7QEGAEsCLAAGAF0CLAAAAAAAAQAAAAAAAQABAAEAEAAVABwABQABAAEAFgA6AAoAUCAAAAAAlgA8AA0AAQCLIAAAAACWAEoAEgACAJIgAAAAAIYYVAAWAAIAAAABADoAEQBUABoAGQBUABoAIQBUABoAKQBUABoAMQBUABoAOQBUABoAQQBUABoASQBUABoAUQBUAB8AWQBUABoAYQBUABoAaQBUABoAcQBUACQAgQBUACoAiQBUABYAkQBSAi8AkQBiAjMAkQBUADgACQBUABYALgALAEUALgATAFEALgAbAFEALgAjAFEALgArAEUALgAzAFcALgA7AFEALgBLAFEALgBTAG8ALgBjAJkALgBrAKYALgBzAK8ALgB7ALgAPgAEgAAAAQAAAAAAAAAAAAAAAAAcAAAAAgAAAAAAAAAAAAAAAQAjAAAAAAAAAAA8TW9kdWxlPgBjdXN0b20uZGxsAEN1c3RvbQBjdXN0b20AbXNjb3JsaWIAU3lzdGVtAE9iamVjdAB4AFJldmVyc2VTdHJpbmcAU3RyaW5nRm9vAC5jdG9yAFN5c3RlbS5SZWZsZWN0aW9uAEFzc2VtYmx5VGl0bGVBdHRyaWJ1dGUAQXNzZW1ibHlEZXNjcmlwdGlvbkF0dHJpYnV0ZQBBc3NlbWJseUNvbmZpZ3VyYXRpb25BdHRyaWJ1dGUAQXNzZW1ibHlDb21wYW55QXR0cmlidXRlAEFzc2VtYmx5UHJvZHVjdEF0dHJpYnV0ZQBBc3NlbWJseUNvcHlyaWdodEF0dHJpYnV0ZQBBc3NlbWJseVRyYWRlbWFya0F0dHJpYnV0ZQBBc3NlbWJseUN1bHR1cmVBdHRyaWJ1dGUAU3lzdGVtLlJ1bnRpbWUuSW50ZXJvcFNlcnZpY2VzAENvbVZpc2libGVBdHRyaWJ1dGUAR3VpZEF0dHJpYnV0ZQBBc3NlbWJseVZlcnNpb25BdHRyaWJ1dGUAQXNzZW1ibHlGaWxlVmVyc2lvbkF0dHJpYnV0ZQBTeXN0ZW0uRGlhZ25vc3RpY3MARGVidWdnYWJsZUF0dHJpYnV0ZQBEZWJ1Z2dpbmdNb2RlcwBTeXN0ZW0uUnVudGltZS5Db21waWxlclNlcnZpY2VzAENvbXBpbGF0aW9uUmVsYXhhdGlvbnNBdHRyaWJ1dGUAUnVudGltZUNvbXBhdGliaWxpdHlBdHRyaWJ1dGUAU3RyaW5nAGdldF9MZW5ndGgAQ2hhcgBnZXRfQ2hhcnMAACVUAGUAcwB0AGkAbgBnACAAYwB1AHMAdABvAG0AIABEAEwATAAAACRGPWtDtNtNmxIqQyaX+MEACLd6XFYZNOCJAgYIBAABDg4DAAAOAyAAAQQgAQEOBCABAQIFIAEBET0EIAEBCAMgAAgEIAEDCAUgAQEdAwYHAwgdAwgLAQAGY3VzdG9tAAAFAQAAAAAXAQASQ29weXJpZ2h0IMKpICAyMDEwAAApAQAkMDA4MjI5YjYtNjJlZC00NWI2LWJkOTAtOWY5MTAxNWU3YTE1AAAMAQAHMS4wLjAuMAAACAEAAgAAAAAACAEACAAAAAAAHgEAAQBUAhZXcmFwTm9uRXhjZXB0aW9uVGhyb3dzAQAAAAAAMP4fTAAAAAACAAAAeQAAAKAmAACgCAAAUlNEUyzGo95U+ElPm8JUo30clfQEAAAAQzpcVXNlcnNcdm1hcnRzaW5jaHlrXERvY3VtZW50c1xWaXN1YWwgU3R1ZGlvIDIwMDhcUHJvamVjdHNcY3VzdG9tXGN1c3RvbVxvYmpcUmVsZWFzZVxjdXN0b20ucGRiAAAAAEQnAAAAAAAAAAAAAF4nAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAABQJwAAAAAAAAAAAAAAAF9Db3JEbGxNYWluAG1zY29yZWUuZGxsAAAAAAD/JQAgQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAQAAAAGAAAgAAAAAAAAAAAAAAAAAAAAQABAAAAMAAAgAAAAAAAAAAAAAAAAAAAAQAAAAAASAAAAFhAAACgAgAAAAAAAAAAAACgAjQAAABWAFMAXwBWAEUAUgBTAEkATwBOAF8ASQBOAEYATwAAAAAAvQTv/gAAAQAAAAEAAAAAAAAAAQAAAAAAPwAAAAAAAAAEAAAAAgAAAAAAAAAAAAAAAAAAAEQAAAABAFYAYQByAEYAaQBsAGUASQBuAGYAbwAAAAAAJAAEAAAAVAByAGEAbgBzAGwAYQB0AGkAbwBuAAAAAAAAALAEAAIAAAEAUwB0AHIAaQBuAGcARgBpAGwAZQBJAG4AZgBvAAAA3AEAAAEAMAAwADAAMAAwADQAYgAwAAAAOAAHAAEARgBpAGwAZQBEAGUAcwBjAHIAaQBwAHQAaQBvAG4AAAAAAGMAdQBzAHQAbwBtAAAAAAAwAAgAAQBGAGkAbABlAFYAZQByAHMAaQBvAG4AAAAAADEALgAwAC4AMAAuADAAAAA4AAsAAQBJAG4AdABlAHIAbgBhAGwATgBhAG0AZQAAAGMAdQBzAHQAbwBtAC4AZABsAGwAAAAAAEgAEgABAEwAZQBnAGEAbABDAG8AcAB5AHIAaQBnAGgAdAAAAEMAbwBwAHkAcgBpAGcAaAB0ACAAqQAgACAAMgAwADEAMAAAAEAACwABAE8AcgBpAGcAaQBuAGEAbABGAGkAbABlAG4AYQBtAGUAAABjAHUAcwB0AG8AbQAuAGQAbABsAAAAAAAwAAcAAQBQAHIAbwBkAHUAYwB0AE4AYQBtAGUAAAAAAGMAdQBzAHQAbwBtAAAAAAA0AAgAAQBQAHIAbwBkAHUAYwB0AFYAZQByAHMAaQBvAG4AAAAxAC4AMAAuADAALgAwAAAAOAAIAAEAQQBzAHMAZQBtAGIAbAB5ACAAVgBlAHIAcwBpAG8AbgAAADEALgAwAC4AMAAuADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAADAAAAHA3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==`;
    /*  CODE THAT CONVERTS  binary data from custom.dll in to base64 string and writes it to the file
            
            
            System.IO.FileStream inFile;
            byte[] binaryData;


            inFile = new System.IO.FileStream(inputFile(some custom dll), System.IO.FileMode.Open, System.IO.FileAccess.Read);
            binaryData = new Byte[inFile.Length];
            long bytesRead = inFile.Read(binaryData, 0, (int)inFile.Length);
            inFile.Close();
            string base64String = System.Convert.ToBase64String(binaryData, 0, binaryData.Length);

          System.IO.StreamWriter outFile;
			try
			{
				outFile = new System.IO.StreamWriter(outputFile(text file), false, System.Text.Encoding.ASCII);
				outFile.Write(base64String);
				outFile.Close();
			}
			catch (System.Exception exp)
			{
				System.Console.WriteLine(`{0}`, exp.Message);
			}
            
            
            */
            byte[] testdlldata = System.Convert.FromBase64String(testdlldataAnyCPU);
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
    <Job Name=`ExCustomDLL_Preprocessing` Custodian=`` Email=``>
    <Add Reference='custom.dll' Type='dfs' />
    <Add Reference=`Microsoft.VisualBasic.dll` Type=`system` />
      <Using>custom</Using>
      <Using>Microsoft.VisualBasic</Using>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                  if(32 != Strings.Asc(' '))
            {
                throw new Exception(`Local: (32 != Microsoft.VisualBasic.Strings.Asc(' '))`);
            }
            

                Qizmt_Log(Custom.StringFoo());
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`ExCustomDLL_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
   <Add Reference='custom.dll' Type='dfs' />
    <Add Reference=`Microsoft.VisualBasic.dll` Type=`system` />
      <Using>custom</Using>
      <Using>Microsoft.VisualBasic</Using>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://ExCustomDLL_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {

              if(32 != Strings.Asc(' '))
                {
                   throw new Exception(`Local: (32 != Microsoft.VisualBasic.Strings.Asc(' '))`);
                }
            
                //Create sample data.
                dfsoutput.WriteLine(Custom.StringFoo());
                dfsoutput.WriteLine(`This is PolindromFinder example with use of the custom made DLL`);
                 dfsoutput.WriteLine(`Polindromes are words that can be read the same way in either direction`);
                 dfsoutput.WriteLine(`Such as aibohphobia alula cammac kayak level radar refer`);
                dfsoutput.WriteLine(`Use of custom made DLLs together with powerful API of Qizmt makes life easier`);
                dfsoutput.WriteLine(`What other features we can implement in the future?`);
                dfsoutput.WriteLine(`What else is needed to improve performance?`);
                
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`ExCustomDLL` Custodian=`` Email=``>
    <Add Reference='custom.dll' Type='dfs' />
    <Add Reference=`Microsoft.VisualBasic.dll` Type=`system` />
      <Using>custom</Using>
      <Using>Microsoft.VisualBasic</Using>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>40</KeyLength>
        <DFSInput>dfs://ExCustomDLL_Input.txt</DFSInput>
        <DFSOutput>dfs://ExCustomDLL_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(' ');
               
               for(int i=0; i<parts.Length;i++)
               {
                   string word ;
                   word = parts[i].ToString();
                   string rword ;
                    rword = Custom.ReverseString(word);
                    
                    if ( rword == word )
                    {
                        recordset count = recordset.Prepare();
                        count.PutInt(1);
                        output.Add(parts[i],count);
                    }
                   
                   
               }
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {

                if(32 != Strings.Asc(' '))
                  {
                     throw new Exception(`Local: (32 != Microsoft.VisualBasic.Strings.Asc(' '))`);
                  }
            
                mstring Polindrome = mstring.Prepare(UnpadKey(key));
                int count = 0 ;
                for(int i = 0; i< values.Length; i++)
                {
                    
                    count++;
                    
                    
                }
                
                Polindrome.AppendM(` - `);
                Polindrome.AppendM(count);
                
                output.Add(Polindrome);
                
               
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    <Job Name=`ExCustomDLL_DisplayInputData` Custodian=`` Email=`` Description=`Display input data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://ExCustomDLL_Input.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display input.
                Qizmt_Log(`Input:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`ExCustomDLL_DisplayOutputData` Custodian=`` Email=`` Description=`Display output data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://ExCustomDLL_Output.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display output.
                Qizmt_Log(`Output:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>

".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region Qizmt-WordCountMemCache.xml
            alljobfiles.Add(@"Qizmt-WordCountMemCache.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`Qizmt-WordCountMemCache_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
        <!--<LocalHost>localhost</LocalHost>-->
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del Qizmt-WordCountMemCache1_Input.txt`);
                Shell(@`Qizmt del Qizmt-WordCountMemCache2_Input.txt`);
                Shell(@`Qizmt del Qizmt-WordCountMemCache3_Input.txt`);
                Shell(@`Qizmt del Qizmt-WordCountMemCache_Output.txt`);
              
               
           
            }
        ]]>
      </Local>
    </Job>
   
     <Job Name=`Qizmt-WordCountMemCache_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WordCountMemCache1_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.
                dfsoutput.WriteLine(`In computer science a cache pronounced kækash is a component that improves performance by transparently storing data such that future requests for that data can be served faster`);
                dfsoutput.WriteLine(`The data that is stored within a cache might be values that have been computed earlier or duplicates of original values that are stored elsewhere`);
                dfsoutput.WriteLine(`regression test`);
              
            }
        ]]>
      </Remote>
    </Job>
     <Job Name=`Qizmt-WordCountMemCache_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WordCountMemCache2_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.
                dfsoutput.WriteLine(`As opposed to a buffer which is managed explicitly by a client a cache stores data transparently`);
                dfsoutput.WriteLine(` Nevertheless caches have proven extremely effective in many areas of computing because `);
                              
            }
        ]]>
      </Remote>
    </Job>
     <Job Name=`Qizmt-WordCountMemCache_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>dfs://Qizmt-WordCountMemCache3_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data.
                dfsoutput.WriteLine(`Nevertheless caches have proven extremely effective in many areas of computing because`);
                dfsoutput.WriteLine(` if data is requested that is physically stored close to data that has been requested already`);
              
            }
    
      ]]>
      </Remote>
    </Job>
    
    <Job Name=`Qizmt-WordCountMemCache` Custodian=`` Email=``>
     <MemCache />
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>16</KeyLength>
        <DFSInput>dfs://Qizmt-WordCountMemCache1_Input.txt</DFSInput>
        <DFSOutput>dfs://Qizmt-WordCountMemCache_Output.txt</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
                
                 mstring sLine= mstring.Prepare(line);
                 mstringarray parts = sLine.SplitM(' ');
              
                 for(int i=0; i < parts.Length; i++)
                     {
                        mstring word = parts[i];
                    
                        if(word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
                          {                        
                            output.Add(word.ToLowerM(), mstring.Prepare(1)); 
                           }   
                     }
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          struct SKeyVal
            {
             public   string ikey;
             public   int ival;
            }
          
            List<SKeyVal> Inputs = new List<SKeyVal>(); 
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
               
                            
                    mstring rs = mstring.Prepare(key);
                    SKeyVal s = new SKeyVal();
                    s.ikey = rs.ToString();
                    
                    for (int i=0; i < values.Length; i ++)
                    {
                        s.ival++;
                        
                    }
                   
                      
                   
                    Inputs.Add(s);
                                
                
                if(StaticGlobals.Qizmt_Last)
                {
                    // On last reduce iteration, so sort the input and insert into the MemCache.
                    
                                 Inputs.Sort(MyCompareKeys1);  
                    
                    string MemCacheName = `Qizmt-WordCountMemCache`;
                    int keyoffset = 0, keylen = 16;
                    int rowlen = 36;
                    
                    // Attach to this worker's view of the MemCache in a using-statement.
                    using(MemCache mc = MemCache.Attach(MemCacheName, Qizmt_ProcessID))
                    {
                        int CurrentMergeKeyIndex = 0;
                        for(mc.MoveFirst(); !mc.EOF; mc.MoveNext()) // Iterate through existing values in MemCache.
                        {
                            MemCache.Tuple t;
                            t = mc.PeekRow(); // Peek at the current row, but don't read past it yet.
                            string k0 = t[0].GetString(); // First part of existing row.
                                                       
                            
                                
                            bool updated = false;
                            while(CurrentMergeKeyIndex < Inputs.Count)
                            {
                                // See if there are new keys to insert or keys to change.
                                int kdiff = MyCompareKeys((Inputs[CurrentMergeKeyIndex].ikey).ToString(),
                                    k0.ToString() );
                                if(kdiff < 0) // Merge key is less than current one, so insert.
                                {
                                    if(updated)
                                    {
                                        throw new Exception(`Keys out of order (2662)`);
                                    }
                                    // Insert here.
                                    t = mc.InsertRow();
                                    t[0].SetString(Inputs[CurrentMergeKeyIndex].ikey); // First part of input.
                                    t[1].SetInt(Inputs[CurrentMergeKeyIndex].ival);//Second part of input.
                                    mstring sLine = mstring.Prepare(Inputs[CurrentMergeKeyIndex].ikey);
                                    sLine.AppendM(`->`);
                                    sLine.AppendM(Inputs[CurrentMergeKeyIndex].ival);
                                    output.Add(sLine);
                                    
                                    
                                    CurrentMergeKeyIndex++;
                                    continue;
                                }
                                else if(kdiff == 0) // Merge key is the same as the current one, so update.
                                {
                                    // They're the same, so update it.
                                    t = mc.ReadRow(); // Read it so it's included, and then update it.
                                    
                                    t[0].SetString(Inputs[CurrentMergeKeyIndex].ikey);
                                    
                                    int k1 = t[1].GetInt();
                                    
                                    t[1].SetInt(Inputs[CurrentMergeKeyIndex].ival+k1);
                                    
                                    // Creating output:
                                    mstring sLine = mstring.Prepare(Inputs[CurrentMergeKeyIndex].ikey);
                                    sLine.AppendM(`->`);
                                    sLine.AppendM(Inputs[CurrentMergeKeyIndex].ival+k1);
                                    output.Add(sLine);
                                    CurrentMergeKeyIndex++;
                                    updated = true;
                                    continue; // Let it keep updating the same one if same keys.
                                }
                                break;
                            }
                            if(updated)
                            {
                                continue;
                            }
                        }
                        // Add extra, higher keys after the last one.
                        while(CurrentMergeKeyIndex < Inputs.Count)
                        {
                            MemCache.Tuple t;
                            t = mc.InsertRow();
                            t[0].SetString(Inputs[CurrentMergeKeyIndex].ikey);
                            t[1].SetInt(Inputs[CurrentMergeKeyIndex].ival);
                            // Creating output:
                            mstring sLine = mstring.Prepare(Inputs[CurrentMergeKeyIndex].ikey);
                                    sLine.AppendM(`->`);
                                    sLine.AppendM(Inputs[CurrentMergeKeyIndex].ival);
                                    output.Add(sLine);
                            
                            CurrentMergeKeyIndex++;
                        }
                    }
                    
                    if(0 == Qizmt_ProcessID)
                    {
                        Qizmt_Log(` Merged data into the MemCache `);
                    }
                    
                }
                
   
                
                
            }
            
            int MyCompareKeys1(SKeyVal x,SKeyVal y)
            {
                int r = (x.ikey).CompareTo(y.ikey);
                
                return r;
                
            }
            
              int MyCompareKeys(string x,string  y)
            {
                int r = x.CompareTo(y);
              
                return r;
            }
                       
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    
      <Job Name=`MemCache_Commit` Custodian=`` Email=`` Description=`Commit the MemCache changes`>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                // Commit the MemCache changes to DFS.
                // Alternatively, could execute commit every X hours from the Qizmt scheduler.
                {
                    DateTime lt = DateTime.Now;
                    Shell(@`Qizmt memcache commit name=Qizmt-TestMemCache`, true);
                    Qizmt_Log(`Committed MemCache; duration: ` + (DateTime.Now - lt));
                }
                
            }
        ]]>
      </Local>
    </Job>
    <Job Name=`Qizmt-WordCountMemCache_DisplayInputData` Custodian=`` Email=`` Description=`Display input data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://Qizmt-WordCountMemCache1_Input.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display input.
                Qizmt_Log(`Input:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
    <Job Name=`Qizmt-WordCountMemCache_DisplayOutputData` Custodian=`` Email=`` Description=`Display output data`>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>dfs://Qizmt-WordCountMemCache_Output.txt</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {    
                //Display output.
                Qizmt_Log(`Output:`);
                System.IO.StreamReader sr = new System.IO.StreamReader(dfsinput);
                Qizmt_Log(sr.ReadToEnd());
            }
        ]]>
      </Remote>
    </Job>
  </Jobs>
</SourceCode>

".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region Qizmt-Wrapper-WordCountMemCache.xml
            alljobfiles.Add(@"Qizmt-Wrapper-WordCountMemCache.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
  <Jobs>
    <Job Name=`ExecXPath_Preprocessing` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
        public virtual void Local()
        {
            if(Qizmt_ExecArgs.Length > 0)
            {
               
                
                if (Qizmt_ExecArgs[0] == `1`)
                {
  string output = Shell(@`Qizmt exec  ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSInput=Qizmt-WordCountMemCache1_Input.txt`` ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSOutput=Qizmt-WordCountMemCache_Output.txt``  Qizmt-WordCountMemCache.xml`);
                 Qizmt_Log(output);
                }
            
             if (Qizmt_ExecArgs[0] == `2`)
                {
   string output = Shell(@`Qizmt exec  ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSInput=Qizmt-WordCountMemCache2_Input.txt``   ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSOutput=Qizmt-WordCountMemCache_Output.txt``  Qizmt-WordCountMemCache.xml`);
                Qizmt_Log(output);
                }
             if (Qizmt_ExecArgs[0] == `3`)
             {
      string output = Shell(@`Qizmt exec  ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSInput=Qizmt-WordCountMemCache3_Input.txt`` ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSOutput=Qizmt-WordCountMemCache_Output.txt``  Qizmt-WordCountMemCache.xml`);
             Qizmt_Log(output);
             }
            
            }
            else
            {
              
                 string schema =
                `string(16),int`; // Default.
            const string DefaultMemCacheSize = `10KB`;
            
                  // Delete old data if any:
            //  Shell(@`Qizmt del dfs://Qizmt-WordCountMemCache*`);
              Shell(@`Qizmt memcache delete name=Qizmt-WordCountMemCache`, true);
                
                // Create the empty MemCache:
              Shell(@`Qizmt memcache create name=Qizmt-WordCountMemCache schema=` + schema);
              Qizmt_Log(` Created MemCache named 'Qizmt-WordCountMemCache' `);
              
       string output = Shell(@`Qizmt exec  ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSInput=Qizmt-WordCountMemCache1_Input.txt`` ``//Job[@Name='Qizmt-WordCountMemCache']/IOSettings/DFSOutput=Qizmt-WordCountMemCache_Output.txt``  Qizmt-WordCountMemCache.xml`);
            Qizmt_Log(output);

                
          }
        }
        ]]>
      </Local>
    </Job>
  </Jobs>
</SourceCode>


".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion

            #region JaggedBinary
            alljobfiles.Add(@"Qizmt-JaggedBinary.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
                @"<SourceCode>
  <Jobs>
    <Job Name=`Cleanup` Custodian=`` Email=``>
      <IOSettings>
        <JobType>local</JobType>
      </IOSettings>
      <Local>
        <![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del JaggedBinary_Input.txt`);
                Shell(@`Qizmt del JaggedBinary_Input.bin`);
                Shell(@`Qizmt del JaggedBinary_Output.txt`);
                Shell(@`Qizmt del JaggedBinary_Output.bin`);
            }
        ]]>
      </Local>
    </Job>
    
    <Job Name=`CreateSampleData` Custodian=`` Email=`` Description=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader></DFSReader>
          <DFSWriter>JaggedBinary_Input.txt</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                //Create sample data
                //Book title; Edition years; Authors
                dfsoutput.WriteLine(`Sunshine State;1999,2001,2003;John Smith,Joe Nathan,Mary Smith`);
                dfsoutput.WriteLine(`Dinner in 30 Minutes;2008,2009;Ray Simon`);
                dfsoutput.WriteLine(`Travel by Train;1999;Jane Zoe,Cathy Zoe`);
                dfsoutput.WriteLine(`Curtain Calls;1970,2000,2003,2008;Peter Uay`);
            }
        ]]>
      </Remote>
    </Job>
    
    <Job Name=`ConvertTextToJaggedBinary` Custodian=`` Email=`` Description=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>JaggedBinary_Input.txt</DFSReader>
          <DFSWriter>JaggedBinary_Input.bin@?</DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                StringBuilder sb = new StringBuilder();
                while(dfsinput.ReadLineAppend(sb))
                {
                    string line = sb.ToString();
                    string[] parts = line.Split(';');
                    
                    string title = parts[0];
                    string[] years = parts[1].Split(',');
                    string[] authors = parts[2].Split(',');                    
                    
                    recordset rs = recordset.Prepare();
                    rs.PutString(title);
                    
                    rs.PutInt(years.Length);
                    for(int i = 0; i < years.Length; i++)
                    {
                        rs.PutInt(Int32.Parse(years[i]));
                    }
                    
                    rs.PutInt(authors.Length);
                    for(int i = 0; i < authors.Length; i++)
                    {
                        rs.PutString(authors[i]);
                    }                    
                    
                    dfsoutput.WriteRecord(rs.ToByteSlice().ToBytes()); //output jagged row
                    sb.Length = 0;
                }
            }
        ]]>
      </Remote>
    </Job>
    
    <Job Name=`mr` Custodian=`` Email=``>
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>int</KeyLength>
        <DFSInput>JaggedBinary_Input.bin@?</DFSInput>
        <DFSOutput>dfs://JaggedBinary_Output.bin@?</DFSOutput>
        <OutputMethod>grouped</OutputMethod>
      </IOSettings>
      <MapReduce>
        <Map>
          <![CDATA[
            public virtual void Map(ByteSlice line, MapOutput output)
            {
               recordset rline = recordset.Prepare(line);   //read a jagged row
               
               mstring title = rline.GetString();
               
               int firstyear = 0;
               {
                   int yearcount = rline.GetInt();
                   for(int i = 0; i < yearcount; i++)
                   {
                       int year = rline.GetInt();
                       if(i == 0)
                       {
                           firstyear = year;
                       }
                   }
               }              
               
               mstring firstauthor = mstring.Prepare();
               {
                   int authorcount = rline.GetInt();            
                   for(int i = 0; i < authorcount; i++)
                   {
                       mstring author = rline.GetString();
                       if(i == 0)
                       {
                           firstauthor = author;
                       }
                   }               
               }
               
               recordset rkey = recordset.Prepare();
               rkey.PutInt(firstyear);
               
               recordset rvalue = recordset.Prepare();
               rvalue.PutString(title);
               rvalue.PutString(firstauthor);
               
               output.Add(rkey, rvalue);  
            }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                recordset rkey = recordset.Prepare(key);
                int firstyear = rkey.GetInt();
                
                for(int i = 0; i < values.Length; i++)
                {
                    recordset rvalue = recordset.Prepare(values[i].Value);
                    mstring title = rvalue.GetString();
                    mstring firstauthor = rvalue.GetString();
                    
                    recordset rout = recordset.Prepare();
                    rout.PutInt(firstyear);
                    rout.PutString(title);
                    rout.PutString(firstauthor);
                    output.Add(rout);   //output jagged row
                }
            }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
    
    <Job Name=`ReadJaggedBinary` Custodian=`` Email=`` Description=``>
      <IOSettings>
        <JobType>remote</JobType>
        <DFS_IO>
          <DFSReader>JaggedBinary_Output.bin@?</DFSReader>
          <DFSWriter></DFSWriter>
        </DFS_IO>
      </IOSettings>
      <Remote>
        <![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                List<byte> buf = new List<byte>();
                while(dfsinput.ReadRecordAppend(buf))
                {
                    recordset rs = recordset.Prepare(ByteSlice.Prepare(buf));
                    
                    int firstyear = rs.GetInt();
                    mstring title = rs.GetString();
                    mstring firstauthor = rs.GetString();
                    
                    DSpace_Log(title.ToString() + `:` + firstyear.ToString() + `:` + firstauthor.ToString());
                    
                    buf.Clear();
                }
            }
        ]]>
      </Remote>
    </Job>
    
  </Jobs>
</SourceCode>
".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion


            #region Test
            StringBuilder csjobs = new StringBuilder();
            for (int ij = 0; ij < alljobfiles.Count; ij++)
            {
                if (csjobs.Length != 0)
                {
                    csjobs.Append(", ");
                }
                csjobs.Append("\"" + alljobfiles[ij] + "\"");
            }
            AELight.DfsDelete(@"Qizmt-Test.xml", false);
            AELight.DfsPutJobsFileContent(@"Qizmt-Test.xml",
(@"<SourceCode>
  <Jobs>
    <Job Name=`Example Test` Custodian=`` email=``>
      <IOSettings>
        <JobType>test</JobType>
      </IOSettings>
      <Test>
        <![CDATA[
        public virtual void Test()
        {
            string[] jobfiles = new string[] { " + csjobs.ToString() + @" };
            foreach(string jobfile in jobfiles)
            {
                Qizmt_Log(`Qizmt exec ` + jobfile);
                Qizmt_Log(Shell(`Qizmt exec ` + jobfile));
                Qizmt_Log(`-`);
            }
        }
        ]]>
      </Test>
    </Job>
  </Jobs>
</SourceCode>
").Replace('`', '"'));
            Console.WriteLine("    Qizmt exec Qizmt-Test.xml");
            #endregion

            //Console.WriteLine("Examples written.");
        }
    }
}
