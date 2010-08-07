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
    public class FastRegressionTest
    {
        public static void GenFastRegressionTests()
        {
            try
            {
                System.IO.Directory.CreateDirectory(@"C:\temp");
            }
            catch
            {
            }

            List<string> alljobfiles = new List<string>();

            #region OptimalDriver_1
            alljobfiles.Add(@"Qizmt-OptimalDriver_1.xml");
            AELight.DfsDelete(alljobfiles[alljobfiles.Count - 1], false);
            AELight.DfsPutJobsFileContent(alljobfiles[alljobfiles.Count - 1],
@"<SourceCode>
	<Jobs>
		<Job Name=`regression_test_OptimalDriver1_Preprocessing` Custodian=`` Email=``>
			<IOSettings>
				<JobType>local</JobType>
				<!--<LocalHost>localhost</LocalHost>-->
			</IOSettings>
			<Local>
				<![CDATA[
            public virtual void Local()
            {
                Shell(@`Qizmt del regression_test_OptimalDriver1_Input.txt`,true);
                Shell(@`Qizmt del regression_test_OptimalDriver1_Output.txt`,true);
         //     Shell(@`Qizmt del log.txt`,true);
            }
        ]]>
			</Local>
		</Job>

		<Job Name=`regression_test_OptimalDriver1_CreateSampleData` Custodian=`` Email=`` Description=`Create sample data`>
			<IOSettings>
				<JobType>remote</JobType>
				<DFS_IO>
					<DFSReader></DFSReader>
					<DFSWriter>dfs://regression_test_OptimalDriver1_Input.txt</DFSWriter>
				</DFS_IO>
			</IOSettings>
			<Remote>
				<![CDATA[
            public virtual void Remote(RemoteInputStream dfsinput, RemoteOutputStream dfsoutput)
            {
                 //Create sample data.  in form of   double,unicode_string(100),string(100),UShort_UInt16,Short_Int16,Long_Int64,ULong_UInt64,Int_Int32,UInt_UInt32,DateTime
                int rowCount = 2000;
                int UnicodeStringLength = 100;
                int stringLength = 100;
                char del = ',';
                byte[] buf;
                byte[] buf1;
                byte[] buf2;
                byte[] buf3;
                byte[] buf4;
                byte[] buf5;
                byte[] buf6; 
                byte[] buf7;
                string s,s1;
                long l64;
                
               Random rnd = new Random(System.DateTime.Now.Millisecond / 2 + System.Diagnostics.Process.GetCurrentProcess().Id / 2);
               
               DateTime dt = DateTime.Now;
                
                List<byte> onerow = new List<byte>();
                
            
              
               for(long rn = 0; rn < rowCount; rn++)        
                  {
                    
                      onerow.Clear();
                    
                    
                    //double  
                    double d = rnd.NextDouble();
                    
                    if(rnd.Next() % 2 == 1)
                    {
                        d = d * -1;
                    }   
                    
                    s = d.ToString();
                    buf = System.Text.Encoding.UTF8.GetBytes(s);
                    
                     foreach(byte b in buf)
                    {
                        onerow.Add(b);
                    }
                    
                     onerow.Add((byte)del);
                     
                       //unicode_string
                    for (int cnt = 0; cnt < UnicodeStringLength; cnt++)
                    {
                        int codeValue = rnd.Next(0x20, 0xFFEF);
                        byte b0 = 0;
                        byte b1 = 0;
                        byte b2 = 0;
                        byte b3 = 0;

                        ConvertCodeValueToBytesUTF8(codeValue, ref b0, ref b1, ref b2, ref b3);
                        
                        if (b0 != 0)
                        {
                            
                           if( b0 !=(byte) ',' && b3 ==0 && b1 == 0 && b2 ==0)
                            onerow.Add(b0);
                            else
                            onerow.Add((byte)';');
                                                              
                        }

                        if (b1 != 0)
                        {
                            onerow.Add(b1);
                        }

                        if (b2 != 0)
                        {
                            onerow.Add(b2);
                        }

                        if (b3 != 0)
                        { 
                       
                            onerow.Add(b3);
                       
                        }
                    }    
                    
                     onerow.Add((byte)del);
                     
                       //string  
                    for(int cnt = 0; cnt < stringLength; cnt++)
                    {
                        byte b = (byte)rnd.Next((int)' ' + 1, (int)'~' + 1);   
                        
                          if ( b != (byte)del)
                                onerow.Add(b);
                          else
                               onerow.Add((byte)';');
                    }    
                     
                       onerow.Add((byte)del);
                     
                    
                     //UInt16/UShort 
                    ushort x = (ushort)rnd.Next(0, ushort.MaxValue-1);
                                       
                    s = x.ToString();
                    buf1 = System.Text.Encoding.UTF8.GetBytes(s);
                    
                    foreach(byte b in buf1)
                    {
                        onerow.Add(b);
                    }
                    
                    onerow.Add((byte)del);
                    
                     //int16/short  
                   short x1 = (short)rnd.Next(short.MinValue+1, Int16.MaxValue-1);
                    
                    s = x1.ToString();
                    buf2 = System.Text.Encoding.UTF8.GetBytes(s);
                    
                    foreach(byte b in buf2)
                    {
                        onerow.Add(b);
                    }
                    
                       onerow.Add((byte)del);
                    
                    //long/int64 
                    long  l = DateTime.Now.Ticks + (long)rnd.Next();
                    
                    if(rnd.Next() % 2 == 1)
                    {
                        l = l * (-1);
                    }
                    
                    s = l.ToString();
                    buf3 = System.Text.Encoding.UTF8.GetBytes(s);
                    
                    foreach(byte b in buf3)
                    {
                        onerow.Add(b);
                    }
                    
                    
                      onerow.Add((byte)del);
                    
                    //ulong/uint64  
                   ulong l1 = (ulong)(DateTime.Now.Ticks + (long)rnd.Next());
                    
                    s = l1.ToString();
                    buf4 = System.Text.Encoding.UTF8.GetBytes(s);
                    
                    foreach(byte b in buf4)
                    {
                        onerow.Add(b);
                    }
                    
                      onerow.Add((byte)del);
                      
                      
                       //int/int32 
                   Int32  x2 = rnd.Next();
                    
                    if(rnd.Next() % 2 == 1)
                    {
                        x2 = x2 * -1;
                    }
                    
                    s = x2.ToString();
                    buf5 = System.Text.Encoding.UTF8.GetBytes(s);
                    
                    foreach(byte b in buf5)
                    {
                        onerow.Add(b);
                    }
                    
                     onerow.Add((byte)del);
                    
                    //UInt32  
                   UInt32 x3 = (UInt32)rnd.Next();
                    
                    s = x3.ToString();
                    buf6 = System.Text.Encoding.UTF8.GetBytes(s);
                    
                    foreach(byte b in buf6)
                    {
                        onerow.Add(b);
                    }
                
                                      
              
                        onerow.Add((byte)del);
                     
                    //DateTime
                    DateTime newdt = dt.AddSeconds((double)rnd.Next());      
                    
                    l64 = newdt.ToBinary();
                    
                    s1 = l64.ToString();
                    buf7 = System.Text.Encoding.UTF8.GetBytes(s1);
         
                   foreach(byte b in buf7)
                    {
                         onerow.Add(b);
                    
                     }
                
                      
                    
                    dfsoutput.WriteLine(onerow);       
                  }
                              
                    
                             
            }
             public static void ConvertCodeValueToBytesUTF8(int codeValue, ref byte b0, ref byte b1, ref byte b2, ref byte b3)
        {
            b0 = 0;
            b1 = 0;
            b2 = 0;
            b3 = 0;

            if (0xD800 <= codeValue && codeValue <= 0xDFFF)
            {
                //These are surrogates, reserved for UTF16.
                return;
            }

            if (0 <= codeValue && codeValue <= 0x7F)
            {
                b0 = (byte)codeValue;               
            }
            else
            {
                if (0x80 <= codeValue && codeValue <= 0x7FF)
                {
                    byte t = (byte)(codeValue >> 6);
                    int x = t & (byte)0x1F;
                    x = x | (byte)0xC0;
                    b0 = (byte)x;

                    t = (byte)codeValue;
                    x = t & (byte)0x3F;
                    x = x | (byte)0x80;
                    b1 = (byte)x;
                }
                else
                {
                    if (0x800 <= codeValue && codeValue <= 0xFFFF)
                    {
                        byte t = (byte)(codeValue >> 12);
                        int x = t & (byte)0x0F;
                        x = x | (byte)0xE0;
                        b0 = (byte)x;

                        t = (byte)(codeValue >> 6);
                        x = t & (byte)0x3F;
                        x = x | (byte)0x80;
                        b1 = (byte)x;

                        t = (byte)codeValue;
                        x = t & (byte)0x3F;
                        x = x | (byte)0x80;
                        b2 = (byte)x;
                    }
                    else
                    {
                        if (0x10000 <= codeValue && codeValue <= 0x10FFFF)
                        {
                            byte t = (byte)(codeValue >> 18);
                            int x = t & (byte)0x07;
                            x = x | (byte)0xF0;
                            b0 = (byte)x;

                            t = (byte)(codeValue >> 12);
                            x = t & (byte)0x3F;
                            x = x | (byte)0x80;
                            b1 = (byte)x;

                            t = (byte)(codeValue >> 6);
                            x = t & (byte)0x3F;
                            x = x | (byte)0x80;
                            b2 = (byte)x;

                            t = (byte)codeValue;
                            x = t & (byte)0x3F;
                            x = x | (byte)0x80;
                            b3 = (byte)x;
                        }
                    }
                }
            }
            }
        ]]>
			</Remote>
		</Job>
		<Job Name=`regression_test_OptimalDriver1` Custodian=`` Email=``>
			<IOSettings>
				<Setting name=`Subprocess_TotalPrime` value=`1` />
				<JobType>mapreduce</JobType>
				<KeyLength>Int16,Short,UInt,UInt16,UInt32,UInt32,UInt64,ULong,UShort</KeyLength>
				<DFSInput>dfs://regression_test_OptimalDriver1_Input.txt</DFSInput>
				<DFSOutput>dfs://regression_test_OptimalDriver1_Output.txt</DFSOutput>

				<OutputMethod>grouped</OutputMethod>
			</IOSettings>
			<MapReduce>
				<Map>
					<![CDATA[
     
            public virtual void Map(ByteSlice line, MapOutput output)
            { 
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   recordset rKey = recordset.Prepare();
                   recordset Value = recordset.Prepare();
                   
                   Int16 i = Int16.Parse(parts[4].ToString());
                   short sh = short.Parse(parts[4].ToString());
                   uint ui  = uint.Parse(parts[8].ToString());
                   UInt16 ui16 = UInt16.Parse(parts[3].ToString());
                   UInt32 ui32 = UInt32.Parse(parts[8].ToString());
                   UInt64 ui64 = UInt64.Parse(parts[6].ToString());
                   ulong ul = ulong.Parse(parts[6].ToString());
                   ushort ush = ushort.Parse(parts[3].ToString());
                   
                   rKey.PutInt16(i);
                   rKey.PutShort(sh);
                   rKey.PutUInt(ui);
                   rKey.PutUInt16(ui16);
                   rKey.PutUInt32(ui32);
                   rKey.PutUInt64(ui64);
                   rKey.PutULong(ul);
                   rKey.PutUShort(ush);
                   
                   
                // testing all  Members of the mstring
                {
                    
                    mstring_test(line);
               
                }
                
                // testing all  Members of the recordset
                {
                    
                    recordset_test(line);
                    
                    
                }
                
                //testing all  Members of the ByteSlice
                {
                    
                    ByteSlice_test(line);
                }
                
                
                //testing all Members of Entry
                {
                    Entry_test(line);
                    
                }
         
                output.Add(rKey, sLine);
       }
            
            public static bool mstring_test(ByteSlice line)
               {  
                     bool result = true;
                   
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                  try
                  {
                      
                   if( (!mstring_AppendM_test(line)) 
                   || (!mstring_Consume_test(line)) 
                   ||(!mstring_Contains_test(line)) 
                   || (!mstring_Copy_test(line))
                   || (!mstring_CsvNexItemTo_test(line))
                   || (!mstring_EndsWith_test(line))
                   || (!mstring_HasNextItem_test(line)) 
                   || (!mstring_IndexOf_test(line))
                   || (!mstring_MAppend_test(line))
                   || (!mstring_MPadLeft_test(line))
                   || (!mstring_MPadRight_test(line))
                   || (!mstring_MReplace_test(line))
                   || (!mstring_MSplit_test(line)) 
                   || (!mstring_MSubstring_test(line)) 
                   || (!mstring_MToLower_mapper_test(line))
                   || (!mstring_MToUpper_mapper_test(line))
                   || (!mstring_MTrinM_mapper_test(line))
                   || (!mstring_NextItemTo_test(line))
                   || (!mstring_operatorEqual_test(line))
                   || (!mstring_operatorNotEqual_test(line))
                   || (!mstring_PadLeftM_test(line))
                   || (!mstring_PadRightM_test(line))
                   || (!mstring_Prepare_test(line))
                   || (!mstring_ReplaceM_test(line))
                   || (!mstring_ResetGetPosition_test(line))
                   || (!mstring_SplitM_test(line))
                   || (!mstring_StartsWith_test(line))
                   || (!mstring_SubstringM_test(line))
                   || (!mstring_ToDataType_test(line))
                   || (!mstring_ToLowerM_test(line))
                   || (!mstring_ToUpperM_test(line)))
                   {
                      
                      throw new Exception(`mstring regression test failed`);
                      
                   }
                   
                  }
                  catch(Exception e)
                  {
                      Log.Qizmt_Log(`Exception`+e.ToString());    
                      result = false;
                  }
                  
                 
                   return result;
                                      
               }
               
                 public static bool recordset_test(ByteSlice line)
               {  
                     bool result = true;
                   
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                  try
                  {
                      
                   if((!recordset_PutInt_test(line))        
                   ||(!recordset_PutInt16_test(line))    
                   ||(!recordset_PutInt32_test(line)) 
                   ||(!recordset_PutULong_test(line))
                   ||(!recordset_PutInt64_test(line))
                   ||(!recordset_PutLong_test(line))
                   ||(!recordset_PutShort_test(line))
                   ||(!recordset_PutString_test(line))
                   ||(!recordset_PutUInt_test(line))
                   ||(!recordset_PutUInt16_test(line))
                   ||(!recordset_PutUInt32_test(line))
                   ||(!recordset_PutUInt64_test(line))
                   ||(!recordset_PutULong_test(line))
                   ||(!recordset_PutUShort_test(line))
                   ||(!recordset_PutDouble_test(line))
                   ||(!recordset_PutDateTime_test(line))
                   ||(!recordset_PutChar_test(line))
                   ||(!recordset_PutBytes_test(line))
                   ||(!recordset_PutBool_test(line))
                   ||(!recordset_PrepareRow_test(line))
                   ||(!recordset_ContainsString_test(line)))
                   {
                      
                      throw new Exception(`recordset regression test failed`);
                      
                   }
                   
                  }
                  catch(Exception e)
                  {
                      Log.Qizmt_Log(`Exception`+e.ToString());    
                      result = false;
                  }
                  
                 
                   return result;
                                      
               }
               
                  public static bool ByteSlice_test(ByteSlice line)
               {  
                     bool result = true;
                   
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                  try
                  {
                      
                   if((!ByteSlice_AppendTo_test(line))
                    ||(!ByteSlice_CopyTo_test(line)) 
                    ||(!ByteSlice_CopyToInt_test(line))
                    ||(!ByteSlice_Create_test(line))
                    ||(!ByteSlice_Prepare_test(line)))
                   {
                      
                      throw new Exception(`ByteSlice regression test failed`);
                      
                   }
                   
                  }
                  catch(Exception e)
                  {
                      Log.Qizmt_Log(`Exception`+e.ToString());    
                      result = false;
                  }
                  
                 
                   return result;
                                      
               }
               
               
               public static bool  Entry_test(ByteSlice line)
               {  
                     bool result = true;
                   
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                  try
                  {
                      
                   if( (!Entry_AsciiToBytes_test(line)) 
                    ||(!Entry_BytesToAscii_test(line))
                    ||(!Entry_BytesToDataType_test(line))
                    ||(!Entry_ToBytes_test(line))
                    ||(!Entry_U_Reg_Reg_U_test(line)))
                   {
                      
                      throw new Exception(`ByteSlice regression test failed`);
                      
                   }
                   
                  }
                  catch(Exception e)
                  {
                      Log.Qizmt_Log(`Exception`+e.ToString());    
                      result = false;
                  }
                  
                 
                   return result;
                                      
               }
               
       
               // MSTRING TESTS
               
               public static bool mstring_AppendM_test(ByteSlice line)
               {
                  
                
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                        
                   
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                      
                   
                try
                   {
                     string s = parts[2].ToString();
                     
                     for(int i = 0; i < s.Length; i++)
                          {
                              
                              char c = s[i];
                              val.AppendM(c);
                              
                          }
            
                     if (parts[2] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type: char failed`);
           
                   }
                catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                  
                    // needs to be cleared before next use
                     val.Clear();
                     
                     
                 try
                   {
                     string s = parts[1].ToString();
                     
                     for(int i = 0; i < s.Length; i++)
                          {
                              
                              char c = s[i];
                              val.AppendM(c);
                              
                          }
                                        
                     if (parts[1] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type: char_unicode failed`);
              
                   }
                 catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                    // needs to be cleared before next use
                     val.Clear();
                   
         /*         
                 try
                  {
                     double  d =  double.Parse(parts[0].ToString());
                     val.AppendM(d);
                     
                       if (parts[0] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type: double failed`);
                           
                  }
                catch(Exception e) 
                  {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     
                   }
                   
                     // needs to be cleared before next use
                        val.Clear();
           */         
                 try
                   {
                      
                       string s = parts[1].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       val.AppendM(su);
                       val.AppendM(su1);
                                
                       if (parts[1] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type: string_unicode failed`);
                         
                   }
                catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                    }
                     
                    // needs to be cleared before next use
                    val.Clear();  
                   
                     
                     try
                     {
                        string s = parts[1].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       mstring msu = mstring.Prepare(su);
                       mstring msu1 = mstring.Prepare(su1);
                       val.AppendM(msu);
                       val.AppendM(msu1);
                 
                       if (parts[1] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type: mstring_unicode failed`);
                           
                     }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                      
                   }
                   
                   
                    // needs to be cleared before next use
                    val.Clear();
                  
                    try
                    {
                       string s = parts[2].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       val.AppendM(su);
                       val.AppendM(su1);
                       if (parts[2] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type: string failed`);
                           
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                       
                   }
                     
                    
                   // needs to be cleared before next use
                    val.Clear();
                    
                  
                   try
                   {
                       string s = parts[2].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       mstring msu = mstring.Prepare(su);
                       mstring msu1 = mstring.Prepare(su1);
                       val.AppendM(msu);
                       val.AppendM(msu1);
                     
                       if (parts[2] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type: mstring failed`);
                           
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                       
                   }
                     
                    // needs to be cleared before next use
                    val.Clear();
                    
                
                   try
                   {             
                       
                     uint sh = uint.Parse(parts[3].ToString());
                       val.AppendM(sh);
           
                     if (parts[3] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type:  UShort_UInt16 failed`);
                           
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false; 
                   }
                   
                    // needs to be cleared before next use
                    val.Clear();
      
                    
                  try
                     {
                       short sh1 = short.Parse(parts[4].ToString()); 
                       val.AppendM(sh1);
                     
                       if (parts[4] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type:  Short_Int16 failed`);
                           
                     }
                  catch(Exception e) 
                     {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                       }
                       
                   // needs to be cleared before next use
                    val.Clear();
                  
                    
                    
                     try
                     {
                       long l = long.Parse(parts[5].ToString());
                       val.AppendM(l);
                     
                      if (parts[5] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type:   Lone_Int64 failed`);
                           
                   
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                       
                   }
                   
                    // needs to be cleared before next use
                    val.Clear();
                    
                    
                     
                     try
                     {
                       ulong ul = ulong.Parse(parts[6].ToString());
                       val.AppendM(ul);
                     
                     if (parts[6] != val)
                           throw new Exception(`Test of the non-static Member 'AppendM()' with input data of type: ULong_UInt64 failed`);
                           
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                      
                   }
                     
                   // needs to be cleared before next use
                    val.Clear();
                   
                    
                   try
                   {
                      int n = int.Parse(parts[7].ToString());
                       val.AppendM(n);
                     
                       if (parts[7] != val)
                           throw new Exception(`Static memeber 'AppendM()' with input data of type: Int_Int32 failed`);
                           
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     }
                     
                       // needs to be cleared before next use
                      val.Clear();
                     
                     try
                     {
                      uint n1 = uint.Parse(parts[8].ToString());
                       val.AppendM(n1);
                     
                       if (parts[8] != val)
                           throw new Exception(`Static memeber 'AppendM()'  with input data of type:   UInt_UInt32 failed`);
                           
                     
                      
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                       
                   }   
                   
                   return result;
                   
              }
              
              public static bool mstring_Consume_test(ByteSlice line)
              {
                  
                   
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   
                   
                     UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                       
                   try
                   {
                     
                    string s = parts[2].ToString();
                     
                     for(int i = 0; i < s.Length; i++)
                          {
                              
                              char c = s[i];
                              val.Consume(ref c);
                              
                          }
            
                  
                     if (parts[2] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type: char failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                        
                     // needs to be cleared before next use
                     val.Clear();
                     
                     
                        try
                   {
                     
                    string s = parts[1].ToString();
                     
                     for(int i = 0; i < s.Length; i++)
                          {
                              
                              char c = s[i];
                              val.Consume(ref c);
                              
                          }
            
                  
                     if (parts[1] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type: char_unicode failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                        
                     // needs to be cleared before next use
                     val.Clear();
          /*             
                  try
                     {
                         
                     double  d = double.Parse(parts[0].ToString());
                     val.Consume(ref d);
                     
                       if (parts[0] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type: double failed`);
                     }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                     // needs to be cleared before next use
                     val.Clear();
             */        
                    
                   try
                   {
                      string s = parts[1].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       val.Consume(ref su);
                       val.Consume(ref su1);
                       if (parts[1] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type: string_unicode failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                  
                    // needs to be cleared before next use
                   val.Clear();
                     
                   try
                   {
                      string s = parts[1].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       mstring msu = mstring.Prepare(su);
                       mstring msu1 = mstring.Prepare(su1);
                       val.Consume(ref msu);
                       val.Consume(ref msu1);
                       
                       if (parts[1] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type: mstring_unicode failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                   
                    // needs to be cleared before next use
                   val.Clear();
                   
                  
                   
                   try
                   {
                     
                      string s = parts[2].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       val.Consume(ref su);
                       val.Consume(ref su1);
                       
                       if (parts[2] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type: string failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                    // needs to be cleared before next use
                   val.Clear();
                  
                   try
                   {
                      string s = parts[2].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       mstring msu = mstring.Prepare(su);
                       mstring msu1 = mstring.Prepare(su1);
                       val.Consume(ref msu);
                       val.Consume(ref msu1);
                       
                       if (parts[2] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type: mstring failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                    // needs to be cleared before next use
                     val.Clear();
                     
                     
                     try
                     {
                       ushort sh = ushort.Parse(parts[3].ToString());
                       val.Consume(ref sh);
                    
                       if (parts[3] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type:  UShort_UInt16 failed`);
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                    // needs to be cleared before next use
                     val.Clear();
                     
                     try
                     {
                     short sh1 = short.Parse(parts[4].ToString()); 
                     val.Consume(ref sh1);
                     
                       if (parts[4] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type:  Short_Int16 failed`);
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                   // needs to be cleared before next use
                   val.Clear();
                     
                   try
                   {
                      string temp2 = parts[5].ToString();
                       long l = long.Parse(temp2);
                       
                       val.Consume(ref l);
                     
                      if (parts[5] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type:   Long_Int64 failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                    // needs to be cleared before next use
                     val.Clear();
                     
                     try
                     {
                       ulong ul = ulong.Parse(parts[6].ToString());
                       val.Consume(ref ul);
                     
                      if (parts[6] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type: ULong_UInt64 failed`);
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                   // needs to be cleared before next use
                   val.Clear();
                     
                   try
                      {
                       int n = int.Parse(parts[7].ToString());
                       val.Consume(ref n);
                     
                       if (parts[7] != val)
                           throw new Exception(`Test of the non-static Member 'Consume()' with input data of type: Int_Int32 failed`);
                      }
                       catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                   // needs to be cleared before next use
                   val.Clear();
                   
                   try
                   {
                       uint n1 = uint.Parse(parts[8].ToString());
                       val.Consume(ref n1);
                     
                       if (parts[8] != val)
                           throw new Exception(`Static memeber 'Consume()' with input data of type:   UInt_UInt32 failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     // needs to be cleared before next use
                     val.Clear();
                     
                      
                   return result;
                  
                  
              }
              
             public static bool  mstring_Contains_test(ByteSlice line)
              
              {
                  
                  
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   
                    UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                       
                    try
                   {
                      string s = parts[2].ToString();
                   
                      char c = s[0];
                             
                      if (!parts[2].Contains(c))
                            
                           throw new Exception(`Test of the non-static Member 'Contains()' with input data of type: char failed`);
               
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                           
                   try
                   {
                      string s = parts[1].ToString();
                
                      char c = s[0];
                             
                       if (!parts[1].Contains(c))
                            
                           throw new Exception(`Test of the non-static Member 'Contains()' with input data of type: char_unicode failed`);
          
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                    
                     
                   try
                   {
                      string s = parts[1].ToString();
                      string su = s.Substring(s.Length/2);
                 
                       if (!parts[1].Contains(su))
                           throw new Exception(`Test of the non-static Member 'Contains()' with input data of type: string_unicode failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                 
                     
                   try
                   {
                      string s = parts[1].ToString();
                      mstring su = mstring.Prepare(s.Substring(s.Length/2));
                 
                       if (!parts[1].Contains(su))
                           throw new Exception(`Test of the non-static Member 'Contains()' with input data of type: mstring_unicode failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                
                   
                   try
                   {
                      string s = parts[2].ToString();
                      string su = s.Substring(s.Length/2);
                 
                       if (!parts[2].Contains(su))
                           throw new Exception(`Test of the non-static Member 'Contains()' with input data of type: string failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                
                  
                   try
                   {
                      string s = parts[2].ToString();
                      mstring su = mstring.Prepare(s.Substring(s.Length/2));
                 
                       if (!parts[2].Contains(su))
                           throw new Exception(`Test of the non-static Member 'Contains()' with input data of type: mstring failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                    
                  return result;
              }
              
                 public static bool mstring_Copy_test(ByteSlice line)
                 {
                   
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                     
                     UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                     
                      try
                   {
                       mstring u2 = mstring.Copy(parts[1]);
                     
                       if (u2!= parts[1])
                           throw new Exception(`Test of the non-static Member 'Copy()' with input data of type: mstring_unicode failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                 
                   
                     try
                   {
                     mstring s2 = mstring.Copy(parts[2]);
                            
                       if (s2!=parts[2])
                           throw new Exception(`Test of the non-static Member 'Copy()' with input data of type: mstring failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                     
                     return result;
               }
                     
                     
                     
                    public static bool  mstring_CsvNexItemTo_test(ByteSlice line)
                    {
                         
                         mstring sLine = mstring.Prepare(line);
                         mstringarray parts = sLine.SplitM(',');
                         mstring val = mstring.Prepare();
                         bool result = true;
                        
                          UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                          
                        
                          double d1 = sLine.CsvNextItemToDouble();           // testing CsvNextItemToDouble();
                          mstring c2 = mstring.Prepare();                           
                          c2 = sLine.CsvNextItemToString();                         // testing CsvNextItemToString() - unicode;
                          mstring c3 = mstring.Prepare(); 
                          c3 = sLine.CsvNextItemToString();                         // testing CsvNextItemToString();
                          UInt16 ush1 = sLine.NextItemToUInt16(',');           // there is no Csv to Short , but we need to get all the elements in order
                          Int16 sh1 = sLine.NextItemToInt16(',');                  // is not going to be used
                          long l1 = sLine.CsvNextItemToLong();                    // testing CsvNextItemToLong();
                          ulong l2 = sLine.NextItemToULong(',');                   //  is not going to be used
                          int num1 = sLine.CsvNextItemToInt();                    // testing CsvNextItemToInt();
                          uint num2 = sLine.NextItemToUInt(',');                   // is not going to be used 
                          
                          
                          mstring sLine1 = mstring.Prepare(line);
                          double d11 = sLine1.CsvNextItemToDouble();             // is not going to be used 
                          mstring c12 = mstring.Prepare();                           
                          c12 = sLine1.CsvNextItemToString();                           // is not going to be used
                          mstring c13 = mstring.Prepare(); 
                          c13 = sLine1.CsvNextItemToString();                           // is not going to be used
                          UInt16 ush11 = sLine1.NextItemToUInt16(',');               // there is no Csv to Short , but we need to get all the elements in order
                          Int16 sh11 = sLine1.NextItemToInt16(',');                     // is not going to be used
                          Int64 l11 = sLine1.CsvNextItemToInt64();                    // testing CsvNextItemToInt64();
                          ulong l12 = sLine1.NextItemToULong(',');                      //  is not going to be used
                          Int32 num11 = sLine1.CsvNextItemToInt32();                 // testing CsvNextItemToInt32();
                          uint num12 = sLine1.NextItemToUInt(',');                      // is not going to be used 
             /*             
                    try
                     {
                         
                     double  d =  double.Parse(parts[0].ToString());
                     
                     
                       if (d != d11)
                           throw new Exception(`Test of the non-static Member 'CsvNextItemToDouble()' with input data of type: mstring failed`);
                           
                     }
                     catch(Exception e) 
                     {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     
                      }
                     
                */      
                        
                    try
                     {
                     
                       if (parts[1] != c2)
                           throw new Exception(`Test of the non-static Member 'CsvNextItemToString()' with input data of type: mstring_unicode failed`);
                           
                     }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                      
                   }
                   
                  
                     
                    try
                    {
                      
                       if (parts[2] != c3 )
                           throw new Exception(`Test of the non-static Member 'CsvNextItemToString()' with input data of type: mstring failed`);
                           
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                       
                   }
                     
                 
                   
                     try
                     {
                       long l = long.Parse(parts[5].ToString());
                     
                     
                      if ( l != l1)
                           throw new Exception(`Test of the non-static Member 'CsvNextItemToLong()' with input data of type: long failed`);
                           
                   
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                       
                   }
                   
                 
                    
                    try
                     {
                         
                       Int64 l64 = Int64.Parse(parts[5].ToString());
                       
                                                            
                      if ( l64 != l11)
                           throw new Exception(`Test of the non-static Member 'CsvNextItemToInt64()' with input data of type:  Int64 failed`);
                           
                   
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                       
                   }
                   
                  
                   
                   
                   try
                   {
                      int n = int.Parse(parts[7].ToString());
                   
                     
                       if (n != num1)
                           throw new Exception(`Test of the non-static Member 'CsvNextItemToInt()' with input data of type:  int failed`);
                           
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     }
                     
                      
                      
                  try
                   {
                      Int32 n32 = Int32.Parse(parts[7].ToString());
                      
                     
                       if (n32 != num11)
                           throw new Exception(`Test of the non-static Member 'CsvNextItemToInt32()' with input data of type:  Int32  failed `);
                           
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     }
                     
                                      
                         return result;
                    }
                    
                    
                     public static bool mstring_EndsWith_test(ByteSlice line)
                     {
                         mstring sLine = mstring.Prepare(line);
                         mstringarray parts = sLine.SplitM(',');
                         mstring val = mstring.Prepare();
                         bool result = true;
                        
                          UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                          
                     try
                     {
                         
                         string s = parts[2].ToString();
                         string s1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                         mstring ms1 = mstring.Prepare(s1);
                         val.AppendM(s);
                 
            
                       if (!val.EndsWith(s1))
                           throw new Exception(`Test of the non-static Member 'EndsWith()' with input data of type: mstring failed`);
                           
                     }
                     catch(Exception e) 
                     {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     
                      }
                        // needs to be cleared before next use
                        val.Clear();
                        
                     try
                     {
                         string s = parts[1].ToString();
                         string s1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                         val.AppendM(s);
                 
            
                         if (!val.EndsWith(s1))
                             throw new Exception(`Test of the non-static Member 'EndsWith()' with input data of type: string failed`);
                           
                     }
                     catch(Exception e) 
                     {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     
                      }
                     
                        // needs to be cleared before next use
                        val.Clear();
                        
                        
                     try
                     {
                         
                         string s = parts[1].ToString();
                         string s1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                         mstring ms1 = mstring.Prepare(s1);
                         val.AppendM(s);
                 
            
                       if (!val.EndsWith(s1))
                           throw new Exception(`Test of the non-static Member 'EndsWith()' with input data of type: mstring_unicode failed`);
                           
                     }
                     catch(Exception e) 
                     {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     
                      }
                     
                        // needs to be cleared before next use
                        val.Clear();
                        
                     try
                     {
                         
                        string s = parts[1].ToString();
                         string s1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                         val.AppendM(s);
                 
            
                       if (!val.EndsWith(s1))
                           throw new Exception(`Test of the non-static Member 'EndsWith()' with input data of type: string_unicode failed`);
                           
                     }
                     catch(Exception e) 
                     {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     
                      }
                     
                        
                     return result;
                     }
                     
                     
                     public static bool mstring_HasNextItem_test(ByteSlice line)
                     {
                         
                         mstring sLine = mstring.Prepare(line);
                         mstringarray parts = sLine.SplitM(',');
                         bool result = true;
                         mstring val = mstring.Prepare();
                          UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                          
                     try
                     {
                        
                     
                       if (!sLine.HasNextItem(','))
                           throw new Exception(`Test of the non-static Member 'HasNextItem()' with input data of type: char failed`);
                           
                     }
                     catch(Exception e) 
                     {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     
                      }
                      
                     try
                     {
                         string s = line.ToString();
                         s += ` ` + s ;
                          mstring ms = mstring.Prepare(s);
                     
                       if (!ms.HasNextItem())
                           throw new Exception(`Test of the non-static Member 'HasNextItem()' with input data of type: no input failed`);
                           
                     }
                     catch(Exception e) 
                     {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     
                      }
                          
                          
                          return result; 
                          
                       
                         
                     }
                     
                     public static bool mstring_IndexOf_test(ByteSlice line)
                     
                     {
                         mstring sLine = mstring.Prepare(line);
                         mstringarray parts = sLine.SplitM(',');
                         bool result = true;
                         
                         
                     UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                       
                   try
                   {
                       string temp = parts[2].ToString();
                       mstring temp1 = mstring.Prepare(temp);
                       
                       int counter = 0;
                       int counter1 = 0;
                       
                       for(int i = 0 ; i < temp.Length; i++)
                       {
                       
                          char c = temp[i];
                         
                          int index = temp1.IndexOf(c);
                          
                          counter += index;
                    
                       }
                       
                          for(int j = 0 ; j < temp.Length; j++)
                       {
                       
                          char c = temp[j];
                         
                          int index = temp1.IndexOf(c);
                          
                          counter1 += index;
                    
                       }
                       
                           if (counter1 != counter )
                                throw new Exception(`Test of the non-static Member 'IndexOf()' with input data of type: char failed`);
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                 
                    
                   try
                   {
                       
                       string temp = parts[1].ToString();
                       mstring temp1 = mstring.Prepare(temp);
                       
                       int counter = 0;
                       int counter1 = 0;
                       
                       for(int i = 0 ; i < temp.Length; i++)
                       {
                       
                          char c = temp[i];
                         
                          int index = temp1.IndexOf(c);
                          
                          counter += index;
                    
                       }
                       
                          for(int j = 0 ; j < temp.Length; j++)
                       {
                       
                          char c = temp[j];
                         
                          int index = temp1.IndexOf(c);
                          
                          counter1 += index;
                    
                       }
                       
                           if (counter1 != counter )
                                throw new Exception(`Test of the non-static Member 'IndexOf()' with input data of type: char_unicode failed`);
                      
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
         /*            
                 try
                   {
                       
                       string temp = parts[2].ToString();
                       mstring m = mstring.Prepare(temp.Substring(temp.Length/2,3));
                       mstring m1 = mstring.Prepare(temp.Substring(temp.Length/2,5));
                     
                       
                       
                          int index = parts[2].IndexOf(m);
                          int index1 = parts[2].IndexOf(m1);
                     
                           if (index !=  index1 )
                                throw new Exception(`Test of the non-static Member 'IndexOf()' with input data of type: mstring failed`);
                      
                       
                       
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
           */      
                     
                   try
                   {
                       string temp = parts[1].ToString();
                       mstring m = mstring.Prepare(temp.Substring(temp.Length/2,5));
                       mstring m1 = mstring.Prepare(temp.Substring(temp.Length/2,5));
                     
                       
                       
                          int index = parts[1].IndexOf(m);
                          int index1 = parts[1].IndexOf(m1);
                     
                           if (index !=  index1 )
                                throw new Exception(`Test of the non-static Member 'IndexOf()' with input data of type: mstring_unicode failed`);
                       }
                   
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       
                       result = false;
                     
                   }
                   
             /*    
                  try
                   {
                       string temp = parts[2].ToString();
                       string s = temp.Substring(temp.Length/2,3);
                       string s1 = temp.Substring(temp.Length/2,5);
                     
                       
                       
                          int index = parts[2].IndexOf(s);
                          int index1 = parts[2].IndexOf(s1);
                     
                           if (index !=  index1 )
                                throw new Exception(`Test of the non-static Member 'IndexOf()' with input data of type: string failed`);
                       
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
               */    
                     
                   try
                   {
                       string temp = parts[1].ToString();
                       string s = temp.Substring(temp.Length/2,5);
                       string s1 = temp.Substring(temp.Length/2,5);
                     
                       
                       
                          int index = parts[1].IndexOf(s);
                          int index1 = parts[1].IndexOf(s1);
                     
                           if (index !=  index1 )
                                throw new Exception(`Test of the non-static Member 'IndexOf()' with input data of type: string_unicode failed`);
                       
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                         
                         return result;
                         
                     }
                     
                     
             public static bool mstring_MAppend_test(ByteSlice line)
               {
                  
                
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                        
                   
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                      
                   
                try
                   {
                     string s = parts[2].ToString();
                     
                     for(int i = 0; i < s.Length; i++)
                          {
                              
                              char c = s[i];
                              val.MAppend(c);
                              
                          }
            
                     if (parts[2] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type: char failed`);
           
                   }
                catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                  
                    // needs to be cleared before next use
                     val.Clear();
                     
                     
                 try
                   {
                     string s = parts[1].ToString();
                     
                     for(int i = 0; i < s.Length; i++)
                          {
                              
                              char c = s[i];
                              val.MAppend(c);
                              
                          }
                                        
                     if (parts[1] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type: char_unicode failed`);
              
                   }
                 catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                    // needs to be cleared before next use
                     val.Clear();
                   
        /*          
                 try
                  {
                     double  d =  double.Parse(parts[0].ToString());
                     val.MAppend(d);
                     
                       if (parts[0] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type: double failed`);
                           
                  }
                catch(Exception e) 
                  {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     
                   }
                   
                      // needs to be cleared before next use
                        val.Clear();
           */         
                 try
                   {
                      
                       string s = parts[1].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       val.MAppend(su);
                       val.MAppend(su1);
                                
                       if (parts[1] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type: string_unicode failed`);
                         
                   }
                catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                    }
                     
                    // needs to be cleared before next use
                    val.Clear();  
                   
                     
                     try
                     {
                        string s = parts[1].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       mstring msu = mstring.Prepare(su);
                       mstring msu1 = mstring.Prepare(su1);
                       val.MAppend(msu);
                       val.MAppend(msu1);
                 
                       if (parts[1] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type: mstring_unicode failed`);
                           
                     }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                      
                   }
                   
                   
                    // needs to be cleared before next use
                    val.Clear();
                  
                    try
                    {
                       string s = parts[2].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       val.MAppend(su);
                       val.MAppend(su1);
                       if (parts[2] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type: string failed`);
                           
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                       
                   }
                     
                    
                   // needs to be cleared before next use
                    val.Clear();
                    
                  
                   try
                   {
                       string s = parts[2].ToString();
                   
                       string su = s.Substring(0,s.Length/2);
                       string su1 = s.Substring(s.Length/2,s.Length-s.Length/2);
                       mstring msu = mstring.Prepare(su);
                       mstring msu1 = mstring.Prepare(su1);
                       val.MAppend(msu);
                       val.MAppend(msu1);
                     
                       if (parts[2] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type: mstring failed`);
                           
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                       
                   }
                     
                    // needs to be cleared before next use
                    val.Clear();
                    
                
                   try
                   {             
                       
                     uint sh = uint.Parse(parts[3].ToString());
                       val.MAppend(sh);
           
                     if (parts[3] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type:  UShort_UInt16 failed`);
                           
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false; 
                   }
                   
                    // needs to be cleared before next use
                    val.Clear();
      
                    
                  try
                     {
                       short sh1 = short.Parse(parts[4].ToString()); 
                       val.MAppend(sh1);
                     
                       if (parts[4] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type:  Short_Int16 failed`);
                           
                     }
                  catch(Exception e) 
                     {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                       }
                       
                   // needs to be cleared before next use
                    val.Clear();
                  
                    
                    
                     try
                     {
                       long l = long.Parse(parts[5].ToString());
                       val.MAppend(l);
                     
                      if (parts[5] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type:   Lone_Int64 failed`);
                           
                   
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                       
                   }
                   
                    // needs to be cleared before next use
                    val.Clear();
                    
                    
                     
                     try
                     {
                       ulong ul = ulong.Parse(parts[6].ToString());
                       val.MAppend(ul);
                     
                     if (parts[6] != val)
                           throw new Exception(`Test of the non-static Member 'MAppend()' with input data of type: ULong_UInt64 failed`);
                           
                     }
                      catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                      
                   }
                     
                   // needs to be cleared before next use
                    val.Clear();
                   
                    
                   try
                   {
                      int n = int.Parse(parts[7].ToString());
                       val.MAppend(n);
                     
                       if (parts[7] != val)
                           throw new Exception(`Static memeber 'MAppend()' with input data of type: Int_Int32 failed`);
                           
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;  
                     }
                     
                       // needs to be cleared before next use
                      val.Clear();
                     
                     try
                     {
                      uint n1 = uint.Parse(parts[8].ToString());
                       val.MAppend(n1);
                     
                       if (parts[8] != val)
                           throw new Exception(`Static memeber 'MAppend()'  with input data of type:   UInt_UInt32 failed`);
                           
                     
                      
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                       
                   }   
                   
                   return result;
                 
                   
              }
              
              
             public static bool mstring_MPadLeft_test(ByteSlice line)
             {
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   bool result = true;
                                           
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                   
                  try
                   {
                     mstring val = mstring.Prepare();
                     string ms =parts[2].ToString();
                     ms = ms.PadLeft(40,'.');
                     val = parts[2].MPadLeft(40,'.'); 
                 
                     if (val.ToString() != ms) 
                           throw new Exception(`Test of the non-static Member 'PadLeft()' with input data of type: char failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                  
                     
                   try
                   {
                     mstring val = mstring.Prepare();
                     string ms =parts[1].ToString();
                      ms = ms.PadLeft(40,'.');
                      val = parts[1].MPadLeft(40,'.');
                     
                   
                                           
                     if (val.ToString() != ms) 
                           throw new Exception(`Test of the non-static Member 'MPadLeft()' with input data of type: char_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                             
                 
                 
                 return result;
             }
                
                  public static bool mstring_MPadRight_test(ByteSlice line)
             {
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   bool result = true;
                                           
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                   
                  try
                   {
                     mstring val = mstring.Prepare();
                     string ms =parts[2].ToString();
                     ms = ms.PadRight(40,'.');
                     val = parts[2].MPadRight(40,'.'); 
                 
                     if (val.ToString() != ms) 
                           throw new Exception(`Test of the non-static Member 'MPadRight()' with input data of type: char failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                  
                     
                   try
                   {
                     mstring val = mstring.Prepare();
                     string ms =parts[1].ToString();
                      ms = ms.PadRight(40,'.');
                      val = parts[1].MPadRight(40,'.');
                     
                   
                                           
                     if (val.ToString() != ms) 
                           throw new Exception(`Test of the non-static Member 'MPadRight()' with input data of type: char_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
        
                 return result;
             }
             
             public static bool mstring_MReplace_test(ByteSlice line)
             {
                 
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   bool flag = true;
                   bool result = true;
                 
                 
               UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                   
                  try
                   {
                      
                    string ms = parts[2].ToString();
                    char oldChar = ms[0];
                    char newChar = ms[1];
                    mstring val = mstring.Prepare();
                    val = parts[2].MReplace(oldChar,newChar);
                    string mr = val.ToString();
                   
                    
                     if ( mr[0] != mr[1] ) 
                           throw new Exception(`Test of the non-static Member 'MReplace()' with input data of type: char _ char failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                   
                      try
                   {
                      
                    string ms = parts[1].ToString();
                    char oldChar = ms[0];
                    char newChar = ms[1];
                    mstring val = mstring.Prepare();
                    val = parts[1].MReplace(oldChar,newChar);
                    string mr = val.ToString();
                    
                    
                     if (mr[0] != mr[1] ) 
                           throw new Exception(`Test of the non-static Member 'MReplace()' with input data of type: (char _ char)_unicode failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                   
                        
                    try
                   {
                      
                    string ms = parts[2].ToString();
                    mstring oldStr = mstring.Prepare(ms.Substring(0,2));
                    mstring newStr = mstring.Prepare(ms.Substring(10,2));
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[2].MReplace(ref oldStr,ref newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'MReplace()' with input data of type: mstring _ mstring failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                  
                    try
                   {
                      
                   string ms = parts[1].ToString();
                    mstring oldStr = mstring.Prepare(ms.Substring(0,2));
                    mstring newStr = mstring.Prepare(ms.Substring(10,2));
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[1].MReplace(ref oldStr,ref newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'MReplace()' with input data of type: (mstring_ msring)_unicode failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                      
                     try
                   {
                       mstring s = mstring.Prepare(`x`);
                       mstring o = mstring.Prepare();
                       mstring n = mstring.Prepare(`p`);
                    
                       s.MReplace(ref o, ref n);
                       flag = true;
                                  
                   }
                     catch(ArgumentException e)
                   {
                       if(e.Message.IndexOf(`zero length`) > -1)
                      {
                           flag = false;
                       }  
                    }
                    finally
                    {
                          if(flag == true)
                      {
                           Log.Qizmt_Log(`Error : Test of the non-static Member 'MReplace()' with input data of type: mstring_mstring_negative has  failed`);
                       }  
                       
                    }
                    
                     try
                   {
                      
                    string ms = parts[2].ToString();
                    mstring oldStr = mstring.Prepare(ms.Substring(0,2));
                    string newStr = ms.Substring(10,2);
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[2].MReplace(ref oldStr,newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'MReplace()' with input data of type: (mstring_string) failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                   
                     try
                   {
                      
                    string ms = parts[1].ToString();
                    mstring oldStr = mstring.Prepare(ms.Substring(0,2));
                    string newStr = ms.Substring(10,2);
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[1].MReplace(ref oldStr,newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'MReplace()' with input data of type: (mstring_ string)_unicode failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                    try
                   {
                       mstring s = mstring.Prepare(`x`);
                       mstring o = mstring.Prepare();
                       string n = `p`;
                    
                       s.MReplace(ref o, n);
                       
                       flag = true;
                                  
                   }
                     catch(ArgumentException e)
                   {
                       if(e.Message.IndexOf(`zero length`) > -1)
                      {
                           flag = false;
                       }  
                    }
                      finally
                    {
                          if(flag == true)
                      {
                           Log.Qizmt_Log(`Error : Test of the non-static Member 'MReplace()' with input data of type: mstring_string_negative has  failed`);
                       }  
                       
                    }
                    
                    
                      try
                   {
                      
                    string ms = parts[2].ToString();
                    string oldStr = ms.Substring(0,2);
                    mstring newStr = mstring.Prepare(ms.Substring(10,2));
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[2].MReplace(oldStr,ref newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'MReplace()' with input data of type: (string_msring) failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                   
                     try
                   {
                      
                    string ms = parts[1].ToString();
                    string oldStr = ms.Substring(0,2);
                    mstring newStr = mstring.Prepare(ms.Substring(10,2));
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[1].MReplace(oldStr,ref newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'MReplace()' with input data of type: (string_mstring)_unicode failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                    try
                   {
                       mstring s = mstring.Prepare(`x`);
                       string o = null;
                       mstring n = mstring.Prepare(`p`);
                    
                       s.MReplace(o, ref n);
                       flag = true;
                                  
                   }
                     catch(ArgumentException e)
                   {
                       if(e.Message.IndexOf(`zero length`) > -1)
                      {
                           flag = false;
                       }  
                    }
                     finally
                    {
                          if(flag == true)
                      {
                           Log.Qizmt_Log(`Error : Test of the non-static Member 'MReplace()' with input data of type: string_mstring_negative has  failed`);
                       }  
                     
                    }
                    
                    
                   
                       try
                   {
                      
                      string ms = parts[2].ToString();
                    string oldStr = ms.Substring(0,2);
                    string newStr = ms.Substring(10,2);
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[2].MReplace(oldStr,newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'MReplace()' with input data of type: (string_string) failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                   
                     try
                   {
                      
                    string ms = parts[1].ToString();
                    string oldStr = ms.Substring(0,2);
                    string newStr = ms.Substring(10,2);
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[1].MReplace(oldStr,newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'MReplace()' with input data of type: (string_string)_unicode failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                    try
                   {
                       mstring s = mstring.Prepare(`x`);
                       string o = null;
                       string n = `p`;
                    
                       s.MReplace(o, n);
                       
                       flag = true;
                                  
                   }
                     catch(ArgumentException e)
                   {
                       if(!(e.Message.IndexOf(`zero length`) > -1))
                      {
                           flag = false ;
                       }  
                    }
                     finally
                    {
                          if(flag == true)
                      {
                           Log.Qizmt_Log(`Error : Test of the non-static Member 'MReplace()' with input data of type: string_string_negative has  failed`);
                       }  
                       
                    }
                    
                    
                   
                 return result;
             }
             
          public static bool mstring_MSplit_test(ByteSlice line)
          {
                   
                   mstring sLine = mstring.Prepare(line); // there is 10 parts in the input string
                   mstringarray parts = sLine.MSplit(',');
                   bool result = true;
                                           
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                     try
                   {
                   
                     if (parts.Length!=10)
                           throw new Exception(`Test of the non-static Member 'MSplit()' with input data of type: char failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                      try
                   {
           
                    string unicode = parts[1].ToString();
                    
                    char c = unicode[3];
                    
                    mstringarray par = parts[1].MSplit(c);
                    string [] pars = unicode.Split(c);
                    
                    
                     if (par.Length != pars.Length )
                           throw new Exception(`Test of the non-static Member 'MSplit()' with input data of type: char_unicode failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
         
                   return result; 
                   
                
            }
             
           
             public static bool mstring_MSubstring_test(ByteSlice line)
             {
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   bool flag = true;
                   
                   
                      UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                      try
                   {
                       string s = parts[2].ToString();
                       string s1 = s.Substring(10);
                       mstring m1 = parts[2].MSubstring(10);
                       mstring m2 = mstring.Prepare(s1);
                     
                     if (m2 != m1)
                           throw new Exception(`Test of the non-static Member 'MSubstring()' with input data of type: int failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                      try
                   {
                       
                       string s1 = parts[2].ToString();
                       string s2 = s1.Substring(10);
                       mstring m1 = parts[2].MSubstring(10);
                       mstring m2 = mstring.Prepare(s2);
                     
                     if (m2 != m1)
                           throw new Exception(`Test of the non-static Member 'MSubstring()' with input data of type: int_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                  try
                   {
                       string s = parts[2].ToString();
                       string s1 = s.Substring(10,5);
                       mstring m1 = parts[2].MSubstring(10,5);
                       mstring m2 = mstring.Prepare(s1);
                     
                     if (m2 != m1)
                           throw new Exception(`Test of the non-static Member 'MSubstring()' with input data of type: int_int failed`);
                   
                   }
                 catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                      try
                   {
                       
                       string s1 = parts[1].ToString();
                       string s2 = s1.Substring(1,3);
                       mstring m1 = parts[1].MSubstring(1,3);
                       mstring m2 = mstring.Prepare(s2);
                     
                     if (m2 != m1)
                           throw new Exception(`Test of the non-static Member 'MSubstring()' with input data of type: int_int_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                      try
                   {
                       
                       mstring s1 = mstring.Prepare(`xyz`);
                     
                       s1.MSubstring(-1, 1);
                       
                     
                           
                    
                   }
                  catch(ArgumentOutOfRangeException e)
                  {
                        if(e.Message.IndexOf(`negative`) > -1)
                          {
                              flag = false;
                          }
                   }
                   
                     finally
                   {
                       if(flag == true)
                     {
                          Log.Qizmt_Log(`Error : Test of the non-static Member 'MSubstring()' with index outside of  boundary failed`);
                         flag = true;
                     }     
                    
                   }
      
                   
                       try
                   {
                       
                       mstring s1 = mstring.Prepare(`xyz`);
                     
                       s1.MSubstring(1, 5);
                       
                                    
                    
                   }
                  catch(ArgumentOutOfRangeException e)
                  {
                     
                      if(e.Message.IndexOf(`negative`) > -1)
                          {
                              flag = false;
                          }
                          
                   }
                   finally
                   {
                       if(flag == true)
                     {
                          Log.Qizmt_Log(`Error : Test of the non-static Member 'MSubstring()' with index outside of  boundary failed`);
                          flag = true;
                     }  
                     
                   }
               
      
                  return result; 
               
             }
             
             
             public static bool mstring_MToLower_mapper_test(ByteSlice line)
             {
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   
                     UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                  
                   try
                   {
                       string s = parts[2].ToString();
                       string s1 = s.ToLower();
                       mstring sLine2 = parts[2].MToLower();
                       mstring sLine1 = mstring.Prepare(s1);
                     
                     if (sLine1 != sLine2)
                           throw new Exception(`Test of the non-static Member 'MToLower()'  failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                   try
                   {
                    
                       string s1 = parts[1].ToString();
                       s1 = s1.ToLower();
                       mstring m1 = parts[1].MToLower();
                       mstring m2 = mstring.Prepare(s1);
                     
                     if (m1 != m2)
                           throw new Exception(`Test of the non-static Member 'MToLower()' _ unicode  failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                 
                                
                 return result;
             }
             
               public static bool mstring_MToUpper_mapper_test(ByteSlice line)
             {
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   
                     UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                  
                   try
                   {
                       string s = parts[2].ToString();
                       string s1 = s.ToUpper();
                       mstring sLine2 = parts[2].MToUpper();
                       mstring sLine1 = mstring.Prepare(s1);
                     
                     if (sLine1 != sLine2)
                           throw new Exception(`Test of the non-static Member 'MToUpper()'  failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                   try
                   {
                    
                       string s1 = parts[1].ToString();
                       s1 = s1.ToUpper();
                       mstring m1 = parts[1].MToUpper();
                       mstring m2 = mstring.Prepare(s1);
                     
                     if (m1 != m2)
                           throw new Exception(`Test of the non-static Member 'MToUpper()' _ unicode  failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                 
                                
                 return result;
             }
             
             public static bool mstring_MTrinM_mapper_test(ByteSlice line)
            {
             
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                
                   
                  UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                  
                   try
                   {
                       string s = parts[2].ToString();
                       char t = s[5];
                       s = s.Trim(t);
                       mstring m =  parts[2].MTrim(t);
                       mstring ms = mstring.Prepare(s);
                     
                     if (ms != m)
                           throw new Exception(`Test of the non-static Member 'MTrinm()' with input data of type: char failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                     try
                   {
                       string s = parts[1].ToString();
                       char t = s[5];
                       s = s.Trim(t);
                       mstring m =  parts[1].MTrim(t);
                       mstring ms = mstring.Prepare(s);
                     
                     if (ms != m)
                           throw new Exception(`Test of the non-static Member 'MTrinm()' with input data of type: char_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                     try
                   {
                        char[] whiteSpaceChars = new char[] { '\u0009', '\u000C', '\u0020','\u2000','\u2001','\u2002','\u2003','\u2004', 
                 '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200A', '\u200B', '\u3000'};
                       string s = parts[2].ToString();
                       
                       string trailingSpaices = null ;
                       
                for(int i = 0 ; i< whiteSpaceChars.Length ; i++)
                    {
                        trailingSpaices += whiteSpaceChars[i];
                    }
                       s = trailingSpaices + s +  trailingSpaices;
                       mstring m =  mstring.Prepare(s);
                       s = s.Trim();
                      m = m.MTrim();
                       mstring ms = mstring.Prepare(s);
                     
                     if (ms != m)
                           throw new Exception(`Test of the non-static Member 'MTrinm()' with input data of type: char(by default- white space) failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                      try
                   {
                        char[] whiteSpaceChars = new char[] { '\u0009', '\u000C', '\u0020','\u2000','\u2001','\u2002','\u2003','\u2004', 
                 '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200A', '\u200B', '\u3000'};
                       string s = parts[1].ToString();
                       
                       string trailingSpaices = null ;
                       
                for(int i = 0 ; i< whiteSpaceChars.Length ; i++)
                    {
                        trailingSpaices += whiteSpaceChars[i];
                    }
                        s = trailingSpaices + s +  trailingSpaices;
                        mstring m =  mstring.Prepare(s);
                        s = s.Trim();
                        m = m.MTrim();
                        mstring ms = mstring.Prepare(s);
                     
                     if (ms != m)
                           throw new Exception(`Test of the non-static Member 'MTrinm()' with input data of type: char(by default- white space)_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
    
                   
                   return result;
               
             }
             
             
             public static bool mstring_NextItemTo_test(ByteSlice line)
             {
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   
                    UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                
                   mstring sLine1 = mstring.Prepare(line);
                   double d = sLine1.NextItemToDouble(',');             
                   mstring cu = mstring.Prepare();                           
                   cu = sLine1.NextItemToString(',');                          
                   mstring cs = mstring.Prepare(); 
                   cs= sLine1.NextItemToString(',');                           
                   UInt16 uint16 = sLine1.NextItemToUInt16(',');              
                   Int16 int16 = sLine1.NextItemToInt16(',');                     
                   Int64 int64 = sLine1.NextItemToInt64(',');                   
                   UInt64 uint64 = sLine1.NextItemToUInt64(',');                      
                   Int32 int32 = sLine1.NextItemToInt32(',');                
                   UInt32 uint32 = sLine1.NextItemToUInt32(',');      
                   
                         
                   mstring sLine2 = mstring.Prepare(line);
                   double d1 = sLine2.NextItemToDouble(',');             
                   mstring cu1 = mstring.Prepare();                           
                   cu1 = sLine2.NextItemToString(',');                          
                   mstring cs1 = mstring.Prepare(); 
                   cs1 = sLine2.NextItemToString(',');           
                   ushort ush = sLine2.NextItemToUShort(',');              
                   short sh = sLine2.NextItemToShort(',');                     
                   long l = sLine2.NextItemToLong(',');                   
                   ulong ul = sLine2.NextItemToULong(',');                      
                   int i = sLine2.NextItemToInt(',');                
                   uint ui = sLine2.NextItemToUInt(',');      
                   
             /*                 
                   try
                   {
                       val.AppendM(d);
                     
                     if (val != parts[0] )
                           throw new Exception(`Test of the non-static Member 'NextItemToInt()' with input data of type: double failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                 
                   val.Clear();
                */   
                      try
                   {
                       val.AppendM(cu);
                     
                     if (val != parts[1] )
                           throw new Exception(`Test of the non-static Member 'NextItemToString()' with input data of type: string_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                      try
                   {
                       val.AppendM(cs);
                     
                     if (val != parts[2] )
                           throw new Exception(`Test of the non-static Member 'NextItemToString()' with input data of type: string failed`);
                          
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                  try
                   {
                       val.AppendM(uint16);
                     
                     if (val != parts[3] )
                           throw new Exception(`Test of the non-static Member 'NextItemToUInt16()' with input data of type: UInt16 failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                    try
                   {
                       val.AppendM(ush);
                     
                     if (val != parts[3] )
                           throw new Exception(`Test of the non-static Member 'NextItemToUshort()' with input data of type: UShort failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                    try
                   {
                       val.AppendM(int16);
                     
                     if (val != parts[4] )
                           throw new Exception(`Test of the non-static Member 'NextItemToInt16()' with input data of type: Int16 failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                    try
                   {
                       val.AppendM(sh);
                     
                     if (val != parts[4] )
                           throw new Exception(`Test of the non-static Member 'NextItemToshort()' with input data of type: Short failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                     try
                   {
                       val.AppendM(int64);
                     
                     if (val != parts[5] )
                           throw new Exception(`Test of the non-static Member 'NextItemToInt64()' with input data of type: Int64 failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                    try
                   {
                       val.AppendM(l);
                     
                     if (val != parts[5] )
                           throw new Exception(`Test of the non-static Member 'NextItemToLong()' with input data of type: Long failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                    try
                   {
                       val.AppendM(uint64);
                     
                     if (val != parts[6] )
                           throw new Exception(`Test of the non-static Member 'NextItemToUInt64()' with input data of type: UInt64 failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                    try
                   {
                       val.AppendM(ul);
                     
                     if (val != parts[6] )
                           throw new Exception(`Test of the non-static Member 'NextItemToULong()' with input data of type: ULong failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                   try
                   {
                       val.AppendM(int32);
                     
                     if (val != parts[7] )
                           throw new Exception(`Test of the non-static Member 'NextItemToInt32()' with input data of type: Int32 failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                  try
                   {
                       val.AppendM(i);
                     
                     if (val != parts[7] )
                           throw new Exception(`Test of the non-static Member 'NextItemToInt()' with input data of type: Int failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                          try
                   {
                       val.AppendM(uint32);
                     
                     if (val != parts[8] )
                           throw new Exception(`Test of the non-static Member 'NextItemToUInt32()' with input data of type: UInt32 failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   val.Clear();
                   
                  try
                   {
                       val.AppendM(ui);
                     
                     if (val != parts[8] )
                           throw new Exception(`Test of the non-static Member 'NextItemToUInt()' with input data of type: uint failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
             
                 return result;
   
             }
             
             public static bool mstring_operatorEqual_test(ByteSlice line)
             {
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   
                    UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
                  try
                   {
                       mstring m1 = mstring.Prepare(parts[2].ToString());
                       mstring m2 = mstring.Prepare(parts[2].ToString());
                       int i = ( m1 == m2 ? 1: 0);
                     
                     if (i == 0)
                           throw new Exception(`Test of Equal operator  with input data of type: mstring failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   try
                   {
                       mstring m1 = mstring.Prepare(parts[1].ToString());
                       mstring m2 = mstring.Prepare(parts[1].ToString());
                       int i = ( m1 == m2 ? 1: 0);
                     
                     if (i == 0)
                           throw new Exception(`Test of Equal operator  with input data of type: mstring_unicode failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                 
                 
                    return result;
             }
             
             public static bool mstring_operatorNotEqual_test(ByteSlice line)
             {
                 
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   
                    UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
                     try
                   {
                       mstring m1 = mstring.Prepare(parts[2].ToString());
                       mstring m2 = mstring.Prepare(parts[2].ToString()+`x`);
                       int i = ( m1 == m2 ? 1: 0);
                     
                     if (i == 1)
                           throw new Exception(`Test of Not Equal operator  with input data of type: mstring failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   try
                   {
                       mstring m1 = mstring.Prepare(parts[1].ToString());
                       mstring m2 = mstring.Prepare(parts[1].ToString() + `x`);
                       int i = ( m1 == m2 ? 1: 0);
                     
                     if (i == 1)
                           throw new Exception(`Test of Not Equal operator  with input data of type: mstring_unicode failed`);
                           
                    
                   }
                   catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                 
                    return result;
             } 
             
            public static bool mstring_PadLeftM_test(ByteSlice line)
            {
                
                 mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   bool result = true;
                                           
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                   
                  try
                   {
                     mstring val = mstring.Prepare();
                     string ms =parts[2].ToString();
                     ms = ms.PadLeft(40,'.');
                     val = parts[2].PadLeftM(40,'.'); 
                 
                     if (val.ToString() != ms) 
                           throw new Exception(`Test of the non-static Member 'PadLeft()' with input data of type: char failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                  
                     
                   try
                   {
                     mstring val = mstring.Prepare();
                     string ms =parts[1].ToString();
                      ms = ms.PadLeft(40,'.');
                      val = parts[1].PadLeftM(40,'.');
                     
                   
                                           
                     if (val.ToString() != ms) 
                           throw new Exception(`Test of the non-static Member 'PadLeftM()' with input data of type: char_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                             
                 
                 
                 return result;
                
            }
            
              public static bool mstring_PadRightM_test(ByteSlice line)
            {
                
                  mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   bool result = true;
                                           
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                   
                  try
                   {
                     mstring val = mstring.Prepare();
                     string ms =parts[2].ToString();
                     ms = ms.PadRight(40,'.');
                     val = parts[2].PadRightM(40,'.'); 
                 
                     if (val.ToString() != ms) 
                           throw new Exception(`Test of the non-static Member 'PadRightM()' with input data of type: char failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                  
                     
                   try
                   {
                     mstring val = mstring.Prepare();
                     string ms =parts[1].ToString();
                      ms = ms.PadRight(40,'.');
                      val = parts[1].PadRightM(40,'.');
                     
                   
                                           
                     if (val.ToString() != ms) 
                           throw new Exception(`Test of the non-static Member 'PadRightM()' with input data of type: char_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
        
                 return result;
                
            }
            
            public static bool mstring_Prepare_test(ByteSlice line)
            {
                 mstring sLine = mstring.Prepare(line);
                 mstringarray parts = sLine.SplitM(',');
                 bool result = true;
                                           
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
                  try
                   {
                       double x = double.Parse(parts[0].ToString());
                       mstring m = mstring.Prepare(x);
                       mstring m1 = mstring.Prepare();
                       m1.AppendM(x);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: double failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                    try
                   {
                       string s = line.ToString();
                       byte [] buf = System.Text.Encoding.Unicode.GetBytes(s);
                       mstring m = mstring.Prepare(buf,0,buf.Length);
                       mstring m1 = mstring.Prepare();
                       m1 = m1.AppendM(s);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: char failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                      try
                   {
                       string s = line.ToString();
                       mstring m = mstring.Prepare();
                       m = m.AppendM(s);
                       mstring m1 = mstring.Prepare(s);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: char failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                     try
                   {
                       string s = parts[2].ToString();
                       mstring m1 = mstring.Prepare();
                      
                        
                       for(int i = 0; i < s.Length; i++)
                       {
                           char c = s[i];
                         
                         mstring m = mstring.Prepare(c);
                      
                         m1 = m1.AppendM(m);
                       }
                                           
                     if ( parts[2] != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: char failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                     try
                   {
                       string s = parts[2].ToString();
                       mstring m = mstring.Prepare(s);
                       mstring m1 = mstring.Prepare();
                       m1.AppendM(s);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: string failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                     try
                   {
                       string s = parts[1].ToString();
                       mstring m = mstring.Prepare(s);
                       mstring m1 = mstring.Prepare();
                       m1.AppendM(s);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: string_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                    try
                   {
                       ushort x = ushort.Parse(parts[3].ToString());
                       mstring m = mstring.Prepare(x);
                       mstring m1 = mstring.Prepare();
                       m1.AppendM(x);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: ushort failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   try
                   {
                       short x = short.Parse(parts[4].ToString());
                       mstring m = mstring.Prepare(x);
                       mstring m1 = mstring.Prepare();
                       m1.AppendM(x);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: short failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   try
                   {
                       long x = long.Parse(parts[5].ToString());
                       mstring m = mstring.Prepare(x);
                       mstring m1 = mstring.Prepare();
                       m1.AppendM(x);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: long failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   try
                   {
                       ulong x = ulong.Parse(parts[6].ToString());
                       mstring m = mstring.Prepare(x);
                       mstring m1 = mstring.Prepare();
                       m1.AppendM(x);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: ulong failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                 
                  try
                   {
                       int x = int.Parse(parts[7].ToString());
                       mstring m = mstring.Prepare(x);
                       mstring m1 = mstring.Prepare();
                       m1.AppendM(x);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: int failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                     try
                   {
                       uint x = uint.Parse(parts[8].ToString());
                       mstring m = mstring.Prepare(x);
                       mstring m1 = mstring.Prepare();
                       m1.AppendM(x);
                                           
                     if ( m != m1) 
                           throw new Exception(`Test of the non-static Member 'Prepare()' with input data of type: uint failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                
                
                   return result;
            }
            
            
            public static bool   mstring_ReplaceM_test(ByteSlice line)
            {
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   bool flag = true;
                   bool result = true;
                 
                 
               UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                   
                  try
                   {
                      
                    string ms = parts[2].ToString();
                    char oldChar = ms[0];
                    char newChar = ms[1];
                    mstring val = mstring.Prepare();
                    val = parts[2].ReplaceM(oldChar,newChar);
                    string mr = val.ToString();
                   
                    
                     if ( mr[0] != mr[1] ) 
                           throw new Exception(`Test of the non-static Member 'ReplaceM()' with input data of type: char _ char failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                   
                      try
                   {
                      
                    string ms = parts[1].ToString();
                    char oldChar = ms[0];
                    char newChar = ms[1];
                    mstring val = mstring.Prepare();
                    val = parts[1].ReplaceM(oldChar,newChar);
                    string mr = val.ToString();
                    
                    
                     if (mr[0] != mr[1] ) 
                           throw new Exception(`Test of the non-static Member 'ReplaceM()' with input data of type: (char _ char)_unicode failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                   
                        
                    try
                   {
                      
                    string ms = parts[2].ToString();
                    mstring oldStr = mstring.Prepare(ms.Substring(0,2));
                    mstring newStr = mstring.Prepare(ms.Substring(10,2));
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[2].ReplaceM(ref oldStr,ref newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'ReplaceM()' with input data of type: mstring _ mstring failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                  
                    try
                   {
                      
                   string ms = parts[1].ToString();
                    mstring oldStr = mstring.Prepare(ms.Substring(0,2));
                    mstring newStr = mstring.Prepare(ms.Substring(10,2));
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[1].ReplaceM(ref oldStr,ref newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'ReplaceM()' with input data of type: (mstring_ msring)_unicode failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                      
                     try
                   {
                       mstring s = mstring.Prepare(`x`);
                       mstring o = mstring.Prepare();
                       mstring n = mstring.Prepare(`p`);
                    
                       s.ReplaceM(ref o, ref n);
                       flag = true;
                                  
                   }
                     catch(ArgumentException e)
                   {
                       if(e.Message.IndexOf(`zero length`) > -1)
                      {
                           flag = false;
                       }  
                    }
                    finally
                    {
                          if(flag == true)
                      {
                           Log.Qizmt_Log(`Error : Test of the non-static Member 'ReplaceM()' with input data of type: mstring_mstring_negative has  failed`);
                       }  
                       
                    }
                    
                     try
                   {
                      
                    string ms = parts[2].ToString();
                    mstring oldStr = mstring.Prepare(ms.Substring(0,2));
                    string newStr = ms.Substring(10,2);
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[2].ReplaceM(ref oldStr,newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'ReplaceM()' with input data of type: (mstring_string) failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                   
                     try
                   {
                      
                    string ms = parts[1].ToString();
                    mstring oldStr = mstring.Prepare(ms.Substring(0,2));
                    string newStr = ms.Substring(10,2);
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[1].ReplaceM(ref oldStr,newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'ReplaceM()' with input data of type: (mstring_ string)_unicode failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                    try
                   {
                       mstring s = mstring.Prepare(`x`);
                       mstring o = mstring.Prepare();
                       string n = `p`;
                    
                       s.ReplaceM(ref o, n);
                       
                       flag = true;
                                  
                   }
                     catch(ArgumentException e)
                   {
                       if(e.Message.IndexOf(`zero length`) > -1)
                      {
                           flag = false;
                       }  
                    }
                      finally
                    {
                          if(flag == true)
                      {
                           Log.Qizmt_Log(`Error : Test of the non-static Member 'ReplaceM()' with input data of type: mstring_string_negative has  failed`);
                       }  
                       
                    }
                    
                    
                      try
                   {
                      
                    string ms = parts[2].ToString();
                    string oldStr = ms.Substring(0,2);
                    mstring newStr = mstring.Prepare(ms.Substring(10,2));
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[2].ReplaceM(oldStr,ref newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'ReplaceM()' with input data of type: (string_msring) failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                   
                     try
                   {
                      
                    string ms = parts[1].ToString();
                    string oldStr = ms.Substring(0,2);
                    mstring newStr = mstring.Prepare(ms.Substring(10,2));
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[1].ReplaceM(oldStr,ref newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'ReplaceM()' with input data of type: (string_mstring)_unicode failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                    try
                   {
                       mstring s = mstring.Prepare(`x`);
                       string o = null;
                       mstring n = mstring.Prepare(`p`);
                    
                       s.ReplaceM(o, ref n);
                       flag = true;
                                  
                   }
                     catch(ArgumentException e)
                   {
                       if(e.Message.IndexOf(`zero length`) > -1)
                      {
                           flag = false;
                       }  
                    }
                     finally
                    {
                          if(flag == true)
                      {
                           Log.Qizmt_Log(`Error : Test of the non-static Member 'ReplaceM()' with input data of type: string_mstring_negative has  failed`);
                       }  
                     
                    }
                    
                    
                   
                       try
                   {
                      
                      string ms = parts[2].ToString();
                    string oldStr = ms.Substring(0,2);
                    string newStr = ms.Substring(10,2);
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[2].ReplaceM(oldStr,newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'ReplaceM()' with input data of type: (string_string) failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                   
                     try
                   {
                      
                    string ms = parts[1].ToString();
                    string oldStr = ms.Substring(0,2);
                    string newStr = ms.Substring(10,2);
                    string soldStr = ms.Substring(0,2);
                    string snewStr = ms.Substring(10,2);
                    mstring val = mstring.Prepare();
                    
                  
                    val = parts[1].ReplaceM(oldStr,newStr);
                    string ms1 = ms.Replace(soldStr,snewStr);
                  
                    mstring val1 = mstring.Prepare(ms1);
                         
            
                     if ( val != val1) 
                           throw new Exception(`Test of the non-static Member 'ReplaceM()' with input data of type: (string_string)_unicode failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                    try
                   {
                       mstring s = mstring.Prepare(`x`);
                       string o = null;
                       string n = `p`;
                    
                       s.ReplaceM(o, n);
                       
                       flag = true;
                                  
                   }
                     catch(ArgumentException e)
                   {
                       if(!(e.Message.IndexOf(`zero length`) > -1))
                      {
                           flag = false ;
                       }  
                    }
                     finally
                    {
                          if(flag == true)
                      {
                           Log.Qizmt_Log(`Error : Test of the non-static Member 'ReplaceM()' with input data of type: string_string_negative has  failed`);
                       }  
                       
                    }
                
                 return result;
                
            }
            
            public static bool mstring_ResetGetPosition_test(ByteSlice line)
            {
                 mstring sLine = mstring.Prepare(line);
                 mstringarray parts = sLine.SplitM(',');
                 bool result = true;
                 
               UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                      
                     try
                   {
                      
                    
                    double d = sLine.NextItemToDouble(',');
                    mstring s = sLine.NextItemToString(',');
                    mstring st = sLine.NextItemToString(',');
                    ushort sh = sLine.NextItemToUShort(',');
                    
                    sLine.ResetGetPosition();
                    
                    double d1 = sLine.NextItemToDouble(',');
                    mstring s1 = sLine.NextItemToString(',');
                    mstring st1 = sLine.NextItemToString(',');
                    ushort sh1 = sLine.NextItemToUShort(',');
                            
            
                     if ( d != d1 || s != s1 || st != st1 || sh != sh1) 
                           throw new Exception(`Test of the non-static Member 'ResetGetPosition()'  failed`);
                                  
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                 
                
                return result;
            }
            
            public static bool mstring_SplitM_test(ByteSlice line)
            {
                
                   mstring sLine = mstring.Prepare(line); // there is 10 parts in the input string
                   mstringarray parts = sLine.SplitM(',');
                   bool result = true;
                                           
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                     try
                   {
                   
                     if (parts.Length!=10)
                           throw new Exception(`Test of the non-static Member 'SplitM()' with input data of type: char failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                      try
                   {
           
                    string unicode = parts[1].ToString();
                    
                    char c = unicode[3];
                    
                    mstringarray par = parts[1].SplitM(c);
                    string [] pars = unicode.Split(c);
                    
                    
                     if (par.Length != pars.Length )
                           throw new Exception(`Test of the non-static Member 'SplitM()' with input data of type: char_unicode failed`);
                   }
                    catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
         
                   return result; 
                
            }
            
            public static bool mstring_StartsWith_test(ByteSlice line)
            {
                
                mstring sLine = mstring.Prepare(line);
                mstringarray parts = sLine.SplitM(',');
                bool result = true;
                
                UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
               try
                   {
                   
                       string s = parts[2].ToString();
                       mstring c = mstring.Prepare(s.Substring(0,10));
                 
                     if (!parts[2].StartsWith(c))
                           throw new Exception(`Test of the non-static Member 'StartsWith()' with input data of type: mstring failed`);
                   }
              catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                      try
                   {
                   
                       string s = parts[1].ToString();
                       mstring c = mstring.Prepare(s.Substring(0,10));
                 
                     if (!parts[1].StartsWith(c))
                           throw new Exception(`Test of the non-static Member 'StartsWith()' with input data of type: mstring_unicode failed`);
                   }
              catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                     try
                   {
                   
                       string s = parts[2].ToString();
                       string c = s.Substring(0,10);
                 
                     if (!parts[2].StartsWith(c))
                           throw new Exception(`Test of the non-static Member 'StartsWith()' with input data of type: string failed`);
                   }
              catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                   
                      try
                   {
                   
                       string s = parts[1].ToString();
                       string c = s.Substring(0,10);
                 
                     if (!parts[1].StartsWith(c))
                           throw new Exception(`Test of the non-static Member 'StartsWith()' with input data of type: string_unicode failed`);
                   }
              catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                   }
                
           
                return result;
            }
            
            public static bool mstring_SubstringM_test(ByteSlice line)
            {
                
                   mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   bool flag = true;
                   
                   
                      UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                      try
                   {
                       string s = parts[2].ToString();
                       string s1 = s.Substring(10);
                       mstring m1 = parts[2].SubstringM(10);
                       mstring m2 = mstring.Prepare(s1);
                     
                     if (m2 != m1)
                           throw new Exception(`Test of the non-static Member 'SubstringM()' with input data of type: int failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                      try
                   {
                       
                       string s1 = parts[2].ToString();
                       string s2 = s1.Substring(10);
                       mstring m1 = parts[2].SubstringM(10);
                       mstring m2 = mstring.Prepare(s2);
                     
                     if (m2 != m1)
                           throw new Exception(`Test of the non-static Member 'SubstringM()' with input data of type: int_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                  try
                   {
                       string s = parts[2].ToString();
                       string s1 = s.Substring(10,5);
                       mstring m1 = parts[2].SubstringM(10,5);
                       mstring m2 = mstring.Prepare(s1);
                     
                     if (m2 != m1)
                           throw new Exception(`Test of the non-static Member 'SubstringM()' with input data of type: int_int failed`);
                   
                   }
                 catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                      try
                   {
                       
                       string s1 = parts[1].ToString();
                       string s2 = s1.Substring(1,3);
                       mstring m1 = parts[1].SubstringM(1,3);
                       mstring m2 = mstring.Prepare(s2);
                     
                     if (m2 != m1)
                           throw new Exception(`Test of the non-static Member 'SubstringM()' with input data of type: int_int_unicode failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
           
                  return result; 
          
            }
            
            public static bool mstring_ToDataType_test(ByteSlice line)
            {
                
                mstring sLine = mstring.Prepare(line);
                mstringarray parts = sLine.SplitM(',');
                mstring val = mstring.Prepare();
                bool result = true;
                bool flag = true;
               
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();

                 
                  try
                   {
                      
                       ByteSlice c = sLine.ToByteSlice();
                       mstring c1 = mstring.Prepare(c);
                    
                     
                     if (sLine != c1)
                           throw new Exception(`Test of the non-static Member 'ToByteSlice()' with input data of type: default failed`);
                           
                    
                   }
                 catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                 try
                   {
                       
                       ByteSlice c = parts[2].ToByteSlice(100);
                       mstring c1 = mstring.Prepare(c);
                     
                     if (c1 != parts[2])
                           throw new Exception(`Test of the non-static Member 'ToByteSlice()' with input data of type: int failed`);
                           
                    
                   }
                 catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
         /*          
                 try
                   {
                       
                       double d = parts[0].ToDouble();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(d);
                     
                     if (c != parts[0])
                           throw new Exception(`Test of the non-static Member 'ToDouble()' with input data of type: double failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
            
                   */
                      try
                   {
                       
                       string s = parts[2].ToString();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(s);
                     
                     if (c != parts[2])
                           throw new Exception(`Test of the non-static Member 'ToString()' with input data of type: string failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
                       try
                   {
                       
                       string s = parts[1].ToString();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(s);
                     
                     if (c != parts[1])
                           throw new Exception(`Test of the non-static Member 'ToString()' with input data of type: string_unicode failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
        
                   
                  try
                   {
                       
                       int  i  = parts[7].ToInt();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(i);
                     
                     if (c != parts[7])
                           throw new Exception(`Test of the non-static Member 'ToInt()' with input data of type: int failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
                   
                     try
                   {
                       
                       uint  i  = parts[8].ToUInt();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(i);
                     
                     if (c != parts[8])
                           throw new Exception(`Test of the non-static Member 'ToUInt()' with input data of type: uint failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
                   try
                   {
                       
                       UInt16  i  = parts[3].ToUInt16();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(i);
                     
                     if (c != parts[3])
                           throw new Exception(`Test of the non-static Member 'ToUInt16()' with input data of type: Uint16 failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
                   try
                   {
                       
                       UInt32  i  = parts[8].ToUInt32();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(i);
                     
                     if (c != parts[8])
                           throw new Exception(`Test of the non-static Member 'ToUInt32()' with input data of type: Uint32 failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
                   
                  try
                   {
                       
                       UInt64  i  = parts[6].ToUInt64();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(i);
                     
                     if (c != parts[6])
                           throw new Exception(`Test of the non-static Member 'ToUInt64()' with input data of type: Uint64 failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
                   
                  try
                   {
                       
                       ulong  i  = parts[6].ToULong();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(i);
                     
                     if (c != parts[6])
                           throw new Exception(`Test of the non-static Member 'ToULong()' with input data of type: ULong failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
                   
                   
                      
                  try
                   {
                       
                       Int16  i  = parts[4].ToInt16();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(i);
                     
                     if (c != parts[4])
                           throw new Exception(`Test of the non-static Member 'ToInt16()' with input data of type: Int16 failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
                    try
                   {
                       
                       short  i  = parts[4].ToInt16();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(i);
                     
                     if (c != parts[4])
                           throw new Exception(`Test of the non-static Member 'ToShort()' with input data of type: short failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
                   
                   
                  try
                   {
                       
                       Int32  i  = parts[7].ToInt32();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(i);
                     
                     if (c != parts[7])
                           throw new Exception(`Test of the non-static Member 'ToInt32()' with input data of type: Int32 failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                   
                        
                  try
                   {
                       
                       Int64 i  = parts[5].ToInt64();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(i);
                     
                     if (c != parts[5])
                           throw new Exception(`Test of the non-static Member 'ToInt64()' with input data of type: Int64 failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                 try
                   {
                       
                       long l  = parts[5].ToLong();
                       mstring c = mstring.Prepare();
                       c = c.AppendM(l);
                     
                     if (c != parts[5])
                           throw new Exception(`Test of the non-static Member 'ToLong()' with input data of type: Long failed`);
                           
                    
                   }
                  catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   } 
                 
               
                return result;
                
            }
            
            public static bool mstring_ToLowerM_test(ByteSlice line)
            {
                
                 mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   
                     UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                  
                   try
                   {
                       string s = parts[2].ToString();
                       string s1 = s.ToLower();
                       mstring sLine2 = parts[2].ToLowerM();
                       mstring sLine1 = mstring.Prepare(s1);
                     
                     if (sLine1 != sLine2)
                           throw new Exception(`Test of the non-static Member 'ToLowerM()'  failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                   try
                   {
                    
                       string s1 = parts[1].ToString();
                       s1 = s1.ToLower();
                       mstring m1 = parts[1].ToLowerM();
                       mstring m2 = mstring.Prepare(s1);
                     
                     if (m1 != m2)
                           throw new Exception(`Test of the non-static Member 'ToLowerM()' _ unicode  failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                 
                                
                 return result;
                
            }
      
            
            public static bool mstring_ToUpperM_test(ByteSlice line)
            {
                
                  mstring sLine = mstring.Prepare(line);
                   mstringarray parts = sLine.SplitM(',');
                   mstring val = mstring.Prepare();
                   bool result = true;
                   
                     UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                  
                   try
                   {
                       string s = parts[2].ToString();
                       string s1 = s.ToUpper();
                       mstring sLine2 = parts[2].ToUpperM();
                       mstring sLine1 = mstring.Prepare(s1);
                     
                     if (sLine1 != sLine2)
                           throw new Exception(`Test of the non-static Member 'ToUpperM()'  failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                   
                   
                   try
                   {
                    
                       string s1 = parts[1].ToString();
                       s1 = s1.ToUpper();
                       mstring m1 = parts[1].ToUpperM();
                       mstring m2 = mstring.Prepare(s1);
                     
                     if (m1 != m2)
                           throw new Exception(`Test of the non-static Member 'ToUpperM()' _ unicode  failed`);
                           
                    
                   }
                     catch(Exception e) 
                   {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
                   }
                 
                                
                 return result;
                
            
            }
       
            
           // RECORDSET   TESTS   
             
             
            public static bool recordset_PutInt_test(ByteSlice line)
            {
                
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               int i = int.Parse(parts[7].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutInt(i);
                val.PutInt(i);
                
                //testing get
                int i2 = val.GetInt();
                int i3 = val.GetInt();
                
                     
                     if (i2 != i3)
                           throw new Exception(`Test of the non-static Member 'PutInt()_GetInt()' have failed`);
                           
                    
                }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
            }
       
            
            
            public static bool recordset_PutInt16_test(ByteSlice line)
            {
                
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               Int16 i = Int16.Parse(parts[4].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutInt16(i);
                val.PutInt16(i);
                
                //testing get
                Int16 i2 = val.GetInt16();
                Int16 i3= val.GetInt16();
                
                     
                     if (i2 != i3)
                           throw new Exception(`Test of the non-static Member 'PutInt16()_GetInt16()' have failed`);
                           
                    
                }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
            }
            
          public static bool recordset_PutInt32_test(ByteSlice line)
            {
                
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               Int32 i = Int32.Parse(parts[7].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutInt32(i);
                val.PutInt32(i);
                
                //testing get
                Int32 i2 = val.GetInt32();
                Int32 i3= val.GetInt32();
                
                     
                     if (i2 != i3)
                           throw new Exception(`Test of the non-static Member 'PutInt32()_GetInt32()' have failed`);
                           
                    
                }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
            }
            
              public static bool recordset_PutInt64_test(ByteSlice line)
            {
                
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               Int64 i = Int64.Parse(parts[5].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutInt64(i);
                val.PutInt64(i);
                
                //testing get
                Int64 i2 = val.GetInt64();
                Int64 i3= val.GetInt64();
                
                     
                     if (i2 != i3)
                           throw new Exception(`Test of the non-static Member 'PutInt64()_GetInt64()' have failed`);
                           
                    
             }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
            }
            
               public static bool recordset_PutLong_test(ByteSlice line)
            {
              
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               long l = long.Parse(parts[5].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutLong(l);
                val.PutLong(l);
                
                //testing get
                long l2 = val.GetLong();
                long l3= val.GetLong();
                
                     
                     if (l2 != l3)
                           throw new Exception(`Test of the non-static Member 'PutLong()_GetLong()' have failed`);
                           
                    
             }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
                
  
            }
         
            
           public static bool recordset_PutShort_test(ByteSlice line)
           {
               
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               short sh = short.Parse(parts[4].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutShort(sh);
                val.PutShort(sh);
                
                //testing get
                short sh2 = val.GetShort();
                short sh3= val.GetShort();
                
                     
                     if (sh2 != sh3)
                           throw new Exception(`Test of the non-static Member 'PutShort()_GetShort()' have failed`);
                           
                    
             }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
                
               
           }
           
           public static bool recordset_PutString_test(ByteSlice line)
           {
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
            try
             {
    
               mstring m1 = mstring.Prepare();
               mstring m2 = mstring.Prepare();
               mstring m3 = mstring.Prepare();
             
               
               m1 = parts[2].MSubstring(0,10);
               m2 = parts[2].MSubstring(10,10);
               m3 = parts[2].MSubstring(20,10);
            
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutString(m1);
                val.PutString(m2);
                val.PutString(m3);
                
               mstring ms1 = mstring.Prepare();
               mstring ms2 = mstring.Prepare();
               mstring ms3 = mstring.Prepare();
                
                //testing get
                ms1 = val.GetString();
                ms2 = val.GetString();
                ms3 = val.GetString();
                
                     
                     if (m1 != ms1 || m2 != ms2 || m3 != ms3)
                           throw new Exception(`Test of the non-static Member 'PutString()_GetString()' with input data : mstring have failed`);
                           
                    
             }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
              
            try
             {
    
               mstring m1 = mstring.Prepare();
               mstring m2 = mstring.Prepare();
               mstring m3 = mstring.Prepare();
             
               
               m1 = parts[1].MSubstring(0,10);
               m2 = parts[1].MSubstring(10,10);
               m3 = parts[1].MSubstring(20,10);
            
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutString(m1);
                val.PutString(m2);
                val.PutString(m3);
                
               mstring ms1 = mstring.Prepare();
               mstring ms2 = mstring.Prepare();
               mstring ms3 = mstring.Prepare();
                
                //testing get
                ms1 = val.GetString();
                ms2 = val.GetString();
                ms3 = val.GetString();
                
                     
                     if (m1 != ms1 || m2 != ms2 || m3 != ms3)
                           throw new Exception(`Test of the non-static Member 'PutString()_GetString()' with input data : mstring_unicode have failed`);
                           
                    
             }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
              
              
             try
             {
    
               
             
               string s = parts[2].ToString();
               
               string s1 = s.Substring(0,10);
               string s2 = s.Substring(10,10);
               string s3 = s.Substring(20,10);
               
               mstring m1 = mstring.Prepare(s1);
               mstring m2 = mstring.Prepare(s2);
               mstring m3 = mstring.Prepare(s3);
                         
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutString(s1);
                val.PutString(s2);
                val.PutString(s3);
                
               mstring ms1 = mstring.Prepare();
               mstring ms2 = mstring.Prepare();
               mstring ms3 = mstring.Prepare();
                
                //testing get
                ms1 = val.GetString();
                ms2 = val.GetString();
                ms3 = val.GetString();
                
                     
                     if (m1 != ms1 || m2 != ms2 || m3 != ms3)
                           throw new Exception(`Test of the non-static Member 'PutString()_GetString()' with input data : string have failed`);
                           
                    
             }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
              
              
                 
             try
             {
    
               
             
               string s = parts[1].ToString();
               
               string s1 = s.Substring(0,10);
               string s2 = s.Substring(10,10);
               string s3 = s.Substring(20,10);
               
               mstring m1 = mstring.Prepare(s1);
               mstring m2 = mstring.Prepare(s2);
               mstring m3 = mstring.Prepare(s3);
                         
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutString(s1);
                val.PutString(s2);
                val.PutString(s3);
                
               mstring ms1 = mstring.Prepare();
               mstring ms2 = mstring.Prepare();
               mstring ms3 = mstring.Prepare();
                
                //testing get
                ms1 = val.GetString();
                ms2 = val.GetString();
                ms3 = val.GetString();
                
                     
                     if (m1 != ms1 || m2 != ms2 || m3 != ms3)
                           throw new Exception(`Test of the non-static Member 'PutString()_GetString()' with input data : string_unicode have failed`);
                           
                    
             }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
              
               
              return result;
               
           }
           
             public static bool recordset_PutUInt_test(ByteSlice line)
            {
                
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               uint ui = uint.Parse(parts[8].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutUInt(ui);
                val.PutUInt(ui);
                
                //testing get
                uint ui2 = val.GetUInt();
                uint ui3 = val.GetUInt();
                
                     
                     if (ui2 != ui3)
                           throw new Exception(`Test of the non-static Member 'PutUInt()_GetUInt()' have failed`);
                           
                    
                }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
            }
            
             public static bool recordset_PutUInt16_test(ByteSlice line)
            {
                
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               UInt16 ui = UInt16.Parse(parts[3].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutUInt16(ui);
                val.PutUInt16(ui);
                
                //testing get
                UInt16 ui2 = val.GetUInt16();
                UInt16 ui3 = val.GetUInt16();
                
                     
                     if (ui2 != ui3)
                           throw new Exception(`Test of the non-static Member 'PutUInt16()_GetUInt16()' have failed`);
                           
                    
                }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
            }
            
           public static bool recordset_PutUInt32_test(ByteSlice line)
            {
                
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               UInt32 ui = UInt32.Parse(parts[8].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutUInt32(ui);
                val.PutUInt32(ui);
                
                //testing get
                UInt32 ui2 = val.GetUInt32();
                UInt32 ui3 = val.GetUInt32();
                
                     
                     if (ui2 != ui3)
                           throw new Exception(`Test of the non-static Member 'PutUInt32()_GetUInt32()' have failed`);
                           
                    
                }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
            }
            
             public static bool recordset_PutUInt64_test(ByteSlice line)
            {
                
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               UInt64 ui = UInt64.Parse(parts[6].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutUInt64(ui);
                val.PutUInt64(ui);
                
                //testing get
                UInt64 ui2 = val.GetUInt64();
                UInt64 ui3 = val.GetUInt64();
                
                     
                     if (ui2 != ui3)
                           throw new Exception(`Test of the non-static Member 'PutUInt64()_GetUInt64()' have failed`);
                           
                    
                }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
            }
            
              public static bool recordset_PutULong_test(ByteSlice line)
            {
                
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               ulong ui = ulong.Parse(parts[6].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutULong(ui);
                val.PutULong(ui);
                
                //testing get
                ulong ui2 = val.GetULong();
                ulong ui3 = val.GetULong();
                
                     
                     if (ui2 != ui3)
                           throw new Exception(`Test of the non-static Member 'PutULong()_GetULong()' have failed`);
                           
                    
                }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
            }
            
             public static bool recordset_PutUShort_test(ByteSlice line)
            {
                
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               ushort ui = ushort.Parse(parts[3].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutUShort(ui);
                val.PutUShort(ui);
                
                //testing get
                ushort ui2 = val.GetUShort();
                ushort ui3 = val.GetUShort();
                
                     
                     if (ui2 != ui3)
                           throw new Exception(`Test of the non-static Member 'PutUShort()_GetUShort()' have failed`);
                           
                    
                }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
            }
            
           public static bool recordset_PutDouble_test(ByteSlice line)
           {
               
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
   
                 UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
             try
             {
    
               double d = double.Parse(parts[0].ToString());
               
               
                //testing put
                recordset val = recordset.Prepare();
                val.PutDouble(d);
                val.PutDouble(d);
                
                //testing get
                double d2 = val.GetDouble();
                double d3 = val.GetDouble();
                
                     
                     if ( d2 != d3)
                           throw new Exception(`Test of the non-static Member 'PutDouble()_GetDouble()' have failed`);
                           
                    
                }
             catch(Exception e) 
             {
                       Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                     
              }
                
                
                return result;
               
               
               
           }
           
           public static bool recordset_PutDateTime_test(ByteSlice line)
           {
                mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
               
                   UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                   
                  try
             {
                string s = parts[9].ToString();   
                long temp = long.Parse(s);
                DateTime dt = DateTime.FromBinary(temp);
                recordset val = recordset.Prepare();
                
                //testing put
                val.PutDateTime(dt);
                val.PutDateTime(dt);
                
                
                //testing get
                DateTime dt1 = val.GetDateTime();
                DateTime dt2 = val.GetDateTime();
                
                if ( dt1 != dt2)
                           throw new Exception(`Test of the non-static Member 'PutDateTime()_GetDateTime()' have failed`);
             }
             catch(Exception e)
             {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                 
             }
                 return result;
           }
           
           public static bool recordset_PutChar_test(ByteSlice line)
           {
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
               
                     
            UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
            
             try
             {
                string s = parts[2].ToString();  
                string s1;
                StringBuilder sb = new StringBuilder();
                recordset val = recordset.Prepare();
                int length = s.Length;
                
                for(int i = 0; i<length; i++)
                    {
                       //testing put
                       val.PutChar(s[i]);
                    }
                
                 for(int i = 0; i<length; i++)
                    {
                       //testing get
                      sb.Append(val.GetChar());
                    }
             
         
                 s1 = sb.ToString();
                if ( s != s1)
                           throw new Exception(`Test of the non-static Member 'PutChar()_GetChar()' have failed`);
             }
             catch(Exception e)
             {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                 
             }
             
                    
            try
             {
                string s = parts[1].ToString();  
                string s1;
                StringBuilder sb = new StringBuilder();
                recordset val = recordset.Prepare();
                int length = s.Length;
                
                for(int i = 0; i<length; i++)
                    {
                       //testing put
                       val.PutChar(s[i]);
                    }
                
                 for(int i = 0; i<length; i++)
                    {
                       //testing get
                      sb.Append(val.GetChar());
                    }
             
         
                 s1 = sb.ToString();
                if ( s != s1)
                           throw new Exception(`Test of the non-static Member 'PutChar()_GetChar()'_unicode have failed`);
             }
             catch(Exception e)
             {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                 
             }
         
                return result;
           }
           
          public static bool recordset_PutBytes_test(ByteSlice line)
          {
               mstring sLine = mstring.Prepare(line);
               mstringarray parts = sLine.SplitM(',');
               bool result = true;
               
             UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
           
            try
             {
                string s = parts[2].ToString();   
                recordset val = recordset.Prepare();
                
                byte [] buf = Encoding.UTF8.GetBytes(s);
                
                //testing put
                val.PutBytes(buf,0,50);
                val.PutBytes(buf,50,50);
                
                
                //testing get
                byte[] buf1 = new byte[50];
                byte[] buf2 = new byte[50];
                val.GetBytes(buf1,0,50);
                val.GetBytes(buf2,0,50);
                
                for(int i = 0 ; i < 50;i++)
                         if ( buf1[i] != buf[i])
                               throw new Exception(`Test of the non-static Member 'PutBytes()_GetBytes()' have failed`);
                               
               for(int i = 50 ; i < 100;i++)
                         if ( buf2[i-50] != buf[i])
                               throw new Exception(`Test of the non-static Member 'PutBytes()_GetBytes()' have failed`);
             }
             catch(Exception e)
             {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                 
             }
                
         return result;
          }          
          
        public static bool recordset_PutBool_test(ByteSlice line)
        {
             mstring sLine = mstring.Prepare(line);
             mstringarray parts = sLine.SplitM(',');
             bool result = true;
            
             UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
             
             try
             {
                string s = parts[2].ToString();   
                recordset val = recordset.Prepare();
                bool test;
                bool testResult;
               
                
                for(int i = 0 ; i < 2000;i++)
                    { 
                        test = (i % 2 == 0);
                        
                        val.PutBool(test);
                        
                        testResult = val.GetBool();
                        
                        if (test != testResult)
                               throw new Exception(`Test of the non-static Member 'PutBool()_GetBool()' have failed`);
                    }         
           
             }
             catch(Exception e)
             {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                 
             }
            
             return result;
        }
        
        public static bool recordset_PrepareRow_test(ByteSlice line)
        {
             mstring sLine = mstring.Prepare(line);
             mstringarray parts = sLine.SplitM(',');
             bool result = true;
            
             UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
             
             try
             {
                double d = double.Parse(parts[0].ToString());
                string u = parts[1].ToString();
                string s = parts[2].ToString();
                ushort ush = ushort.Parse(parts[3].ToString());
                short  sh = short.Parse(parts[4].ToString());
                long l = long.Parse(parts[5].ToString());
                ulong ul = ulong.Parse(parts[6].ToString());
                int ii = int.Parse(parts[7].ToString());
                uint uii = uint.Parse(parts[8].ToString());
                long temp = long.Parse(parts[9].ToString());
                DateTime dt = DateTime.FromBinary(temp);
                
                recordset rec = recordset.Prepare();
              
                rec.PutDouble(d);
                rec.PutString(u);
                rec.PutString(s);
                rec.PutUShort(ush);
                rec.PutShort(sh);
                rec.PutLong(l);
                rec.PutULong(ul);
                rec.PutInt(ii);
                rec.PutUInt(uii);
                rec.PutDateTime(dt);
                
                ByteSlice test;
                test = rec.ToByteSlice();
                
                recordset val = recordset.PrepareRow(test);
                mstring m1 = mstring.Prepare();
                mstring m2 = mstring.Prepare();
                
                double d1 = val.GetDouble();
                m1 = val.GetString();
                string u1 = m1.ToString();
                m2 = val.GetString();
                string s1 = m2.ToString();
                ushort ush1 = val.GetUShort();
                short  sh1 = val.GetShort();
                long l1 = val.GetLong();
                ulong ul1 = val.GetULong();
                int ii1 = val.GetInt();
                uint uii1 = val.GetUInt();
                DateTime dt1 = val.GetDateTime();
            
               
                
                if( d != d1 || u != u1 || s!= s1 || ush != ush1 || sh != sh1 || l != l1 || ul != ul1 || ii != ii1 || uii != uii1|| d != d1)
             
                               throw new Exception(`Test of the static Member 'PrepareRow' have failed`);
               
           
             }
             catch(Exception e)
             {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                 
             }
            
             return result;
        }
        
        
        public static bool recordset_ContainsString_test(ByteSlice line)
        {
             mstring sLine = mstring.Prepare(line);
             mstringarray parts = sLine.SplitM(',');
             bool result = true;
             
             UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
             
            try
             {
               
                recordset res = recordset.Prepare();
                res.PutInt(15);
                res.PutString(parts[2]);
                res.PutDouble(0.00000005);
                        
                        if (!res.ContainsString)
                               throw new Exception(`Test of the non-static property 'ContainsString' have failed`);
                    
           
             }
             catch(Exception e)
             {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                       result = false;
                 
             }
            
            
             return result;
            
        }
        
        // BYTESLICE TESTS
        
        
        public static bool ByteSlice_AppendTo_test(ByteSlice line)
        {
            string sLine = line.ToString();
            bool result = true;
        
          UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
            
          try
          {
                                 
             List<byte> buf = new List<byte>();
             line.AppendTo(buf);
              
            string s = System.Text.Encoding.UTF8.GetString(buf.ToArray());
        
            if (sLine != s)
                        throw new Exception(`Test of the non-static property 'AppendTo()' have failed`);
                    
           
             }
             catch(Exception e)
             {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
            
            
             return result;            
        }
        
        public static bool ByteSlice_CopyTo_test(ByteSlice line)
        {
            
            mstring sLine = mstring.Prepare(line);
            mstringarray parts = sLine.SplitM(',');
            string s = parts[2].ToString();
            ByteSlice b = parts[2].ToByteSlice();
            
            bool result = true;
            
             UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
            
          try
          {
                                 
             byte[] buf = new byte[100];
             b.CopyTo(buf);
              
            string s1 = System.Text.Encoding.UTF8.GetString(buf);
           
                       
            if (s1 != s)
                        throw new Exception(`Test of the non-static property 'CopyTo()' have failed`);
                    
           
             }
             catch(Exception e)
             {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
            
            
             return result;            
            
            
            
        }
        
        public static bool ByteSlice_CopyToInt_test(ByteSlice line)
        {
            
             mstring sLine = mstring.Prepare(line);
            mstringarray parts = sLine.SplitM(',');
            string s = parts[2].ToString();
            ByteSlice b = parts[2].ToByteSlice();
            
            bool result = true;
            
             UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
            
          try
          {
                                 
             byte[] buf = new byte[105];
             b.CopyTo(buf,5);
              
            string s1 = System.Text.Encoding.UTF8.GetString(buf,5,100);
           
                       
            if (s1 != s)
                        throw new Exception(`Test of the non-static property 'CopyToInt()' have failed`);
                    
           
             }
             catch(Exception e)
             {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
            
            
             return result;            
      
        }
        
        public static bool ByteSlice_Create_test(ByteSlice line)
        {
           
            mstring sLine = mstring.Prepare(line);
            mstringarray parts = sLine.SplitM(',');
            ByteSlice b = parts[2].ToByteSlice();
           
            
            bool result = true;
            
         UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
            
          try
          {
                                 
             ByteSlice b1 = ByteSlice.Create(b,0,b.Length);
             
             mstring m = mstring.Prepare(b1);
              
      
            if ( parts[2] != m )
                        throw new Exception(`Test of the non-static property 'Create()' with input data of type: ByteSlice_int_int have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
              try
          {
              string s = parts[2].ToString();
              byte[] buf = System.Text.Encoding.UTF8.GetBytes(s);
                                 
             ByteSlice b1 = ByteSlice.Create(buf);
             
             string s1 = b1.ToString();
              
      
            if ( s != s1 )
                        throw new Exception(`Test of the non-static property 'Create()' with input data: IList have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
         try
          {
              string s = parts[2].ToString();
              byte[] buf = System.Text.Encoding.UTF8.GetBytes(s);
                                 
             ByteSlice b1 = ByteSlice.Create(buf,0,buf.Length);
             string s1 = b1.ToString();
      
            if ( s!= s1 )
                        throw new Exception(`Test of the non-static property 'Create()' with input data: IList_int_int have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
            
             
                   
         try
          {
              string s = parts[2].ToString();
             
                                 
             ByteSlice b1 = ByteSlice.Create(s);
              
             mstring m = mstring.Prepare(b1);
      
            if ( parts[2] != m )
                        throw new Exception(`Test of the non-static property 'Create()' with input data: string have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
             
         try
          {
              string s = parts[1].ToString();
             
                                 
             ByteSlice b1 = ByteSlice.Create(s);
              
             mstring m = mstring.Prepare(b1);
      
            if ( parts[1] != m )
                        throw new Exception(`Test of the non-static property 'Create()' with input data: string_unicode have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
            
             
         try
          {
              string s = parts[2].ToString();
              StringBuilder sb = new StringBuilder();
              sb.Append(s);
                                 
             ByteSlice b1 = ByteSlice.Create(sb);
              
             mstring m = mstring.Prepare(b1);
      
            if ( parts[2] != m )
                        throw new Exception(`Test of the non-static property 'Create()' with input data: StringBuilder have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
         try
          {
              string s = parts[1].ToString();
              StringBuilder sb = new StringBuilder();
              sb.Append(s);
                                 
             ByteSlice b1 = ByteSlice.Create(sb);
              
             mstring m = mstring.Prepare(b1);
      
            if ( parts[1] != m )
                        throw new Exception(`Test of the non-static property 'Create()' with input data: StringBuilder_unicode have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
            
             
                    
         try
          {
                   
             ByteSlice b1 = ByteSlice.Create();
              
      
            if ( b1.Length != 0 )
                        throw new Exception(`Test of the non-static property 'Create()' with no input data have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
     
             return result;              
            
            
        }
        
        public static bool ByteSlice_Prepare_test(ByteSlice line)
        {
            mstring sLine = mstring.Prepare(line);
            mstringarray parts = sLine.SplitM(',');
            ByteSlice b = parts[2].ToByteSlice();
            bool result = true;
            
           UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
             
          try
          {
                   
             ByteSlice b1 = ByteSlice.Prepare(b,0,b.Length);
              
      
            if ( b1.ToString() != b.ToString() )
                        throw new Exception(`Test of the static property 'Prepare()' with  input data : ByteSlice_int_int  failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
          try
          {
                   
              string s = parts[2].ToString();
              byte[] buf = System.Text.Encoding.UTF8.GetBytes(s);
                                 
              ByteSlice b1 = ByteSlice.Prepare(buf);
             
              string s1 = b1.ToString();
              
      
               if ( s != s1 )
                        throw new Exception(`Test of the static property 'Prepare()' with  input data : IList  failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
           try
           {
                   
              string s = parts[2].ToString();
              byte[] buf = System.Text.Encoding.UTF8.GetBytes(s);
                                 
              ByteSlice b1 = ByteSlice.Prepare(buf,0,buf.Length);
             
              string s1 = b1.ToString();
              
      
               if ( s != s1 )
                       throw new Exception(`Test of the static property 'Prepare()' with  input data : IList_int_int  failed`);
                    
           
           }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
          try
           {
                   
              string s = parts[2].ToString();
            
                                 
              ByteSlice b1 = ByteSlice.Prepare(parts[2]);
             
              string s1 = b1.ToString();
              
      
               if ( s != s1 )
                       throw new Exception(`Test of the static property 'Prepare()' with  input data : mstring  failed`);
                    
           
           }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
         try
           {
                   
              string s = parts[1].ToString();
            
                                 
              ByteSlice b1 = ByteSlice.Prepare(parts[1]);
             
              string s1 = b1.ToString();
              
      
               if ( s != s1 )
                       throw new Exception(`Test of the static property 'Prepare()' with  input data : mstring_unicode  failed`);
                    
           
           }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
           try
           {
                   
              string s = parts[2].ToString();
            
                                 
              ByteSlice b1 = ByteSlice.Prepare(parts[2].ToString());
             
              string s1 = b1.ToString();
              
      
               if ( s != s1 )
                       throw new Exception(`Test of the static property 'Prepare()' with  input data : string  failed`);
                    
           
           }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
         try
           {
                   
              string s = parts[1].ToString();
            
                                 
              ByteSlice b1 = ByteSlice.Prepare(parts[1].ToString());
             
              string s1 = b1.ToString();
              
      
               if ( s != s1 )
                       throw new Exception(`Test of the static property 'Prepare()' with  input data : string_unicode  failed`);
                    
           
           }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
                     
         try
          {
              string s = parts[2].ToString();
              StringBuilder sb = new StringBuilder();
              sb.Append(s);
                                 
             ByteSlice b1 = ByteSlice.Prepare(sb);
              
             mstring m = mstring.Prepare(b1);
      
            if ( parts[2] != m )
                        throw new Exception(`Test of the static property 'Prepare()' with input data: StringBuilder have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
         try
          {
              string s = parts[1].ToString();
              StringBuilder sb = new StringBuilder();
              sb.Append(s);
                                 
             ByteSlice b1 = ByteSlice.Prepare(sb);
              
             mstring m = mstring.Prepare(b1);
      
            if ( parts[1] != m )
                        throw new Exception(`Test of the static property 'Prepare()' with input data: StringBuilder_unicode have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
            
             
                    
         try
          {
                   
             ByteSlice b1 = ByteSlice.Prepare();
              
      
            if ( b1.Length != 0 )
                        throw new Exception(`Test of the static property 'Prepare()' with no input data have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
             
         try
          {
              string s = parts[2].ToString();
              
           
             ByteSlice b1 = ByteSlice.PreparePaddedMString(parts[2],150);
              
             string s1 = UnpadKey(b1).ToString();
      
            if ( s != s1 )
                        throw new Exception(`Test of the static property 'PreparePaddedMString()' with input data: mstring have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
         try
          {  string s = parts[1].ToString();
         
             ByteSlice b1 = ByteSlice.PreparePaddedMString(parts[1],500);
              
             string s1 = UnpadKey(b1).ToString();
             
          
      
            if ( s !=  s1 )
                        throw new Exception(`Test of the static property 'PreparePaddedMString()' with input data: mstring_unicode have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
            
             
         try
          {
              string s = parts[2].ToString();
                        
             ByteSlice b1 = ByteSlice.PreparePaddedStringAscii(s,150);
              
             string s1 = UnpadKey(b1).ToString();
      
            if ( s != s1 )
                        throw new Exception(`Test of the static property 'PreparePaddedStringAscii()' with input data: mstring have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
         try
          {  string s = parts[2].ToString();
         
             ByteSlice b1 = ByteSlice.PreparePaddedStringUTF8(s,150);
              
             string s1 = UnpadKey(b1).ToString();
             
          
      
            if ( s !=  s1 )
                        throw new Exception(`Test of the static property 'PreparePaddedStringUTF8()' with input data: string have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
            
             
            try
            {
              string s = parts[1].ToString();
                        
             ByteSlice b1 = ByteSlice.PreparePaddedStringUTF8(s,500);
              
             string s1 = UnpadKey(b1).ToString();
      
            if ( s != s1 )
                        throw new Exception(`Test of the static property 'PreparePaddedStringUTF8()' with input data: string_unicode have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
           try
            {
              string s = parts[2].ToString();
                        
               byte[] buf = System.Text.Encoding.UTF8.GetBytes(s);
               byte[] buf2 = ByteSlice.PreparePaddedUTF8Bytes(buf, 0, buf.Length, 150);
               ByteSlice b1 = ByteSlice.Prepare(buf2);
               
                          
             string s1 = UnpadKey(b1).ToString();
      
            if ( s != s1 )
                        throw new Exception(`Test of the static property 'PreparePaddedStringUTF8Bytes()' with input data: string have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
           try
            {
              string s = parts[1].ToString();
                        
               byte[] buf = System.Text.Encoding.UTF8.GetBytes(s);
               byte[] buf2 = ByteSlice.PreparePaddedUTF8Bytes(buf, 0, buf.Length, 500);
               ByteSlice b1 = ByteSlice.Prepare(buf2);
              
             string s1 = UnpadKey(b1).ToString();
      
            if ( s != s1 )
                        throw new Exception(`Test of the static property 'PreparePaddedStringUTF8Bytes()' with input data: string_unicode have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
               try
            {
           
               byte[] buf = b.ToBytes();
               string s = Encoding.UTF8.GetString(buf);
           
               string s1 = parts[2].ToString();
                            
            if ( s != s1 )
                        throw new Exception(`Test of the static property 'ToBytes()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
     
             
            try
            {
           
               
               string s = parts[2].ToString();
               string s1 = b.ToString();
               
                              
                 if ( s != s1 )
                        throw new Exception(`Test of the static property 'ToBytes()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
            try
            {
           
              
               string s = b.ToString();
           
               string s1 = parts[2].ToString();
                            
            if ( s != s1 )
                        throw new Exception(`Test of the static property 'ToString()'   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
     
                try
            {
               ByteSlice b1 = parts[1].ToByteSlice();
           
               string s = b1.ToString();
           
               string s1 = parts[1].ToString();
                            
            if ( s != s1 )
                        throw new Exception(`Test of the static property 'ToString()'_unicode  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
     
             return result;
         }
         
         
         // ENTRY TESTS
         
         public static bool Entry_AsciiToBytes_test(ByteSlice line)
         {
             mstring sLine = mstring.Prepare(line);
             mstringarray parts = sLine.SplitM(',');
             mstring val = mstring.Prepare();
             bool result = true;
             
             UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
             
            try
            { 
               string s = parts[2].ToString();
               byte [] buf = new byte[s.Length];
           
               Entry.AsciiToBytes(s,buf,0);
               ByteSlice b = ByteSlice.Prepare(buf);
               mstring m = mstring.Prepare(b);
                            
            if ( m != parts[2] )
                        throw new Exception(`Test of the static property 'AsciiToBytes()'   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
           try
            { 
               string s = parts[2].ToString();
               List<byte> buf = new List<byte>();
           
               Entry.AsciiToBytesAppend(s,buf);
               ByteSlice b = ByteSlice.Prepare(buf);
               mstring m = mstring.Prepare(b);
                            
            if ( m != parts[2] )
                        throw new Exception(`Test of the static property 'AsciiToBytesAppend()' _string_List  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
                 try
            { 
               string s = parts[2].ToString();
               StringBuilder sb = new StringBuilder();
               sb.Append(s);
               List<byte> buf = new List<byte>();
           
               Entry.AsciiToBytesAppend(sb,buf);
               ByteSlice b = ByteSlice.Prepare(buf);
               mstring m = mstring.Prepare(b);
                            
            if ( m != parts[2] )
                        throw new Exception(`Test of the static property 'AsciiToBytesAppend()'_StringBuilder_List  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
      
             return result; 
         }
         
         public static bool Entry_BytesToAscii_test(ByteSlice line)
         {
             mstring sLine = mstring.Prepare(line);
             mstringarray parts = sLine.SplitM(',');
             mstring val = mstring.Prepare();
             bool result = true;
             
              UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
             
            try
            { 
               string s = parts[2].ToString();
               List<byte> buf = new List<byte>();
               Entry.AsciiToBytesAppend(s,buf);
               s = Entry.BytesToAscii(buf);
               ByteSlice b = ByteSlice.Prepare(buf);
               mstring m = mstring.Prepare(s);
                            
            if ( m != parts[2] )
                        throw new Exception(`Test of the static property 'AsciiToBytes()'_List   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
            try
            { 
               string s = parts[2].ToString();
               List<byte> buf = new List<byte>();
               Entry.AsciiToBytesAppend(s,buf);
               s = Entry.BytesToAscii(buf,0,buf.Count);
               ByteSlice b = ByteSlice.Prepare(buf);
               mstring m = mstring.Prepare(s);
                            
            if (m!= parts[2])
                        throw new Exception(`Test of the static property 'AsciiToBytes()'_List_int_int   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
            try
            { 
               string s = parts[2].ToString();
               StringBuilder sb = new StringBuilder();
               List<byte> buf = new List<byte>();
               Entry.AsciiToBytesAppend(s,buf);
               Entry.BytesToAsciiAppend(buf,sb);
               ByteSlice b = ByteSlice.Prepare(buf);
               mstring m = mstring.Prepare(sb.ToString());
                            
            if (m!= parts[2])
                        throw new Exception(`Test of the static property 'AsciiToBytesAppend()'_List_StringBuilder   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
            try
            { 
               string s = parts[2].ToString();
               StringBuilder sb = new StringBuilder();
               List<byte> buf = new List<byte>();
               Entry.AsciiToBytesAppend(s,buf);
               Entry.BytesToAsciiAppend(buf,sb,0,buf.Count);
               ByteSlice b = ByteSlice.Prepare(buf);
               mstring m = mstring.Prepare(sb.ToString());
                            
            if (m!= parts[2])
                        throw new Exception(`Test of the static property 'AsciiToBytes()'_List_StringBuilder_int_int   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             return result; 
         }
         
         
         
         public static bool Entry_BytesToDataType_test(ByteSlice line)
         {
             mstring sLine = mstring.Prepare(line);
             mstringarray parts = sLine.SplitM(',');
             bool result = true;
             
              UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
             
            try
            { 
               string s = parts[0].ToString();
               double d = double.Parse(s);
               byte[] buf = new byte[9];
               Entry.DoubleToBytes(d,buf,0);
               d = Entry.BytesToDouble(buf);
               double d1;
               Entry.DoubleToBytes(d,buf,0);
               d1 = Entry.BytesToDouble(buf);
             
                            
            if ( d != d1)
                        throw new Exception(`Test of the static property 'BytesToDouble()'_List   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
                
            try
            { 
               string s = parts[0].ToString();
               double d = double.Parse(s);
               byte[] buf = new byte[9];
               Entry.DoubleToBytes(d,buf,0);
               d = Entry.BytesToDouble(buf,0);
               double d1;
               Entry.DoubleToBytes(d,buf,0);
               d1 = Entry.BytesToDouble(buf,0);
             
                            
            if ( d != d1)
                        throw new Exception(`Test of the static property 'BytesToDouble()'_List_int   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
                try
            { 
               string s = parts[7].ToString();
               int i = int.Parse(s);
               byte[] buf = Entry.ToBytes(i);
               i = Entry.BytesToInt(buf);
               int i1;
               Entry.ToBytes(i);
               i1 = Entry.BytesToInt(buf);
               
            if ( i != i1)
                        throw new Exception(`Test of the static property 'BytesToInt()'_List   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
              try
            { 
               string s = parts[7].ToString();
               int i = int.Parse(s);
               byte[] buf = Entry.ToBytes(i);
               i = Entry.BytesToInt(buf,0);
               int i1;
               Entry.ToBytes(i);
               i1 = Entry.BytesToInt(buf,0);
               
            if ( i != i1)
                        throw new Exception(`Test of the static property 'BytesToInt()'_List_int   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
               try
            { 
               string s = parts[4].ToString();
               Int16 i = Int16.Parse(s);
               byte[] buf = Entry.Int16ToBytes(i);
               i = Entry.BytesToInt16(buf);
               Int16 i1;
               Entry.Int16ToBytes(i);
               i1 = Entry.BytesToInt16(buf);
               
            if ( i != i1)
                        throw new Exception(`Test of the static property 'BytesToInt16()'_List   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
              try
            { 
               string s = parts[4].ToString();
               Int16 i = Int16.Parse(s);
               byte[] buf = Entry.Int16ToBytes(i);
               i = Entry.BytesToInt16(buf,0);
               Int16 i1;
               Entry.Int16ToBytes(i);
               i1 = Entry.BytesToInt16(buf,0);
               
            if ( i != i1)
                        throw new Exception(`Test of the static property 'BytesToInt16()'_List_int   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
                try
            { 
               string s = parts[5].ToString();
               long l = long.Parse(s);
               byte[] buf = new byte[8];
               Entry.LongToBytes(l,buf,0);
               l = Entry.BytesToLong(buf);
               long l1;
               Entry.LongToBytes(l,buf,0);
               l1 = Entry.BytesToLong(buf);
               
            if ( l != l1)
                        throw new Exception(`Test of the static property 'BytesToLong()'_List   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
              try
            { 
               string s = parts[5].ToString();
               long l = long.Parse(s);
               byte[] buf = new byte[8];
               Entry.LongToBytes(l,buf,0);
               l = Entry.BytesToLong(buf,0);
               long l1;
               Entry.LongToBytes(l,buf,0);
               l1 = Entry.BytesToLong(buf,0);
               
            if ( l != l1)
                        throw new Exception(`Test of the static property 'BytesToLong()'_List_int   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
             
                try
            { 
               string s = parts[3].ToString();
               UInt16 i = UInt16.Parse(s);
               byte[] buf = Entry.UInt16ToBytes(i);
               i = Entry.BytesToUInt16(buf);
               UInt16 i1;
               Entry.UInt16ToBytes(i);
               i1 = Entry.BytesToUInt16(buf);
               
            if ( i != i1)
                        throw new Exception(`Test of the static property 'BytesToUInt16()'_List   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
              try
            { 
               string s = parts[3].ToString();
               UInt16 i = UInt16.Parse(s);
               byte[] buf = Entry.UInt16ToBytes(i);
               i = Entry.BytesToUInt16(buf,0);
               UInt16 i1;
               Entry.UInt16ToBytes(i);
               i1 = Entry.BytesToUInt16(buf,0);
               
            if ( i != i1)
                        throw new Exception(`Test of the static property 'BytesToUInt16()'_List_int   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
          
             
                try
            { 
               string s = parts[8].ToString();
               UInt32 i = UInt32.Parse(s);
               byte[] buf = Entry.UInt32ToBytes(i);
               i = Entry.BytesToUInt32(buf);
               UInt32 i1;
               Entry.UInt32ToBytes(i);
               i1 = Entry.BytesToUInt32(buf);
               
            if ( i != i1)
                        throw new Exception(`Test of the static property 'BytesToUInt32()'_List   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
              try
            { 
               string s = parts[8].ToString();
               UInt32 i = UInt32.Parse(s);
               byte[] buf = Entry.UInt32ToBytes(i);
               i = Entry.BytesToUInt32(buf,0);
               UInt32 i1;
               Entry.UInt32ToBytes(i);
               i1 = Entry.BytesToUInt32(buf,0);
               
            if ( i != i1)
                        throw new Exception(`Test of the static property 'BytesToUInt32()'_List_int   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
            try
            { 
               string s = parts[6].ToString();
               ulong l = ulong.Parse(s);
               byte[] buf = new byte[8];
               Entry.ULongToBytes(l,buf,0);
               l = Entry.BytesToULong(buf);
               ulong l1;
               Entry.ULongToBytes(l,buf,0);
               l1 = Entry.BytesToULong(buf);
               
            if ( l != l1)
                        throw new Exception(`Test of the static property 'BytesToULong()'_List   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
             
           try
            { 
               string s = parts[6].ToString();
               ulong l = ulong.Parse(s);
               byte[] buf = new byte[8];
               Entry.ULongToBytes(l,buf,0);
               l = Entry.BytesToULong(buf,0);
               ulong l1;
               Entry.ULongToBytes(l,buf,0);
               l1 = Entry.BytesToULong(buf,0);
               
            if ( l != l1)
                        throw new Exception(`Test of the static property 'BytesToULong()'_List_int   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
      
             
              return result; 
         }
          
         
         public static bool Entry_ToBytes_test(ByteSlice line)
         {
             mstring sLine = mstring.Prepare(line);
             mstringarray parts = sLine.SplitM(',');
             bool result = true;
             
         UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
      
           try
            { 
               string s = parts[0].ToString();
               double d = double.Parse(s);
               byte[] buf = new byte[9];
               byte[] buf1 = new byte[9];
             
               double d1 = d;
               Entry.DoubleToBytes(d,buf,0);
               Entry.DoubleToBytes(d1,buf1,0);
               d = Entry.BytesToDouble(buf,0);
               d1 = Entry.BytesToDouble(buf1,0);
           
            if ( d != d1)
                        throw new Exception(`Test of the static property 'DoubleToBytes()'   have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
            try
            { 
               string s = parts[4].ToString();
               Int16 i = Int16.Parse(s);
               byte[] buf = Entry.Int16ToBytes(i);
               byte [] buf2 = new byte[buf.Length];
               Int16 i1;
            
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                  for(int j = 0 ; j < buf.Length; j++)
                        buf2[j] = buf[buf.Length-j-1];
                        
                   i1 = System.BitConverter.ToInt16(buf2,0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToInt16(buf,0);
              }
 
             if ( i != i1)
                       throw new Exception(`Test of the static property 'Int16ToBytes()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
               try
            { 
               string s = parts[4].ToString();
               Int16 i = Int16.Parse(s);
               byte[] buf = new byte[2];
               Entry.Int16ToBytes(i,buf,0);
               byte [] buf2 = new byte[buf.Length];
               Int16 i1;
            
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                  for(int j = 0 ; j < buf.Length; j++)
                        buf2[j] = buf[buf.Length-j-1];
                        
                   i1 = System.BitConverter.ToInt16(buf2,0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToInt16(buf,0);
              }
 
             if ( i != i1)
                       throw new Exception(`Test of the static property 'Int16ToBytes()'_ByteArray_int  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
           try
            { 
               string s = parts[4].ToString();
               Int16 i = Int16.Parse(s);
               
               List<byte>  buf = new List<byte>();
             
               Entry.Int16ToBytesAppend(i,buf);
               
               Int16 i1;
           
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                   buf.Reverse();
                   i1 = System.BitConverter.ToInt16(buf.ToArray(),0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToInt16(buf.ToArray(),0);
              }
 
      
             if ( i != i1)
                       throw new Exception(`Test of the static property 'Int16ToBytesAppend()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
      
          
           try
            { 
               string s = parts[5].ToString();
               long l = long.Parse(s);
               byte[] buf = new byte[8];
               Entry.LongToBytes(l,buf,0);
               byte [] buf2 = new byte[buf.Length];
               long l1;
            
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                  for(int j = 0 ; j < buf.Length; j++)
                        buf2[j] = buf[buf.Length-j-1];
                        
                   l1 = System.BitConverter.ToInt64(buf2,0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   l1 = System.BitConverter.ToInt64(buf,0);
              }
 
             if ( l != l1)
                       throw new Exception(`Test of the static property 'LongToBytes()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
                   
            try
            { 
               string s = parts[7].ToString();
               int i = int.Parse(s);
               byte[] buf = Entry.ToBytes(i);
               byte [] buf2 = new byte[buf.Length];
               int i1;
            
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                  for(int j = 0 ; j < buf.Length; j++)
                        buf2[j] = buf[buf.Length-j-1];
                        
                   i1 = System.BitConverter.ToInt32(buf2,0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToInt32(buf,0);
              }
 
             if ( i != i1)
                       throw new Exception(`Test of the static property 'ToBytes(int)'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
               try
            { 
               string s = parts[7].ToString();
               int i = int.Parse(s);
               byte[] buf = new byte[4];
               Entry.ToBytes(i,buf,0);
               byte [] buf2 = new byte[buf.Length];
               int i1;
            
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                  for(int j = 0 ; j < buf.Length; j++)
                        buf2[j] = buf[buf.Length-j-1];
                        
                   i1 = System.BitConverter.ToInt32(buf2,0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToInt32(buf,0);
              }
 
             if ( i != i1)
                       throw new Exception(`Test of the static property 'ToBytes(ByteArray_int)'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
           try
            { 
               string s = parts[7].ToString();
               int i = int.Parse(s);
               
               List<byte>  buf = new List<byte>();
             
               Entry.ToBytesAppend(i,buf);
               
               int i1;
           
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                   buf.Reverse();
                   i1 = System.BitConverter.ToInt32(buf.ToArray(),0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToInt32(buf.ToArray(),0);
              }
 
      
             if ( i != i1)
                       throw new Exception(`Test of the static property 'ToBytesAppend()'_int  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
      
             
                   
           try
            { 
               string s = parts[5].ToString();
               long i = long.Parse(s);
               
               List<byte>  buf = new List<byte>();
             
               Entry.ToBytesAppend64(i,buf);
               
               long i1;
           
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                   buf.Reverse();
                   i1 = System.BitConverter.ToInt64(buf.ToArray(),0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToInt64(buf.ToArray(),0);
              }
 
      
             if ( i != i1)
                       throw new Exception(`Test of the static property 'ToBytesAppend64()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
      
             
            try
            { 
               string s = parts[0].ToString();
               double d = double.Parse(s);
               double d1 = d;
               List<byte>  buf = new List<byte>();
               List<byte>  buf1 = new List<byte>();
               Entry.ToBytesAppendDouble(d,buf);
               Entry.ToBytesAppendDouble(d1,buf1);
           
               double d2 = Entry.BytesToDouble(buf);
               double d3 = Entry.BytesToDouble(buf1);
   
               if ( d2 != d3)
                       throw new Exception(`Test of the static property 'ToBytesAppendDouble()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
            
               try
            { 
               string s = parts[3].ToString();
               UInt16 i = UInt16.Parse(s);
               byte[] buf = Entry.UInt16ToBytes(i);
               byte [] buf2 = new byte[buf.Length];
               UInt16 i1;
            
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                  for(int j = 0 ; j < buf.Length; j++)
                        buf2[j] = buf[buf.Length-j-1];
                        
                   i1 = System.BitConverter.ToUInt16(buf2,0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToUInt16(buf,0);
              }
 
             if ( i != i1)
                       throw new Exception(`Test of the static property 'UInt16ToBytes()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
               try
            { 
               string s = parts[3].ToString();
               UInt16 i = UInt16.Parse(s);
               byte[] buf = new byte[2];
               Entry.UInt16ToBytes(i,buf,0);
               byte [] buf2 = new byte[buf.Length];
               UInt16 i1;
            
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                  for(int j = 0 ; j < buf.Length; j++)
                        buf2[j] = buf[buf.Length-j-1];
                        
                   i1 = System.BitConverter.ToUInt16(buf2,0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToUInt16(buf,0);
              }
 
             if ( i != i1)
                       throw new Exception(`Test of the static property 'UInt16ToBytes()'_ByteArray_int  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
           try
            { 
               string s = parts[3].ToString();
               UInt16 i = UInt16.Parse(s);
               
               List<byte>  buf = new List<byte>();
             
               Entry.UInt16ToBytesAppend(i,buf);
               
               UInt16 i1;
           
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                   buf.Reverse();
                   i1 = System.BitConverter.ToUInt16(buf.ToArray(),0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToUInt16(buf.ToArray(),0);
              }
 
      
             if ( i != i1)
                       throw new Exception(`Test of the static property 'UInt16ToBytesAppend()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
                 
               try
            { 
               string s = parts[8].ToString();
               UInt32 i = UInt32.Parse(s);
               byte[] buf = Entry.UInt32ToBytes(i);
               byte [] buf2 = new byte[buf.Length];
               UInt32 i1;
            
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                  for(int j = 0 ; j < buf.Length; j++)
                        buf2[j] = buf[buf.Length-j-1];
                        
                   i1 = System.BitConverter.ToUInt32(buf2,0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToUInt32(buf,0);
              }
 
             if ( i != i1)
                       throw new Exception(`Test of the static property 'UInt32ToBytes()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
               try
            { 
               string s = parts[8].ToString();
               UInt32 i = UInt32.Parse(s);
               byte[] buf = new byte[4];
               Entry.UInt32ToBytes(i,buf,0);
               byte [] buf2 = new byte[buf.Length];
               UInt32 i1;
            
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                  for(int j = 0 ; j < buf.Length; j++)
                        buf2[j] = buf[buf.Length-j-1];
                        
                   i1 = System.BitConverter.ToUInt32(buf2,0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToUInt32(buf,0);
              }
 
             if ( i != i1)
                       throw new Exception(`Test of the static property 'UInt32ToBytes()'_ByteArray_int  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
           try
            { 
               string s = parts[8].ToString();
               UInt32 i = UInt32.Parse(s);
               
               List<byte>  buf = new List<byte>();
             
               Entry.UInt32ToBytesAppend(i,buf);
               
               UInt32 i1;
           
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                   buf.Reverse();
                   i1 = System.BitConverter.ToUInt32(buf.ToArray(),0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToUInt32(buf.ToArray(),0);
              }
 
      
             if ( i != i1)
                       throw new Exception(`Test of the static property 'UInt32ToBytesAppend()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
               try
            { 
               string s = parts[6].ToString();
               UInt64 i = UInt64.Parse(s);
               byte[] buf = new byte[8];
               Entry.ULongToBytes(i,buf,0);
               byte [] buf2 = new byte[buf.Length];
               UInt64 i1;
            
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                  for(int j = 0 ; j < buf.Length; j++)
                        buf2[j] = buf[buf.Length-j-1];
                        
                   i1 = System.BitConverter.ToUInt64(buf2,0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToUInt64(buf,0);
              }
 
             if ( i != i1)
                       throw new Exception(`Test of the static property 'UInt64ToBytes()'_ByteArray_int  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
           try
            { 
               string s = parts[6].ToString();
               UInt64 i = UInt64.Parse(s);
               
               List<byte>  buf = new List<byte>();
             
               Entry.ULongToBytesAppend(i,buf);
               
               UInt64 i1;
           
            if (System.BitConverter.IsLittleEndian) // checking for endianess
               {
                   buf.Reverse();
                   i1 = System.BitConverter.ToUInt64(buf.ToArray(),0);  //  little endian byte array is taken as argument
               }
            else
              { 
                   i1 = System.BitConverter.ToUInt64(buf.ToArray(),0);
              }
 
      
             if ( i != i1)
                       throw new Exception(`Test of the static property 'UInt64ToBytesAppend()'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
             return result; 
      
             
         }
         
         
         
          public static bool Entry_U_Reg_Reg_U_test(ByteSlice line)
           {
                 
             mstring sLine = mstring.Prepare(line);
             mstringarray parts = sLine.SplitM(',');
             bool result = true;
             
            UserMapper.DfsMapper  Log = new UserMapper.DfsMapper();
                 
                      
           try
            { 
               string s = parts[3].ToString();
               UInt16 i = UInt16.Parse(s);
               Int16 i1 = (Int16)(i - Int16.MaxValue - 1);
               Int16 i2 = Entry.ToInt16(i);
                      
             if ( i2 != i1)
                  throw new Exception(`Test of the static property 'ToInt16'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
          try
            { 
               string s = parts[4].ToString();
               Int16 i = Int16.Parse(s);
               UInt16 i1 = (UInt16)(i + Int16.MaxValue + 1);
               UInt16 i2 = Entry.ToUInt16(i);
                      
             if ( i2 != i1)
                  throw new Exception(`Test of the static property 'ToUInt16'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
                  
           try
            { 
               string s = parts[8].ToString();
               uint i = uint.Parse(s);
               int i1 = (int)(i - int.MaxValue - 1);
               int i2 = Entry.ToInt32(i);
                      
             if ( i2 != i1)
                  throw new Exception(`Test of the static property 'ToInt32'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
          try
            { 
               string s = parts[7].ToString();
               int i = int.Parse(s);
               uint i1 = (uint)(i + int.MaxValue + 1);
               uint i2 = Entry.ToUInt32(i);
                      
             if ( i2 != i1)
                  throw new Exception(`Test of the static property 'ToUInt32'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
               try
            { 
               string s = parts[6].ToString();
               UInt64 i = UInt64.Parse(s);
               Int64 i1 = (Int64)(i - Int64.MaxValue - 1);
               Int64 i2 = Entry.ToInt64(i);
                      
             if ( i2 != i1)
                  throw new Exception(`Test of the static property 'ToInt64'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
             
             
          try
            { 
               string s = parts[5].ToString();
               Int64 i = Int64.Parse(s);
               UInt64 i1 = (UInt64)(i + Int64.MaxValue + 1);
               UInt64 i2 = Entry.ToUInt64(i);
                      
             if ( i2 != i1)
                  throw new Exception(`Test of the static property 'ToUInt64'  have failed`);
                    
           
             }
          catch(Exception e)
            {
                  Log.Qizmt_Log(`Exception`+e.ToString());
                  
                  result = false;
             }
        
                 return result;
           
             }
         
       
         
        ]]>
				</Map>
				<Reduce>
					<![CDATA[
            public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
            {
                
                
            }   
        ]]>
				</Reduce>
			</MapReduce>
		</Job>
		<Job>
			<Narrative>
				<Name>regression_test_OptimalDriver1_post-processing</Name>
				<Custodian></Custodian>
				<email></email>
			</Narrative>0
			<IOSettings>
				<JobType>local</JobType>
			</IOSettings>
			<Local>
				<![CDATA[
        public virtual void Local()
        {
           //Compare input and output file, pass if they are the same.
          //  Shell(@`Qizmt exec regression_test_checkTestResult.xml regression_test_mstring_Common(char)_Input.txt regression_test_mstring_Common(char)_Output.txt regression_test_mstring_Common(char).xml`);     
            
            Shell(@`Qizmt del regression_test_mstring_Common(char)_Input.txt`, true);
            Shell(@`Qizmt del regression_test_mstring_Common(char)_Output.txt`, true); 
        }
        ]]>
			</Local>
		</Job>
	</Jobs>
</SourceCode>".Replace('`', '"'));
            Console.WriteLine("    Qizmt exec {0}", alljobfiles[alljobfiles.Count - 1]);
            #endregion
        }

    }
}
