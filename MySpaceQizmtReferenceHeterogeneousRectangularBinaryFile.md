<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)

## Heterogeneous Rectangular Binary Files ##

Input and Output files in mapreduce job with different record lengths

### Example ###
```
<Job Name="testJob" Custodian="" Email="">
  <IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>int</KeyLength>
    <DFSInput>dfs://job_Input1.txt;job_Input2.bin@Int;job_Input3.bin@Int,Int</DFSInput>
    <DFSOutput>job_Output1.txt;job_Output2.bin@Int,Int,Int;job_Output3.bin@Long</DFSOutput>
    <OutputMethod>grouped</OutputMethod>
  </IOSettings>
  <MapReduce>
    <Map>
      <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {  
              int key = 0;
              int num1 = 0;
              int num2 = 0;
              
              if(StaticGlobals.Qizmt_InputFileName == "job_Input1.txt")
              {                
                  mstring mline = mstring.Prepare(line);
                  key = mline.NextItemToInt(',');
                  num1 = mline.NextItemToInt(',');
                  num2 = mline.NextItemToInt(',');
              }
              else if(StaticGlobals.Qizmt_InputFileName == "job_Input2.bin")
              {
                  recordset rline = recordset.Prepare(line);
                  key = rline.GetInt();
                  num1 = 0;
                  num2 = 0;
              }
              else
              {
                  recordset rline = recordset.Prepare(line);
                  key = rline.GetInt();
                  num1 = (int)rline.GetInt();
                  num2 = 0;
              }                
                
               recordset rkey = recordset.Prepare();
               rkey.PutInt(key);
                
               recordset rval = recordset.Prepare();
               rval.PutInt(num1);
               rval.PutInt(num2);
               output.Add(rkey, rval);
          }
        ]]>
    </Map>
    <Reduce>
      <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {     
              recordset rkey = recordset.Prepare(key);
              int ikey = rkey.GetInt();
              
              long sum = 0;              
              for(int i = 0; i < values.Length; i++)
              {
                  recordset rval = recordset.Prepare(values[i].Value);
                  int num1 = rval.GetInt();
                  int num2 = rval.GetInt();
                  sum = num1 + num2;    
                  
                  //output this line as a string
                  mstring ms = mstring.Prepare(ikey);
                  ms = ms.AppendM(',').AppendM(num1).AppendM(',').AppendM(num2);
                  output.GetOutputByIndex(0).Add(ms);
                  
                  //output this line as a binary record
                  recordset rout = recordset.Prepare();
                  rout.PutInt(ikey);
                  rout.PutInt(num1);
                  rout.PutInt(num2);
                  output.GetOutputByIndex(1).WriteRecord(rout.ToByteSlice());                                
              }
              
              //output the sum as a binary record
              {
                  recordset rout = recordset.Prepare();
                  rout.PutLong(sum);
                  output.GetOutputByIndex(2).WriteRecord(rout.ToByteSlice());      
              }                      
          }
        ]]>
    </Reduce>
  </MapReduce>
</Job>
```