<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Methods of ReduceOutput](MySpaceQizmtReferenceReduceOutputMethods.md)



# `WriteRecord` #
`public void WriteRecord(ByteSlice entry)`

Writes the entry to a rectangular binary output file in MR.DFS. This may be used from remote jobs or reducers to produce nullable rectangular binary data.

### Remarks ###
Exception will be thrown if the length of the entry `ByteSlice` is different from the record length of the rectangular binary output file.

### Example ###
```
<Job Name="testJob" Custodian="" Email="">
  <IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>int</KeyLength>
    <DFSInput>dfs://job_Input.txt</DFSInput>
    <DFSOutput>job_Output.bin@Int,Int</DFSOutput>
    <OutputMethod>grouped</OutputMethod>
  </IOSettings>
  <MapReduce>
    <Map>
      <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {  
                mstring mline = mstring.Prepare(line);
                int key = mline.NextItemToInt(',');
                int num1 = mline.NextItemToInt(',');
                int num2 = mline.NextItemToInt(',');
                
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
              
              int sum = 0;              
              for(int i = 0; i < values.Length; i++)
              {
                  recordset rval = recordset.Prepare(values[i].Value);
                  int num1 = rval.GetInt();
                  int num2 = rval.GetInt();
                  sum = num1 + num2;                  
              }
              
              recordset rout = recordset.Prepare();
              rout.PutInt(ikey);
              rout.PutInt(sum);
              output.WriteRecord(rout.ToByteSlice());              
          }
        ]]>
    </Reduce>
  </MapReduce>
</Job>
```