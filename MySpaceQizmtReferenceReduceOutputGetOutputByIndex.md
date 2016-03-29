<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Methods of ReduceOutput](MySpaceQizmtReferenceReduceOutputMethods.md)



# `GetOutputByIndex` #
`public ReduceOutput GetOutputByIndex(int i)`

Returns the ReduceOutput instance that corresponds to the i-th output file.

### Remarks ###
This allows outputing to a specific file when there are multiple output files.

### Example ###
```
<Job Name="testJob" Custodian="" Email="">
  <IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>int</KeyLength>
    <DFSInput>dfs://job_Input*.txt</DFSInput>
    <DFSOutput>job_Output1.txt;job_Output2.txt</DFSOutput>
    <OutputMethod>grouped</OutputMethod>
  </IOSettings>
  <MapReduce>
    <Map>
      <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {                
                int key = 0;
                if(StaticGlobals.DSpace_InputFileName == "job_Input1.txt")
                {
                    key = 1;
                }
                else if(StaticGlobals.DSpace_InputFileName == "job_Input2.txt")
                {
                    key = 2;
                }
                
                recordset rkey = recordset.Prepare();
                rkey.PutInt(key);
                mstring val = mstring.Prepare(line);
                output.Add(rkey, val);
          }
        ]]>
    </Map>
    <Reduce>
      <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {     
              recordset rkey = recordset.Prepare(key);
              int ikey = rkey.GetInt();
              ReduceOutput thisoutput = output.GetOutputByIndex(ikey - 1);
              
              for(int i = 0; i < values.Length; i++)
              {
                 thisoutput.Add(values[i].Value);  
              }
          }
        ]]>
    </Reduce>
  </MapReduce>
</Job>
```


