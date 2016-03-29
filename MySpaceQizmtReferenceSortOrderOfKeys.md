<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)

## Specify the sort order of keys, ascending or descending, in Job/IOSettings/OutputDirection. This only takes effect when the `OutputMethod` is sorted or hashsorted. ##

### Example ###
```

<Job Name="outputDirection_desc_mapreduce" Custodian="" Email="">
  <IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://desc_mapreduce_Input.txt</DFSInput>
    <DFSOutput>dfs://desc_mapreduce_OutputD.txt</DFSOutput>
    <OutputMethod>sorted</OutputMethod>
    <OutputDirection>descending</OutputDirection>
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
  </MapReduce>
</Job>

```