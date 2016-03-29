<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)

## Use of Job/Unsafe to allow a job to run unsafe code ##


### Example ###
This example uses Pointer.

```
<Job Name="PointerTest_CopyReverse" Custodian="" email="">
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
```