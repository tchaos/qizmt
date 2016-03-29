<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)

## IntermediateDataAddressing and KeyRepeatedEnabled ##

64bit intermediate data addressing allows 1GB of key/value pairs to go to a reducer.  `KeyRepeatedEnabled` allows a key with more than 1 GB of key/value pairs to go to multiple reducers.

### Remarks ###

Even with 64bit mode, only 1GB is sent into reducer at a time.  If a key has more than 1 GB of key/value pairs, for example, 20 GB, then it will go into 20 reducer exectuions on the same key.  The constant `Qizmt_KeyRepeated` is true when a key goes to multiple reducers.   There is an exponential IO cost when this occurs.

### Example ###
```
<Job Name="WordCount" Custodian="" email="">
  <IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>16</KeyLength>
    <DFSInput>dfs://WordCount_Input.txt</DFSInput>
    <DFSOutput>dfs://WordCount_Output.txt</DFSOutput>
  </IOSettings>

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
```