<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt SQL Tutorial](MySpaceQizmtSQLQuickStartGuide.md) / [Walkthrough](MySpaceQizmtSQLQuickStartGuideWalkthroughContents.md)


# Walkthrough: Qizmt SQL Extension (contd) #



## 19.  Sample Mapreduce Code ##

Replace the entire contents of the mapreducer with the following code:
_(Note: key length may contain any combination of comma separated null-able types_

_These include: nInt,nLong,nDouble,nDateTime,nChar(m)_

```
<SourceCode>
  <Jobs>
    <Job Name="MyPaintingsWordCount" Custodian="" Email="">
      <IOSettings>
        <JobType>mapreduce</JobType>
        <KeyLength>nChar(25)</KeyLength>
        <DFSInput>dfs://RDBMS_Table_paintings@nInt,nInt,nChar(300),nDouble,nLong,nInt,nDateTime</DFSInput>
        <DFSOutput>dfs://MyPaintingsWordCount_Output@nChar(25),nInt</DFSOutput>
        <OutputMethod>sorted</OutputMethod>
      </IOSettings>
      <Add Reference="RDBMS_DBCORE.dll" Type="dfs"/>
      <Using>RDBMS_DBCORE</Using>
      <MapReduce>
        <Map>
          <![CDATA[
          public virtual void Map(ByteSlice line, MapOutput output)
          {              
              DbRecordset dbr = DbRecordset.Prepare(line);              
              dbr.GetInt();
              dbr.GetInt();              

              mstring sTitle = dbr.GetString(300);              
              sTitle = sTitle.TrimM('\0');//trim off padding
              mstringarray parts = sTitle.SplitM(' ');
              for(int i=0; i < parts.Length; i++)
              {
                    mstring word = parts[i];
                    if(word.Length > 0)
                    {
                        DbRecordset dbrKey = DbRecordset.Prepare();
                        dbrKey.PutString(word.ToLowerM(), 25);
                        DbRecordset dbrValue = DbRecordset.Prepare();
                        dbrValue.PutInt(1);
                        output.Add(dbrKey.ToByteSlice(), dbrValue.ToByteSlice());
                    }                                 
             }
          }
        ]]>
        </Map>
        <Reduce>
          <![CDATA[
          public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
          {
              
              DbRecordset dbrKey = DbRecordset.Prepare(key);
              mstring sWord = dbrKey.GetString(25);
              int iCount = values.Length;
              DbRecordset count_of_word = DbRecordset.Prepare();
              count_of_word.PutString(sWord, 25);
              count_of_word.PutInt(iCount);
              output.Add(count_of_word.ToByteSlice());
          }
        ]]>
        </Reduce>
      </MapReduce>
    </Job>
  </Jobs>
</SourceCode>
```

## 20.  Add Breakpoint ##

Add a breakpoint to the last line of the Reducer, by placing a cursor on the line containing _output.Add(count\_of\_word);_ and then press **F9**.


<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/QSQL_AddBreakPoint.png' alt='Add Break Point' />

[< PREV](MySpaceQizmtSQLQuickStartGuideWalkthrough7.md)
[NEXT >](MySpaceQizmtSQLQuickStartGuideWalkthrough9.md)