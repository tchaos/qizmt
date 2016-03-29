<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Methods of ReduceOutput](MySpaceQizmtReferenceReduceOutputMethods.md)



# `Cache` #
`public void Cache(ByteSlice key, ByteSlice value)`

Writes key and value to explicit cache.
### Remarks ###
Once a key-value pair has been added to explicit cache, the next time mapReducer runs, the values will be passed back in.  Keys cannot be changed.

> Explicit cache overrides the automatic cache.  It speeds up subsequent runs.

### Example ###
Specify cache configurations:

```
<Delta>
    <Name>MyMapReduce_Cache</Name>
    <DFSInput>dfs://data_input_*.txt</DFSInput>
</Delta>
<IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://NOTHING*NOTHING</DFSInput>
    <DFSOutput>dfs://MyMapReduce_Output.txt</DFSOutput>
</IOSettings>
```


```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values[i].Value.ToString();
        output.Cache(key, values[i].Value);
    }

    output.Add(ByteSlice.Prepare("key=" + sKey), true);
    output.Add(ByteSlice.Prepare("values=" + sValue), true);
} 
```

---




`public void Cache(string key, string value)`

Writes key string and value string to explicit cache.
### Remarks ###
Once a key-value pair has been added to explicit cache, the next time mapReducer runs, the values will be passed back in.  Keys cannot be changed.

> Explicit cache overrides the automatic cache.  It speeds up subsequent runs.

### Example ###
Specify cache configurations:

```
<Delta>
    <Name>MyMapReduce_Cache</Name>
    <DFSInput>dfs://data_input_*.txt</DFSInput>
</Delta>
<IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://NOTHING*NOTHING</DFSInput>
    <DFSOutput>dfs://MyMapReduce_Output.txt</DFSOutput>
</IOSettings>
```


```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    mstring sLine = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        mstring sValue = mstring.Prepare(values[i].Value);
        sLine.AppendM(sValue);
        sLine.AppendM(',');

        output.Cache("1", "10");
    }

    output.Add(sKey);
    output.Add(sLine);
}
```

---




`public void Cache(recordset key, recordset value)`

Writes key recordset and value recordset to explicit cache.
### Remarks ###
Once a key-value pair has been added to explicit cache, the next time mapReducer runs, the values will be passed back in.  Keys cannot be changed.

> Explicit cache overrides the automatic cache.  It speeds up subsequent runs.

### Example ###
Specify cache configurations:

```
<Delta>
    <Name>MyMapReduce_Cache</Name>
    <DFSInput>dfs://data_input_*.txt</DFSInput>
</Delta>
<IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://NOTHING*NOTHING</DFSInput>
    <DFSOutput>dfs://MyMapReduce_Output.txt</DFSOutput>
</IOSettings>
```


```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(UnpadKey(key));

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        output.Cache(rKey, recordset.Prepare());
    }
} 
```

---




`public void Cache(mstring key, mstring value)`

Writes key mstring and value mstring to explicit cache.
### Remarks ###
Once a key-value pair has been added to explicit cache, the next time mapReducer runs, the values will be passed back in.  Keys cannot be changed.

> Explicit cache overrides the automatic cache.  It speeds up subsequent runs.

### Example ###
Specify cache configurations:

```
<Delta>
    <Name>MyMapReduce_Cache</Name>
    <DFSInput>dfs://data_input_*.txt</DFSInput>
</Delta>
<IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://NOTHING*NOTHING</DFSInput>
    <DFSOutput>dfs://MyMapReduce_Output.txt</DFSOutput>
</IOSettings>
```


```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    mstring sLine = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        mstring sValue = mstring.Prepare(values[i].Value);
        sLine.AppendM(sValue);
        sLine.AppendM(',');

        output.Cache(sKey, mstring.Prepare());
    }

    output.Add(sKey);
    output.Add(sLine);
} 
```

---




`public void Cache(mstring key, recordset value)`

Writes key mstring and value recordset to explicit cache.
### Remarks ###
Once a key-value pair has been added to explicit cache, the next time mapReducer runs, the values will be passed back in.  Keys cannot be changed.

> Explicit cache overrides the automatic cache.  It speeds up subsequent runs.

### Example ###
Specify cache configurations:

```
<Delta>
    <Name>MyMapReduce_Cache</Name>
    <DFSInput>dfs://data_input_*.txt</DFSInput>
</Delta>
<IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://NOTHING*NOTHING</DFSInput>
    <DFSOutput>dfs://MyMapReduce_Output.txt</DFSOutput>
</IOSettings>
```


```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));

    for (int i = 0; i < values.Length; i++)
    {
        recordset rs = recordset.Prepare(values[i].Value);

        output.Cache(sKey, rs);
    }
} 
```