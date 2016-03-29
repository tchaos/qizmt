<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Properties and Indexer of `ByteSliceList` (Equivalent to `RandomAccessEntries`) #


## `Length` ##
`public int Length{get;`}

Returns the count of entries that are passed into reducer.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values[i].Value.ToString();
    }

    output.Add(ByteSlice.Prepare("key=" + sKey), true);
    output.Add(ByteSlice.Prepare("values=" + sValue), true);
} 
```

---




## `Current` ##
`public ByteSlice Current{get;`}

Returns the current value ByteSlice.

#### Example ####
```
public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    mstring sValue = mstring.Prepare();

    while (values.MoveNext())
    {
        sValue.AppendM(',');
        sValue.AppendM(mstring.Prepare(values.Current()));
    }

    output.Add(sValue);
} 
```

---




## `Indexer` ##
`public ReduceEntry this[int i]`

Returns the i-th ReduceEntry.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values[i].Value.ToString();
    }

    output.Add(ByteSlice.Prepare("key=" + sKey), true);
    output.Add(ByteSlice.Prepare("values=" + sValue), true);
} 
```

---




## `Items` ##
`public RandomAccessEntriesItems Items`

Returns the value ByteSlices.

#### Example ####
```
public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values.Items[i].ToString();
    }

    output.Add(ByteSlice.Prepare("key=" + sKey), true);
    output.Add(ByteSlice.Prepare("values=" + sValue), true);
} 
```