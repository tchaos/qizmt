<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Properties of `ReduceEntry` #



## `Key` ##
`public ByteSlice Key`

Returns the Key of this entry.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    for (int i = 0; i < values.Length; i++)
    {
        ReduceEntry entry = values[i];
        output.Add(entry.Key);
        output.Add(entry.Value);
    }
} 
```

---




## `Value` ##
`public ByteSlice Value`

Returns the Value of this entry.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    for (int i = 0; i < values.Length; i++)
    {
        ReduceEntry entry = values[i];
        output.Add(entry.Key);
        output.Add(entry.Value);
    }
} 
```