<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Methods of `ByteSliceList` #



## `MoveNext` ##
`public bool MoveNext()`

Moves the current position to the next value ByteSlice.  If there is no more value ByteSlice left, it returns false.

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




## `Reset` ##
`public void Reset()`

Moves the current position to the first value ByteSlice.

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

    values.Reset();

    while (values.MoveNext())
    {
        sValue.AppendM('-');
        sValue.AppendM(mstring.Prepare(values.Current()));
    }

    output.Add(sValue);
} 
```