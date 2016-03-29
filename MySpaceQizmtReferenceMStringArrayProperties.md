<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Properties and Indexer of `mstringarray` #



## `Length` ##
`public int Length{get;`}

Returns the length of this mstringarray.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstringarray arr = mstringarray.Prepare(10);

    for (int i = 0; i < arr.Length; i++)
    {
        arr[i] = mstring.Prepare(i);
    }

    mstring ms = mstring.Prepare();

    for (int i = 0; i < arr.Length; i++)
    {
        ms = ms.MAppend(arr[i]);
    }

    output.Add(ms, mstring.Prepare());
} 
```

---




## `Indexer` ##
`public mstring this[int i]`

Returns the i-th mstring in the array.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstringarray arr = mstringarray.Prepare(10);

    for (int i = 0; i < arr.Length; i++)
    {
        arr[i] = mstring.Prepare(i);
    }

    mstring ms = mstring.Prepare();

    for (int i = 0; i < arr.Length; i++)
    {
        ms = ms.MAppend(arr[i]);
    }

    output.Add(ms, mstring.Prepare());
} 
```