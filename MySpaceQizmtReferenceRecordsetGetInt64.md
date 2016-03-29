<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `GetInt64` #
`public Int64 GetInt64()`

Get the next Int64 from the recordset.
### Remarks ###
The order of putting and getting items from the recordset must be the same.

### Example ###
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    int i1 = rKey.GetInt32();
    int i2 = rKey.GetInt32();

    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetInt64();
        mstring s = rValue.GetString();
        int i4 = rValue.GetInt32();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




# `GetLong` #
`public Int64 GetLong()`

Get the next Int64 from the recordset.
### Remarks ###
This is equivalent to GetInt64().

### Example ###
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    int i1 = rKey.GetInt32();
    int i2 = rKey.GetInt32();

    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetLong();
        mstring s = rValue.GetString();
        int i4 = rValue.GetInt32();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```