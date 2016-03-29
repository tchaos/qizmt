<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `GetUInt32` #
`public UInt32 GetUInt32()`

Get the next UInt32 from the recordset.

### Example ###
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetInt64();
        mstring s = rValue.GetString();
        UInt32 i4 = rValue.GetUInt32();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




# `GetUInt` #
`public UInt32 GetUInt()`

Get the next UInt32 from the recordset.
### Remarks ###
This is equivalent to GetUInt32.

### Example ###
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetInt64();
        mstring s = rValue.GetString();
        UInt32 i4 = rValue.GetUInt();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```