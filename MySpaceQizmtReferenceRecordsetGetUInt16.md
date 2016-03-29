<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `GetUInt16` #
`public UInt16 GetUInt16()`

Get the next UInt16 from the recordset.

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

        UInt16 i3 = rValue.GetUInt16();
        char s = rValue.GetChar();
        int i4 = rValue.GetInt32();

        mstring delimiter = mstring.Prepare(",");
        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




# `GetUShort` #
`public UInt16 GetUShort()`

Get the next UInt16 from the recordset.
### Remarks ###
This is equivalent to GetUInt16.

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

        UInt16 i3 = rValue.GetUShort();
        char s = rValue.GetChar();
        int i4 = rValue.GetInt32();

        mstring delimiter = mstring.Prepare(",");
        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```