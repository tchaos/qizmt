<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `PutUInt32` #
`public void PutUInt32(UInt32 x)`

Put a UInt32 x into the recordset.
### Remarks ###
The order of putting and getting items from the recordset must be the same.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    int i1 = ms.NextItemToInt32(',');
    int i2 = ms.NextItemToInt32(',');
    Int64 i3 = ms.NextItemToInt64(',');
    mstring s = ms.NextItemToString(',');
    UInt32 i4 = ms.NextItemToUInt32(',');

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutUInt32(i4);

    output.Add(rKey, rValue);
} 
```

---




# `PutUInt` #
`public void PutUInt(UInt32 x)`

Put a UInt32 x into the recordset.
### Remarks ###
This is equivalent to PutUInt32.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    int i1 = ms.NextItemToInt32(',');
    int i2 = ms.NextItemToInt32(',');
    Int64 i3 = ms.NextItemToInt64(',');
    mstring s = ms.NextItemToString(',');
    UInt32 i4 = ms.NextItemToUInt32(',');

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutUInt(i4);

    output.Add(rKey, rValue);
} 
```