<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `PutUInt64` #
`public void PutUInt64(UInt64 x)`

Put a UInt64 x into the recordset.
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
    UInt64 i4 = ms.NextItemToUInt64(',');

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutUInt64(i4);

    output.Add(rKey, rValue);
} 
```

---




# `PutULong` #
`public void PutULong(UInt64 x)`

Put a UInt64 x into the recordset.
### Remarks ###
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    int i1 = ms.NextItemToInt32(',');
    int i2 = ms.NextItemToInt32(',');
    Int64 i3 = ms.NextItemToInt64(',');
    mstring s = ms.NextItemToString(',');
    UInt64 i4 = ms.NextItemToUInt64(',');

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutULong(i4);

    output.Add(rKey, rValue);
} 
```