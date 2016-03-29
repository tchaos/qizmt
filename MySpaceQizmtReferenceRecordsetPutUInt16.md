<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `PutUInt16` #
`public void PutUInt16(UInt16 x)`

Put a UInt16 x into the recordset.
### Remarks ###
The order of putting and getting items from the recordset must be the same.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,i
    //61,92,383,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutInt32(i4);

    Int16 m = (Int16)(i1 % 10);
    rValue.PutUInt16(m);

    output.Add(rKey, rValue);
} 
```

---




# `PutUShort` #
`public void PutUShort(UInt16 x)`

Put a UInt16 x into the recordset.
### Remarks ###
This is equivalent to PutUInt16.  The order of putting and getting items from the recordset must be the same.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,i
    //61,92,383,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutInt32(i4);

    Int16 m = (Int16)(i1 % 10);
    rValue.PutUShort(m);

    output.Add(rKey, rValue);
} 
```