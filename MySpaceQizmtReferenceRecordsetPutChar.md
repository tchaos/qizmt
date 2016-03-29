<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `PutChar` #
`public void PutChar(char c)`

Put a char c into the recordset.
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
    rValue.PutString('-');
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);
} 
```