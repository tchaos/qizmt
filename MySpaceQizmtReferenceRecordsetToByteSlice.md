<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of recordset](MySpaceQizmtReferenceRecordsetMethods.md)



# `ToByteSlice` #
`public ByteSlice ToByteSlice()`

Converts this recordset to a `ByteSlice`.  This recordset is changed after the conversion.
### Remarks ###
It is adivsed not to re-use the recordset after it has been converted to a ByteSlice.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    int x = sLine.NextItemToInt(',');
    int y = sLine.NextItemToInt(',');
    mstring name = sLine.NextItemToString(',');

    recordset key = recordset.Prepare();
    key.PutInt(x);
    key.PutInt(y);

    recordset value = recordset.Prepare();
    value.PutString(name);

    ByteSlice bKey = key.ToByteSlice(10);
    ByteSlice bValue = value.ToByteSlice();

    output.Add(bKey, bValue);
} 
```

---




`public ByteSlice ToByteSlice(int size)`

Converts this recordset to a ByteSlice of length = size.   Padding will occur if the recordset if smaller than size.  If the recordset is bigger than size, it will throw an exception.  This recordset is changed after the conversion.
### Remarks ###
It is adivsed not to re-use the recordset after it has been converted to a ByteSlice.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    int x = sLine.NextItemToInt(',');
    int y = sLine.NextItemToInt(',');
    mstring name = sLine.NextItemToString(',');

    recordset key = recordset.Prepare();
    key.PutInt(x);
    key.PutInt(y);

    recordset value = recordset.Prepare();
    value.PutString(name);

    ByteSlice bKey = key.ToByteSlice(10);
    ByteSlice bValue = value.ToByteSlice();

    output.Add(bKey, bValue);
} 
```