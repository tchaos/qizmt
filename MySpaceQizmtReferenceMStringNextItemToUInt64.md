<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `NextItemToUInt64` #
`public Int64 NextItemToUInt64(char delimiter)`

Get the next UInt64 item, delimited by the char delimiter.

### Example ###
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt64 i1 = sLine.NextItemToUInt64('|');
    mstring value = sLine.NextItemToString('|');
    UInt64 i2 = sLine.NextItemToUInt64('|');

    UInt64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




# `NextItemToULong` #
`public Int64 NextItemToULong(char delimiter)`

Get the next UInt64 item, delimited by the char delimiter.
#### Remarks ####
This is equivalent to NextItemToUInt64.

### Example ###
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt64 i1 = sLine.NextItemToULong('|');
    mstring value = sLine.NextItemToString('|');
    UInt64 i2 = sLine.NextItemToULong('|');

    UInt64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```