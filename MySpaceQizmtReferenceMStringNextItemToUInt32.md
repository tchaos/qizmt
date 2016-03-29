<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `NextItemToUInt32` #
`public Int32 NextItemToUInt32(char delimiter)`

Get the next UInt32 item, delimited by the char delimiter.

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

    UInt32 i1 = sLine.NextItemToUInt32('|');
    mstring value = sLine.NextItemToString('|');
    UInt32 i2 = sLine.NextItemToUInt32('|');

    UInt32 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




# `NextItemToUInt` #
`public Int32 NextItemToUInt(char delimiter)`

Get the next UInt32 item, delimited by the char delimiter.
### Remarks ###
This is equivalent to NextItemToUInt32.

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

    UInt32 i1 = sLine.NextItemToUInt('|');
    mstring value = sLine.NextItemToString('|');
    UInt32 i2 = sLine.NextItemToUInt('|');

    UInt32 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```