<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `NextItemToInt64` #
`public Int64 NextItemToInt64(char delimiter)`

Get the next Int64 item, delimited by the char delimiter.

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

    Int64 i1 = sLine.NextItemToInt64('|');
    mstring value = sLine.NextItemToString('|');
    Int64 i2 = sLine.NextItemToInt64('|');

    Int64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---


# `NextItemToLong` #
`public Int64 NextItemToLong(char delimiter)`

Get the next Int64 item, delimited by the char delimiter.
### Remarks ###
This is equivalent to NextItemToInt64(char delimiter).

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

    Int64 i1 = sLine.NextItemToLong('|');
    mstring value = sLine.NextItemToString('|');
    Int64 i2 = sLine.NextItemToLong('|');

    Int64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```
