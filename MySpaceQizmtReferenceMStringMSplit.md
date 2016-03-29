<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `MSplit` #
`public mstringarray MSplit(char delimiter)`

Returns a mstring array that contains the substrings in this instance that are delimited by the char delimiter.

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

    mstringarray parts = sLine.MSplit('|');

    Int64 i1 = parts[0].ToInt64();
    mstring value = parts[1];
    Int64 i2 = parts[2].ToInt64();

    Int64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```