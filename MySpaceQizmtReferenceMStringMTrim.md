<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `MTrim` #
`public mstring MTrim()`

Trims off spaces from the left and right of this mstring.  The mstring is changed here.  The mstring itself is returned.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstringarray parts = sLine.SplitM(' ');

    for (int i = 0; i < parts.Length; i++)
    {
        mstring word = parts[i].MTrim().MTrim(',').MTrim('!').MTrim('?').MTrim(':').MTrim(';').MTrim('(').MTrim(')');

        if (word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
        {
            output.Add(word.ToLowerM(), mstring.Prepare(1));
        }
    }
} 
```

---




`public mstring MTrim(char c)`

Trims off char c from the left and right of this mstring.  The mstring is changed here.  The mstring itself is returned.
### Remarks ###
remarks

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstringarray parts = sLine.SplitM(' ');

    for (int i = 0; i < parts.Length; i++)
    {
        mstring word = parts[i].MTrim().MTrim(',').MTrim('!').MTrim('?').MTrim(':').MTrim(';').MTrim('(').MTrim(')');

        if (word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
        {
            output.Add(word.ToLowerM(), mstring.Prepare(1));
        }
    }
} 
```

---


# `TrimM` #
`public mstring TrimM()`

Trims off spaces from the left and right of this mstring.  The mstring is changed here.  The mstring itself is returned.
### Remarks ###
This is equivalent to `Mtrim()`.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstringarray parts = sLine.SplitM(' ');

    for (int i = 0; i < parts.Length; i++)
    {
        mstring word = parts[i].TrimM().TrimM(',').TrimM('!').TrimM('?').TrimM(':').TrimM(';').TrimM('(').TrimM(')');

        if (word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
        {
            output.Add(word.ToLowerM(), mstring.Prepare(1));
        }
    }
} 
```

---




`public mstring TrimM(char c)`

Trims off char c from the left and right of this mstring.  The mstring is changed here.  The mstring itself is returned.
### Remarks ###
This is equivalent to Mtrim(char c)

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstringarray parts = sLine.SplitM(' ');

    for (int i = 0; i < parts.Length; i++)
    {
        mstring word = parts[i].TrimM().TrimM(',').TrimM('!').TrimM('?').TrimM(':').TrimM(';').TrimM('(').TrimM(')');

        if (word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
        {
            output.Add(word.ToLowerM(), mstring.Prepare(1));
        }
    }
} 
```