<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Static methods of Entry](MySpaceQizmtReferenceEntryStaticMethods.md)



# `AsciiToBytes` #
`public static void AsciiToBytes(string x, byte[] buffer, int offset)`

Converts string to bytes using ASCII encoding and appends to buffer with offset.

### Example ###
```
byte[] myBuffer = new byte[10];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    string value = parts[1];
    for (int i = 0; i < value.Length; i++)
    {
        //Some code for manipulating the string...                  
    }

    Entry.AsciiToBytes(value, myBuffer, 0);
    output.Add(ByteSlice.Prepare(parts[0].PadRight(16, '\0')), ByteSlice.Prepare(myBuffer));
} 
```