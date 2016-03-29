<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Static Methods of `ByteSlice` #



## `Prepare` ##
`public static ByteSlice Prepare(string x)`

Converts the string x into buffer using UTF-8 encoding.  A new `ByteSlice` instance is then created using this buffer.

#### Example ####
```
/*
This code uses this sample input:

K38000,Gary Doe
I56463,Joes Smith
A14546,John Smith
C94854,Mary Doe
Y139409,Sam Smith
E09809,John Doe
*/
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    output.Add(ByteSlice.Prepare(parts[0].PadRight(16, '\0')), ByteSlice.Prepare(parts[1]));
} 
```

---




`public static ByteSlice Prepare(StringBuilder x)`

Converts the StringBuilder instance to string, which is then converted to buffer using UTF-8 encoding.  A new `ByteSlice` instance is then created

using this buffer.

#### Example ####
```
StringBuilder sb = new StringBuilder();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    sb.Remove(0, sb.Length);

    for (int i = 0; i < values.Length; i++)
    {
        sb.Append(values[i].Value.ToString());
        sb.Append("|");
    }

    output.Add(ByteSlice.Prepare("Values:"));
    output.Add(ByteSlice.Prepare(sb));
} 
```

---




`public static ByteSlice Prepare(IList<byte> data)`

Creates a new `ByteSlice` instance using data.

#### Example 1 ####
```
byte[] valueBuffer = new byte[20];
byte[] keyBuffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    for (int i = 0; i < valueBuffer.Length; i++)
    {
        valueBuffer[0] = 0x0;
    }

    line.CopyTo(valueBuffer);

    keyBuffer[0] = valueBuffer[0];
    keyBuffer[1] = valueBuffer[1];

    output.Add(ByteSlice.Prepare(keyBuffer), ByteSlice.Prepare(valueBuffer));
} 
```



#### Example 2 ####
In this example, a `List<byte>` instance is passed into the `AppendTo` method to get the bytes of the line.
```
byte[] keyBuffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    valueBuffer.Clear();
    line.AppendTo(valueBuffer);

    keyBuffer[0] = valueBuffer[0];
    keyBuffer[1] = valueBuffer[1];

    output.Add(ByteSlice.Prepare(keyBuffer), ByteSlice.Prepare(valueBuffer));
}
```

---




`public static ByteSlice Prepare(IList<byte> data, int offset, int length)`

Creates a new `ByteSlice` instance using data, offset, and length.

#### Example 1 ####
```
byte[] myBuffer = new byte[10];

public virtual void Map(ByteSlice line, MapOutput output)
{
    for (int i = 0; i < myBuffer.Length; i++)
    {
        myBuffer[0] = 0x0;
    }

    line.CopyTo(myBuffer);

    //Using the first 4 bytes as key.
    output.Add(ByteSlice.Prepare(myBuffer, 0, 4), ByteSlice.Prepare("1"));
} 
```



#### Example 2 ####
```
List<byte> myBuffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    myBuffer.Clear();
    line.CopyTo(myBuffer);

    //Using the first 4 bytes as key.
    output.Add(ByteSlice.Prepare(myBuffer, 0, 4), ByteSlice.Prepare("1"));
}
```

---




`public static ByteSlice Prepare(ByteSlice data, int offset, int length)`

Creates a new `ByteSlice` instance using data, offset, and length.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //Using the first 4 bytes as key.
    output.Add(ByteSlice.Prepare(line, 0, 4), ByteSlice.Prepare("1"));
} 
```

---




`public static ByteSlice Prepare()`

Creates a new ByteSlice instance with empty buffer.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //Use the first 4 bytes as key and empty ByteSlice as key.
    output.Add(ByteSlice.Prepare(line, 0, 4), ByteSlice.Prepare());
} 
```

---




## `PreparePaddedStringAscii` ##
`public static ByteSlice PreparePaddedStringAscii(string x, int size)`

Converts the string into buffer using ASCII encoding.  If the length of buffer is larger than size, the buffer is truncated.  If the length of

buffer is smaller than size, the buffer is padded with Ø bytes.  A new ByteSlice instance is then created using this buffer.
#### Remarks ####
To recover the string, the buffer must be unpadded first using the `UnpadKey` method.   Please see example.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], 16), ByteSlice.Prepare(parts[1]));
}

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += values[i].Value.ToString();
    }
    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
} 
```

---




## `PreparePaddedStringUTF8` ##
`public static ByteSlice PreparePaddedStringUTF8(string x, int size)`

Converts the string into buffer using UTF-8 encoding.  If the length of buffer is larger than size, the buffer is truncated at character breaks.

If the length of buffer is smaller than size, the buffer is padded with Ø bytes.  A new `ByteSlice` instance is then created using this buffer.
#### Remarks ####
To recover the string, the buffer must be unpadded first using the UnpadKey method.   Please see example.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    output.Add(ByteSlice.PreparePaddedStringUTF8(parts[0], 16), ByteSlice.Prepare(parts[1]));
}

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += values[i].Value.ToString();
    }
    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
} 
```