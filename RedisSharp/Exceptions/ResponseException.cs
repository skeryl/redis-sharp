using System;
using System.Collections.Generic;
using System.Text;

namespace RedisSharp.Exceptions
{
  public class ResponseException : Exception
  {
    public override string Message
    {
      get
      {
          return string.Format("{0}\r\n{1}", base.Message, string.Format("\r\n\t\tComand Text: {0}\r\n\r\n\t\tRaw Data: {1}\r\n", Command,
                                             RawData != null ? Encoding.UTF8.GetString(RawData) : new object()));
      }
    }

    public byte[] RawData { get; protected set; }

    public string Command { get; protected set; }

    public string Code { get; private set; }

    public ResponseException(string message)
      : base(string.Format("Response error: {0}", message))
    {
      Code = message;
      Command = string.Empty;
    }

    public ResponseException(string message, byte[] data, string cmd, string[] args)
      : this(message)
    {
      RawData = data;
      Command = args == null || data == null ? cmd : string.Format(cmd, args);
    }

    public ResponseException(string message, string cmd, IEnumerable<string> args)
      : this(message)
    {
      var stringBuilder = new StringBuilder();
      foreach (string str in args)
        stringBuilder.AppendLine(str);
      RawData = Encoding.UTF8.GetBytes(stringBuilder.ToString());
      Command = cmd;
    }
  }
}
