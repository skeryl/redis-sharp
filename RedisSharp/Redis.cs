using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using RedisSharp.Exceptions;

namespace RedisSharp
{
    public class Redis : IDisposable
    {
        private const long UnixEpoch = 621355968000000000L;
        private const char CharSpace = ' ';
        private readonly byte[] _endData = new[] {(byte) '\r', (byte) '\n'};
        private BufferedStream _bstream;
        private int _db;
        private Socket _socket;

        public Redis(string host, int port, bool logToConsole = false)
        {
            if (host == null)
                throw new ArgumentNullException("host");
            IsLoggingOn = logToConsole;
            Host = host;
            Port = port;
            SendTimeout = -1;
        }

        public Redis(string host)
            : this(host, 6379)
        {
        }

        public Redis()
            : this("localhost", 6379)
        {
        }

        public string Host { get; private set; }

        public int Port { get; private set; }

        public int RetryTimeout { get; set; }

        public int RetryCount { get; set; }

        public int SendTimeout { get; set; }

        public string Password { get; set; }

        public bool IsLoggingOn { get; protected set; }

        public int Db
        {
            get { return _db; }
            set
            {
                _db = value;
                SendExpectSuccess("SELECT {0}\r\n", new object[] { _db });
            }
        }

        public string this[string key]
        {
            get { return GetString(key); }
            set { Set(key, value); }
        }

        public int DbSize
        {
            get { return SendExpectInt("DBSIZE\r\n"); }
        }

        public DateTime LastSave
        {
            get
            {
                return new DateTime(UnixEpoch) + TimeSpan.FromSeconds(SendExpectInt("LASTSAVE\r\n"));
            }
        }

        public string[] Keys
        {
            get { return GetKeys("*"); }
        }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        ~Redis()
        {
            Dispose(false);
        }

        public void Set(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            Set(key, Encoding.UTF8.GetBytes(value));
        }

        public void Set(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            if (value.Length > 1073741824)
                throw new ArgumentException("value exceeds 1G", "value");
            if (!SendDataCommand(value, "SET", new[] { key }))
                throw new Exception("Unable to connect");
            ExpectSuccess();
        }

        public bool SetNX(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            return SetNX(key, Encoding.UTF8.GetBytes(value));
        }

        public bool SetNX(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            if (value.Length > 1073741824)
                throw new ArgumentException("value exceeds 1G", "value");
            return SendDataExpectInt(value, "SETNX {0} {1}\r\n", key, value.Length.ToString()) > 0;
        }

        public void Set(IDictionary<string, string> dict)
        {
            Set((dict).ToDictionary((k => k.Key), (v => Encoding.UTF8.GetBytes(v.Value))));
        }

        public void Set(IDictionary<string, byte[]> dict)
        {
            if (dict == null)
                throw new ArgumentNullException("dict");
            byte[] bytes1 = Encoding.UTF8.GetBytes("\r\n");
            var memoryStream = new MemoryStream();
            foreach (string index in dict.Keys)
            {
                byte[] buffer = dict[index];
                byte[] bytes2 = Encoding.UTF8.GetBytes("$" + index.Length + "\r\n");
                byte[] bytes3 = Encoding.UTF8.GetBytes(index + "\r\n");
                byte[] bytes4 = Encoding.UTF8.GetBytes("$" + buffer.Length + "\r\n");
                memoryStream.Write(bytes2, 0, bytes2.Length);
                memoryStream.Write(bytes3, 0, bytes3.Length);
                memoryStream.Write(bytes4, 0, bytes4.Length);
                memoryStream.Write(buffer, 0, buffer.Length);
                memoryStream.Write(bytes1, 0, bytes1.Length);
            }
            SendDataCommand(memoryStream.ToArray(), "*" + (dict.Count*2 + 1) + "\r\n$4\r\nMSET\r\n");
            ExpectSuccess();
        }

        public byte[] Get(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectData(null, "GET", key);
        }

        public string GetString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return Encoding.UTF8.GetString(Get(key));
        }

        public byte[][] Sort(SortOptions options)
        {
            return SendDataCommandExpectMultiBulkReply(null, options.ToCommand());
        }

        public byte[] GetSet(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            if (value.Length > 1073741824)
                throw new ArgumentException("value exceeds 1G", "value");
            if (!SendDataCommand(value, "GETSET", new[] { key }))
                throw new Exception("Unable to connect");
            return ReadData();
        }

        public string GetSet(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            return Encoding.UTF8.GetString(GetSet(key, Encoding.UTF8.GetBytes(value)));
        }

        private string ReadLine()
        {
            var stringBuilder = new StringBuilder();
            int num;
            while ((num = _bstream.ReadByte()) != -1)
            {
                if (num != 13)
                {
                    if (num != 10)
                        stringBuilder.Append((char) num);
                    else
                        break;
                }
            }
            return (stringBuilder).ToString();
        }

        private void Connect()
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                          {
                              NoDelay = true,
                              SendTimeout = SendTimeout
                          };
            _socket.Connect(Host, Port);
            if (!_socket.Connected)
            {
                _socket.Close();
                _socket = null;
            }
            else
            {
                _bstream = new BufferedStream(new NetworkStream(_socket), 16384);
                if (Password == null)
                    return;
                SendExpectSuccess("AUTH {0}\r\n", new object[] { Password });
            }
        }

        private byte[] Bytes(string str)
        {
            return Encoding.UTF8.GetBytes(str);
        }

        private bool SendDataCommand(byte[] data, string cmd, params string[] args)
        {
            var list = new List<byte[]>();
            if ((args).Any())
                list.AddRange((args).Select(Bytes));
            if (data != null)
                list.Add(data);
            return SendDataCommand(cmd, list.ToArray());
        }

        private bool SendDataCommand(string cmd, params byte[][] args)
        {
            if (_socket == null)
                Connect();
            if (_socket == null)
                return false;
            var list = new List<byte>();
            string str = string.Format("*{0}\r\n", (1 + args.Length));
            list.AddRange(Bytes(str));
            list.AddRange(GetCommandBytes(cmd));
            foreach (var data in args)
                list.AddRange(GetCommandBytes(data));
            try
            {
                byte[] numArray = list.ToArray();
                Log("S: " + Encoding.UTF8.GetString(numArray));
                _socket.Send(numArray);
            }
            catch (SocketException)
            {
                _socket.Close();
                _socket = null;
                return false;
            }
            return true;
        }

        private IEnumerable<byte> GetCommandBytes(string arg)
        {
            return GetCommandBytes(Bytes(arg));
        }

        private IEnumerable<byte> GetCommandBytes(byte[] data)
        {
            var list = new List<byte>();
            if (data != null)
            {
                string str = string.Format("${0}\r\n", data.Length);
                list.AddRange(Bytes(str));
                list.AddRange(data);
                list.AddRange(_endData);
            }
            return list;
        }

        private bool SendCommand(string cmd, params object[] args)
        {
            if (_socket == null)
                Connect();
            if (_socket == null)
                return false;
            byte[] bytes = Encoding.UTF8.GetBytes(args == null || args.Length <= 0 ? cmd : string.Format(cmd, args));
            try
            {
                if (args != null)
                    Log("S: " + string.Format(cmd, args));
                _socket.Send(bytes);
            }
            catch (SocketException)
            {
                _socket.Close();
                _socket = null;
                return false;
            }
            return true;
        }

        [Conditional("DEBUG")]
        private void Log(string fmt, params object[] args)
        {
            if (!IsLoggingOn)
                return;
            Console.WriteLine("{0}", args == null || args.Length <= 0 ? fmt : string.Format(fmt, args).Trim());
        }

        private void ExpectSuccess()
        {
            int num = _bstream.ReadByte();
            if (num == -1)
                throw new ResponseException("No more data");
            string str = ReadLine();
            Log(num +  str);
            if (num == 45)
                throw new ResponseException(str.StartsWith("ERR") ? str.Substring(4) : str);
        }

        private void SendExpectSuccess(string cmd, params object[] args)
        {
            if (!SendCommand(cmd, args))
                throw new Exception("Unable to connect");
            ExpectSuccess();
        }

        private int SendDataExpectInt(byte[] data, string cmd, params string[] args)
        {
            if (!SendDataCommand(data, cmd, args))
                throw new Exception("Unable to connect");
            int num = _bstream.ReadByte();
            if (num == -1)
                throw new ResponseException("No more data", data, cmd, args);
            string s = ReadLine();
            Log("R: " + s);
            if (num == 45)
                throw new ResponseException(s.StartsWith("ERR") ? s.Substring(4) : s, data, cmd, args);
            int result;
            if (num == 58 && int.TryParse(s, out result))
                return result;
            throw new ResponseException("Unknown reply on integer request: " + num + s, data, cmd, args);
        }

        private int SendDataExpectInt(string cmd, params byte[][] args)
        {
            if (!SendDataCommand(cmd, args))
                throw new Exception("Unable to connect");
            var strArray = new string[args.Length];
            int result;
            for (result = 0; result < args.Length; ++result)
                strArray[result] = Encoding.UTF8.GetString(args[result]);
            int num = _bstream.ReadByte();
            if (num == -1)
                throw new ResponseException("No more data", cmd, strArray);
            string s = ReadLine();
            Log("R: " + s);
            if (num == 45)
                throw new ResponseException(s.StartsWith("ERR") ? s.Substring(4) : s, cmd, strArray);
            if (num == 58 && int.TryParse(s, out result))
                return result;
            throw new ResponseException("Unknown reply on integer request: " + num + s, cmd, strArray);
        }

        private int SendExpectInt(string cmd, params object[] args)
        {
            if (!SendCommand(cmd, args))
                throw new Exception("Unable to connect");
            int num = _bstream.ReadByte();
            if (num == -1)
                throw new ResponseException("No more data");
            string s = ReadLine();
            Log("R: " + s);
            if (num == 45)
                throw new ResponseException(s.StartsWith("ERR") ? s.Substring(4) : s);
            int result;
            if (num == 58 && int.TryParse(s, out result))
                return result;
            throw new ResponseException("Unknown reply on integer request: " + num + s);
        }

        private string SendExpectString(string cmd, params object[] args)
        {
            if (!SendCommand(cmd, args))
                throw new Exception("Unable to connect");
            int num = _bstream.ReadByte();
            if (num == -1)
                throw new ResponseException("No more data");
            string str = ReadLine();
            Log("R: " + str);
            if (num == 45)
                throw new ResponseException(str.StartsWith("ERR") ? str.Substring(4) : str);
            if (num == 43)
                return str;
            throw new ResponseException("Unknown reply on integer request: " + num + str);
        }

        private string SendGetString(string cmd, params object[] args)
        {
            if (!SendCommand(cmd, args))
                throw new Exception("Unable to connect");
            return ReadLine();
        }

        private byte[] SendExpectData(byte[] data, string cmd, params string[] args)
        {
            if (!SendDataCommand(data, cmd, args))
                throw new Exception("Unable to connect");
            return ReadData();
        }

        private byte[] ReadData()
        {
            string str = ReadLine();
            Log("R: {0}", str);
            if (str.Length == 0)
                throw new ResponseException("Zero length respose");
            switch (str[0])
            {
                case '-':
                    throw new ResponseException(str.StartsWith("-ERR") ? str.Substring(5) : str.Substring(1));
                case '$':
                    if (str == "$-1")
                        return null;
                    int result1;
                    if (!int.TryParse(str.Substring(1), out result1))
                        throw new ResponseException("Invalid length");
                    var buffer = new byte[result1];
                    int offset = 0;
                    do
                    {
                        int num = _bstream.Read(buffer, offset, result1 - offset);
                        if (num < 1)
                            throw new ResponseException("Invalid termination mid stream");
                        offset += num;
                    } while (offset < result1);
                    if (_bstream.ReadByte() != 13 || _bstream.ReadByte() != 10)
                        throw new ResponseException("Invalid termination");
                    return buffer;
                case '*':
                    int result2;
                    if (int.TryParse(str.Substring(1), out result2))
                        return result2 <= 0 ? new byte[0] : ReadData();
                    throw new ResponseException("Unexpected length parameter" + str);
                default:
                    throw new ResponseException("Unexpected reply: " + str);
            }
        }

        public bool ContainsKey(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectInt("EXISTS " + key + "\r\n") == 1;
        }

        public bool Remove(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectInt("DEL " + key + "\r\n", key) == 1;
        }

        public int Remove(params string[] args)
        {
            if (args == null)
                throw new ArgumentNullException("args");
            return SendExpectInt("DEL " + string.Join(" ", args) + "\r\n");
        }

        public int Increment(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectInt("INCR " + key + "\r\n");
        }

        public int Increment(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectInt("INCRBY {0} {1}\r\n", key, count);
        }

        public int Decrement(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectInt("DECR " + key + "\r\n");
        }

        public int Decrement(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectInt("DECRBY {0} {1}\r\n",  key,  count);
        }

        public KeyType TypeOf(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            switch (SendExpectString("TYPE {0}\r\n", key))
            {
                case "none":
                    return KeyType.None;
                case "string":
                    return KeyType.String;
                case "set":
                    return KeyType.Set;
                case "list":
                    return KeyType.List;
                default:
                    throw new ResponseException("Invalid value");
            }
        }

        public string RandomKey()
        {
            return SendExpectString("RANDOMKEY\r\n");
        }

        public bool Rename(string oldKeyname, string newKeyname)
        {
            if (oldKeyname == null)
                throw new ArgumentNullException("oldKeyname");
            if (newKeyname == null)
                throw new ArgumentNullException("newKeyname");
            return SendGetString("RENAME {0} {1}\r\n",  oldKeyname,  newKeyname)[0] == 43;
        }

        public bool Expire(string key, int seconds)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectInt("EXPIRE {0} {1}\r\n",  key,  seconds) == 1;
        }

        public bool ExpireAt(string key, int time)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectInt("EXPIREAT {0} {1}\r\n",  key,  time) == 1;
        }

        public int TimeToLive(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectInt("TTL {0}\r\n", key);
        }

        public string Save()
        {
            return SendGetString("SAVE\r\n");
        }

        public void BackgroundSave()
        {
            SendGetString("BGSAVE\r\n");
        }

        public void Shutdown()
        {
            SendGetString("SHUTDOWN\r\n");
        }

        public void FlushAll()
        {
            SendGetString("FLUSHALL\r\n");
        }

        public void FlushDb()
        {
            SendGetString("FLUSHDB\r\n");
        }

        public Dictionary<string, string> GetInfo()
        {
            byte[] bytes = SendExpectData(null, "INFO");
            var dictionary = new Dictionary<string, string>();
            string @string = Encoding.UTF8.GetString(bytes);
            var chArray = new[] { '\n' };
            foreach (string str in @string.Split(chArray))
            {
                int length = str.IndexOf(':');
                if (length != -1)
                    dictionary.Add(str.Substring(0, length), str.Substring(length + 1));
            }
            return dictionary;
        }

        public string[] GetKeys(string pattern)
        {
            if (pattern == null)
                throw new ArgumentNullException("pattern");
            byte[] bytes = SendExpectData(null, "KEYS", pattern);
            if (bytes.Length == 0)
                return new string[0];
            return Encoding.UTF8.GetString(bytes).Split(new[] { CharSpace });
        }

        public byte[][] GetKeys(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");
            if (keys.Length == 0)
                throw new ArgumentException("keys");
            return SendDataCommandExpectMultiBulkReply(null, "MGET", keys);
        }

        public byte[][] SendDataCommandExpectMultiBulkReply(byte[] data, string command, params string[] args)
        {
            if (!SendDataCommand(data, command, args))
                throw new Exception("Unable to connect");
            int num = _bstream.ReadByte();
            if (num == -1)
                throw new ResponseException("No more data");
            string s = ReadLine();
            Log("R: " + s);
            if (num == 45)
                throw new ResponseException(s.StartsWith("ERR") ? s.Substring(4) : s);
            int result;
            if (num != 42 || !int.TryParse(s, out result))
                throw new ResponseException("Unknown reply on multi-request: " + num + s);
            var numArray = new byte[result][];
            for (int index = 0; index < result; ++index)
                numArray[index] = ReadData();
            return numArray;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || _socket == null)
                return;
            SendCommand("QUIT\r\n");
            _socket.Close();
            _socket = null;
        }

        public byte[][] ListRange(string key, int start, int end)
        {
            return SendDataCommandExpectMultiBulkReply(null, "LRANGE", key, start.ToString(), end.ToString());
        }

        public void LeftPush(string key, string value)
        {
            SendExpectSuccess("LPUSH {0} {1}\r\n{2}\r\n", key, value.Length, value);
        }

        public void RightPush(string key, string value)
        {
            SendExpectSuccess("RPUSH {0} {1}\r\n", key, value);
        }

        public int ListLength(string key)
        {
            return SendExpectInt("LLEN {0}\r\n", key);
        }

        public byte[] ListIndex(string key, int index)
        {
            SendCommand("LINDEX {0} {1}\r\n",  key,  index);
            return ReadData();
        }

        public byte[] LeftPop(string key)
        {
            SendCommand("LPOP {0}\r\n",key);
            return ReadData();
        }

        public bool AddToSet(string key, byte[] member)
        {
            return SendDataExpectInt(member, "SADD", key) > 0;
        }

        public int AddToSet(string key, IEnumerable<byte[]> members)
        {
            var list = new List<byte[]> { Bytes(key) };
            list.AddRange(members);
            return SendDataExpectInt("SADD", list.ToArray());
        }

        public bool AddToSet(string key, string member)
        {
            return AddToSet(key, Encoding.UTF8.GetBytes(member));
        }

        public int AddToSet(string key, IEnumerable<string> members)
        {
            return SendExpectInt("SADD {0} {1}\r\n", key, string.Join(" ", members));
        }

        public int CardinalityOfSet(string key)
        {
            return SendDataExpectInt(null, "SCARD", key);
        }

        public bool IsMemberOfSet(string key, byte[] member)
        {
            return SendDataExpectInt(member, "SISMEMBER", key) > 0;
        }

        public bool IsMemberOfSet(string key, string member)
        {
            return IsMemberOfSet(key, Encoding.UTF8.GetBytes(member));
        }

        public byte[][] GetMembersOfSet(string key)
        {
            return SendDataCommandExpectMultiBulkReply(null, "SMEMBERS", key);
        }

        public byte[] GetRandomMemberOfSet(string key)
        {
            return SendExpectData(null, "SRANDMEMBER", key);
        }

        public byte[] PopRandomMemberOfSet(string key)
        {
            return SendExpectData(null, "SPOP", key);
        }

        public bool RemoveFromSet(string key, byte[] member)
        {
            return SendDataExpectInt(member, "SREM", key) > 0;
        }

        public bool RemoveFromSet(string key, string member)
        {
            return RemoveFromSet(key, Encoding.UTF8.GetBytes(member));
        }

        public byte[][] GetUnionOfSets(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException();
            return SendDataCommandExpectMultiBulkReply(null, "SUNION", keys);
        }

        private void StoreSetCommands(string cmd, string destKey, params string[] keys)
        {
            if (string.IsNullOrEmpty(cmd))
                throw new ArgumentNullException("cmd");
            if (string.IsNullOrEmpty(destKey))
                throw new ArgumentNullException("destKey");
            if (keys == null)
                throw new ArgumentNullException("keys");
            SendExpectSuccess("{0} {1} {2}\r\n",  cmd,  destKey,  string.Join(" ", keys));
        }

        public void StoreUnionOfSets(string destKey, params string[] keys)
        {
            StoreSetCommands("SUNIONSTORE", destKey, keys);
        }

        public byte[][] GetIntersectionOfSets(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException();
            return SendDataCommandExpectMultiBulkReply(null, "SINTER", keys);
        }

        public void StoreIntersectionOfSets(string destKey, params string[] keys)
        {
            StoreSetCommands("SINTERSTORE", destKey, keys);
        }

        public byte[][] GetDifferenceOfSets(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");
            return SendDataCommandExpectMultiBulkReply(null, "SDIFF", keys);
        }

        public void StoreDifferenceOfSets(string destKey, params string[] keys)
        {
            StoreSetCommands("SDIFFSTORE", destKey, keys);
        }

        public bool MoveMemberToSet(string srcKey, string destKey, byte[] member)
        {
            return SendDataExpectInt(member, "SMOVE", srcKey, destKey) > 0;
        }
    }
}