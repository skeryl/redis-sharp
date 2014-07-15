using System;
using System.Collections.Generic;
using System.Text;
using RedisSharp;

namespace UnitTests
{
    public class LegacyTests
    {
        public static void Main(string[] args)
        {
            Redis client = args.Length >= 2 ? new Redis(args[0], Convert.ToInt16(args[1])) : new Redis();

            client.Set("foo", "bar");
            client.FlushAll();
            if (client.Keys.Length > 0)
                Console.WriteLine("error: there should be no keys but there were {0}", client.Keys.Length);
            client.Set("foo", "bar");
            if (client.Keys.Length < 1)
                Console.WriteLine("error: there should be at least one key");
            if (client.GetKeys("f*").Length < 1)
                Console.WriteLine("error: there should be at least one key");

            if (client.TypeOf("foo") != KeyType.String)
                Console.WriteLine("error: type is not string");
            client.Set("bar", "foo");

            var arr = client.GetKeys("foo", "bar");
            if (arr.Length != 2)
                Console.WriteLine("error, expected 2 values");
            if (arr[0].Length != 3)
                Console.WriteLine("error, expected foo to be 3");
            if (arr[1].Length != 3)
                Console.WriteLine("error, expected bar to be 3");

            client["one"] = "world";
            if (client.GetSet("one", "newvalue") != "world")
                Console.WriteLine("error: Getset failed");
            if (!client.Rename("one", "two"))
                Console.WriteLine("error: failed to rename");
            if (client.Rename("one", "one"))
                Console.WriteLine("error: should have sent an error on rename");
            client.Db = 10;
            client.Set("foo", "diez");
            if (client.GetString("foo") != "diez")
            {
                Console.WriteLine("error: got {0}", client.GetString("foo"));
            }
            if (!client.Remove("foo"))
                Console.WriteLine("error: Could not remove foo");
            client.Db = 0;
            if (client.GetString("foo") != "bar")
                Console.WriteLine("error, foo was not bar");
            if (!client.ContainsKey("foo"))
                Console.WriteLine("error, there is no foo");
            if (client.Remove("foo", "bar") != 2)
                Console.WriteLine("error: did not remove two keys");
            if (client.ContainsKey("foo"))
                Console.WriteLine("error, foo should be gone.");
            client.Save();
            client.BackgroundSave();
            Console.WriteLine("Last save: {0}", client.LastSave);
            //r.Shutdown ();

            var info = client.GetInfo();
            foreach (var k in info.Keys)
            {
                Console.WriteLine("{0} -> {1}", k, info[k]);
            }

            var dict = new Dictionary<string, byte[]>();
            dict["hello"] = Encoding.UTF8.GetBytes("world");
            dict["goodbye"] = Encoding.UTF8.GetBytes("my dear");

            //r.Set (dict);

            client.RightPush("alist", "avalue");
            client.RightPush("alist", "another value");
            Assert(client.ListLength("alist") == 2, "List length should have been 2");

            var value = Encoding.UTF8.GetString(client.ListIndex("alist", 1));
            if (!value.Equals("another value"))
                Console.WriteLine("error: Received {0} and should have been 'another value'", value);
            value = Encoding.UTF8.GetString(client.LeftPop("alist"));
            if (!value.Equals("avalue"))
                Console.WriteLine("error: Received {0} and should have been 'avalue'", value);
            if (client.ListLength("alist") != 1)
                Console.WriteLine("error: List should have one element after pop");
            client.RightPush("alist", "yet another value");
            byte[][] values = client.ListRange("alist", 0, 1);
            if (!Encoding.UTF8.GetString(values[0]).Equals("another value"))
                Console.WriteLine("error: Range did not return the right values");

            Assert(client.AddToSet("FOO", Encoding.UTF8.GetBytes("BAR")), "Problem adding to set");
            Assert(client.AddToSet("FOO", Encoding.UTF8.GetBytes("BAZ")), "Problem adding to set");
            Assert(client.AddToSet("FOO", "Hoge"), "Problem adding string to set");
            Assert(client.CardinalityOfSet("FOO") == 3, "Cardinality should have been 3 after adding 3 items to set");
            Assert(client.IsMemberOfSet("FOO", Encoding.UTF8.GetBytes("BAR")), "BAR should have been in the set");
            Assert(client.IsMemberOfSet("FOO", "BAR"), "BAR should have been in the set");
            byte[][] members = client.GetMembersOfSet("FOO");
            Assert(members.Length == 3, "Set should have had 3 members");

            Assert(client.RemoveFromSet("FOO", "Hoge"), "Should have removed Hoge from set");
            Assert(!client.RemoveFromSet("FOO", "Hoge"), "Hoge should not have existed to be removed");
            Assert(2 == client.GetMembersOfSet("FOO").Length, "Set should have 2 members after removing Hoge");

            Assert(client.AddToSet("BAR", Encoding.UTF8.GetBytes("BAR")), "Problem adding to set");
            Assert(client.AddToSet("BAR", Encoding.UTF8.GetBytes("ITEM1")), "Problem adding to set");
            Assert(client.AddToSet("BAR", Encoding.UTF8.GetBytes("ITEM2")), "Problem adding string to set");

            Assert(client.GetUnionOfSets("FOO", "BAR").Length == 4, "Resulting union should have 4 items");
            Assert(1 == client.GetIntersectionOfSets("FOO", "BAR").Length, "Resulting intersection should have 1 item");
            Assert(1 == client.GetDifferenceOfSets("FOO", "BAR").Length, "Resulting difference should have 1 item");
            Assert(2 == client.GetDifferenceOfSets("BAR", "FOO").Length, "Resulting difference should have 2 items");

            byte[] itm = client.GetRandomMemberOfSet("FOO");
            Assert(null != itm, "GetRandomMemberOfSet should have returned an item");
            Assert(client.MoveMemberToSet("FOO", "BAR", itm), "Data within itm should have been moved to set BAR");


            client.FlushDb();

            if (client.Keys.Length > 0)
                Console.WriteLine("error: there should be no keys but there were {0}", client.Keys.Length);
        }

        static void Assert(bool condition, string message)
        {
            if (!condition)
            {
                Console.WriteLine("error: {0}", message);
            }
        }
    }
}
