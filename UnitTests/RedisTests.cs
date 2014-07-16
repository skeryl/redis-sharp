using System;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RedisSharp;

namespace UnitTests
{
    [TestClass]
    public class RedisTests
    {
        private readonly string _hostname;
        private readonly int _port;
        private readonly bool _doLegacyTests;

        public RedisTests()
        {
            _hostname = "localhost";
            _port = 6379;
            _doLegacyTests = false;
        }

        [TestMethod]
        public void PerformLegacyTests()
        {
            if(_doLegacyTests)
            {
                bool pass;
                string message = string.Empty;
                try
                {
                    LegacyTests.Main(new[] { _hostname, _port.ToString() });
                    pass = true;
                }
                catch (Exception e)
                {
                    message = e.ToString();
                    pass = false;
                }
                Assert.IsTrue(pass, message);
            }
        }

        [TestMethod]
        public void SetTests()
        {
            using (var client = new Redis(_hostname, _port))
            {
                client.FlushDb();

                Assert.IsTrue(client.AddToSet("FOO", Encoding.UTF8.GetBytes("BAR")), "Problem adding to set");
                Assert.IsTrue(client.AddToSet("FOO", Encoding.UTF8.GetBytes("BAZ")), "Problem adding to set");
                Assert.IsTrue(client.AddToSet("FOO", "Hoge"), "Problem adding string to set");
                Assert.IsTrue(client.CardinalityOfSet("FOO") == 3, "Cardinality should have been 3 after adding 3 items to set");
                Assert.IsTrue(client.IsMemberOfSet("FOO", Encoding.UTF8.GetBytes("BAR")), "BAR should have been in the set");
                Assert.IsTrue(client.IsMemberOfSet("FOO", "BAR"), "BAR should have been in the set");
                
                byte[][] members = client.GetMembersOfSet("FOO");
                Assert.IsTrue(members.Length == 3, "Set should have had 3 members");

                Assert.IsTrue(client.RemoveFromSet("FOO", "Hoge"), "Should have removed Hoge from set");
                Assert.IsTrue(!client.RemoveFromSet("FOO", "Hoge"), "Hoge should not have existed to be removed");
                Assert.IsTrue(2 == client.GetMembersOfSet("FOO").Length, "Set should have 2 members after removing Hoge");

                Assert.IsTrue(client.AddToSet("BAR", Encoding.UTF8.GetBytes("BAR")), "Problem adding to set");
                Assert.IsTrue(client.AddToSet("BAR", Encoding.UTF8.GetBytes("ITEM1")), "Problem adding to set");
                Assert.IsTrue(client.AddToSet("BAR", Encoding.UTF8.GetBytes("ITEM2")), "Problem adding string to set");

                Assert.IsTrue(client.GetUnionOfSets("FOO", "BAR").Length == 4, "Resulting union should have 4 items");
                Assert.IsTrue(1 == client.GetIntersectionOfSets("FOO", "BAR").Length, "Resulting intersection should have 1 item");
                Assert.IsTrue(1 == client.GetDifferenceOfSets("FOO", "BAR").Length, "Resulting difference should have 1 item");
                Assert.IsTrue(2 == client.GetDifferenceOfSets("BAR", "FOO").Length, "Resulting difference should have 2 items");

                byte[] itm = client.GetRandomMemberOfSet("FOO");
                Assert.IsNotNull(itm, "GetRandomMemberOfSet should have returned an item");
                Assert.IsTrue(client.MoveMemberToSet("FOO", "BAR", itm), "Data within itm should have been moved to set BAR");
            }
        }

        [TestMethod]
        public void ChangeDbTest()
        {
            using (var client = new Redis(_hostname, _port))
            {
                client.Db = 0;
                client.Set("foo", "bar");

                client.Db = 10;
                client.Set("foo", "diez");

                Assert.AreEqual(client.GetString("foo"), "diez");

                client.Db = 0;
                Assert.AreEqual(client.GetString("foo"), "bar");
            }
        }

        [TestMethod]
        public void SetAndGetTest()
        {
            using (var client = new Redis(_hostname, _port))
            {
                client.Set("foo", "bar");
                Assert.AreEqual(client.GetString("foo"), "bar");
            }
        }

        [TestMethod]
        public void GetSetAndRenameTest()
        {
            using (var client = new Redis(_hostname, _port))
            {
                client["one"] = "world";
                Assert.AreEqual(client.GetSet("one", "newvalue"), "world");
                Assert.IsTrue(client.Rename("one", "two"), "Failed to rename key 'one'.");
                Assert.IsFalse(client.Rename("one", "one"), "Should have sent an error on rename.");
            }
        }

        [TestMethod]
        public void ContainsKeyAndRemoveTest()
        {
            using (var client = new Redis(_hostname, _port))
            {
                client.Set("unknown", "nothing");
                Assert.IsTrue(client.ContainsKey("unknown"));
                client.Remove("unknown");
                Assert.IsFalse(client.ContainsKey("unknown"));

                client.Set("foo", "bar");
                client.Set("bar", "foo");
                Assert.IsTrue(client.Remove("foo", "bar") == 2, "Two keys should have been removed.");
            }
        }

        [TestMethod]
        public void SaveTest()
        {
            bool pass;
            string message = string.Empty;
            try
            {
                using (var client = new Redis(_hostname, _port))
                {
                    client.Save();
                    client.BackgroundSave();
                    Console.WriteLine("Last save: {0}", client.LastSave);
                }
                pass = true;
            }
            catch (Exception e)
            {
                message = e.ToString();
                pass = false;
            }
            Assert.IsTrue(pass, message);
        }

        [TestMethod]
        public void GetInfoTest()
        {
            bool pass;
            string message = string.Empty;
            try
            {
                using (var client = new Redis(_hostname, _port))
                {
                    var info = client.GetInfo();
                    foreach (var k in info.Keys)
                        Console.WriteLine("{0} -> {1}", k, info[k]);
                }
                pass = true;
            }
            catch (Exception e)
            {
                message = e.ToString();
                pass = false;
            }
            Assert.IsTrue(pass, message);
        }

        [TestMethod]
        public void KeyTests()
        {
            using (var client = new Redis(_hostname, _port))
            {
                client.Set("foo", "bar");
                Assert.IsTrue(client.GetKeys("f*").Length >= 1, "There should be at least one key that starts with 'f'.");

                client.Set("bar", "foo");
                Assert.IsTrue(client.GetKeys("foo", "bar").Length == 2, "2 keys should have been retrieved.");

                client.Remove("unknown");
                var keys = client.GetKeys("foo", "bar", "unknown");
                Assert.IsTrue(keys.Length == 3, "3 keys should have been retrieved.");
                Assert.IsNull(keys[2]);
            }
        }

        [TestMethod]
        public void ListTests()
        {
            using (var client = new Redis(_hostname, _port, true))
            {
                const string listKey = "alist";
                client.Remove(listKey);

                client.RightPush(listKey, "avalue");
                client.RightPush(listKey, "another value");
                Assert.IsTrue(client.ListLength(listKey) == 2, "List length should have been 2");

                var value = Encoding.UTF8.GetString(client.ListIndex(listKey, 1));
                Assert.AreEqual(value, "another value");

                value = Encoding.UTF8.GetString(client.LeftPop(listKey));
                Assert.AreEqual(value, "avalue");

                Assert.IsTrue(client.ListLength(listKey) == 1, "List should have one element after pop.");

                client.RightPush(listKey, "yet another value");

                value = "a value \"from\" the left! (with embedded quotes)";
                client.LeftPush(listKey, value);
                Assert.AreEqual(Encoding.UTF8.GetString(client.LeftPop(listKey)), value);

                byte[][] values = client.ListRange(listKey, 0, 2);
                Assert.AreEqual(Encoding.UTF8.GetString(values[0]), "another value");
            }
        }

        [TestMethod]
        public void FlushTest()
        {
            using (var client = new Redis(_hostname, _port, true))
            {
                client.FlushDb();
                client.Set("test", "value");
                Assert.IsTrue(client.Keys.Length > 0, "Failed to complete test: 'FlushTest'. No items exist in the DB to flush.");
                client.FlushDb();
                Assert.IsFalse(client.Keys.Length > 0, String.Format("there should be no keys but there were {0}", client.Keys.Length));
            }
        }
    }
}
