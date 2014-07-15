using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTests
{
    [TestClass]
    public class RedisTests
    {
        private readonly string _hostname;
        private readonly int _port;

        public RedisTests()
        {
            _hostname = "localhost";
            _port = 6379;
        }

        [TestMethod]
        public void PerformLegacyTests()
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
}
