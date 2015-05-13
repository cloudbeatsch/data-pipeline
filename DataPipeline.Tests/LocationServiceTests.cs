using System;
using System.Linq;
using Microsoft.SCP;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;

using DataPipeline;
using DataPipeline.Entities;
using DataPipeline.Services;

namespace DataPipeline.Tests
{
    [TestClass]
    public class LocationServiceTest
    {
        string locationMessageFixture = @"{ 
            ""from"":""550808654e2e400705d41008"",
            ""type"":""location"",
            ""expires"":""2500-01-01T00:00:00.000Z"",
            ""body"": {
                ""longitude"":-122.2673,
                ""latitude"":47.6768,
                ""speed"":17.03037474036217,
                ""heading"":71.53113981010392
            },
            ""ts"":""2015-03-17T11:01:00.714Z"",
            ""ver"":0.2,
            ""updated_at"":""2015-03-17T11:01:01.705Z"",
            ""created_at"":""2015-03-17T11:01:01.705Z"",
            ""id"":""5508096d959431cb0552388e""
        }";

        [TestMethod]
        public void TestLocationDecode()
        {
            var location = LocationService.decodeMessage(locationMessageFixture);
            Assert.AreEqual(location.Latitude, 47.6768);
            Assert.AreEqual(location.Longitude, -122.2673);
        }
    }
}
