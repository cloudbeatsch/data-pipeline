using System;
using System.Globalization;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

using Newtonsoft.Json.Linq;

using DataPipeline.Entities;

namespace DataPipeline.Services
{
    public class LocationService
    {
        public static Location decodeMessage(string message)
        {
            JObject jsonMessage = JObject.Parse(message);
            jsonMessage["body"] = JObject.Parse(jsonMessage["body"].ToString());

            Location location = null;

            if (jsonMessage["type"].ToString() == "location")
            {
                DateTime dt = DateTime.Parse(jsonMessage["ts"].ToString(), null, System.Globalization.DateTimeStyles.RoundtripKind);
                // Send this data along
                location = new Location
                {
                    PrincipalId = jsonMessage["from"].ToString(),
                    MeasurementTime = dt.Ticks,
                    Latitude = Double.Parse(jsonMessage["body"]["latitude"].ToString()),
                    Longitude = Double.Parse(jsonMessage["body"]["longitude"].ToString())
                };
            }

            return location;
        }
    }
}
