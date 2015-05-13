using System;
using System.Globalization;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;

using DataPipeline.Entities;
using DataPipeline.Services;

namespace DataPipeline.Bolts
{
    public class LocationsBolt : ISCPBolt
    {
        Context ctx;

        public LocationsBolt(Context ctx)
        {
            this.ctx = ctx;
            this.DefineSchemas();
        }

        public void DefineSchemas()
        {
            //
            // Example incoming message:
            // {
            //   "from":"550808654e2e400705d41008",
            //   "type":"location",
            //   "index_until":"2015-03-24T11:01:01.705Z",
            //   "expires":"2500-01-01T00:00:00.000Z",
            //   "body": {
            //             "longitude":-122.26736575899427,
            //             "latitude":47.67685521923653,
            //             "speed":17.03037474036217,
            //             "heading":71.53113981010392
            //           },
            //   "tags": [
            //             "involves:550808654e2e400705d41008"
            //           ],
            //   "response_to":[],
            //   "ts":"2015-03-17T11:01:00.714Z",
            //   "ver":0.2,
            //   "updated_at":"2015-03-17T11:01:01.705Z",
            //   "created_at":"2015-03-17T11:01:01.705Z",
            //   "id":"5508096d959431cb0552388e"
            // }
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { 
                typeof(string)   // Message json as string
            });

            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add("default", new List<Type>() { 
                typeof(string), // PrincipalId
                typeof(long),   // MeasurementTime
                typeof(double), // Latitude
                typeof(double)  // Longitude
            });

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));
            //this.ctx.DeclareCustomizedDeserializer(new CustomizedInteropJSONDeserializer());
        }

        // Get a new instance of the bolt
        public static LocationsBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new LocationsBolt(ctx);
        }

        public void Execute(SCPTuple tuple)
        {
            try
            {
                var message = (string)tuple.GetString(0);
                var location = LocationService.decodeMessage(message);

                if (location != null)
                {
                    this.ctx.Emit(
                        Constants.DEFAULT_STREAM_ID, new List<SCPTuple> { 
                        tuple 
                    }, new Values(
                            location.PrincipalId,
                            location.MeasurementTime,
                            location.Latitude,
                            location.Longitude
                        )
                    );
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
         }
    }
}