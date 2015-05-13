using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

using Newtonsoft.Json;

using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;

using DataPipeline.Entities;

namespace DataPipeline.Spouts
{
    
    public class LocalSpout : ISCPSpout
    {
		private string[] locations;

        private Context ctx;
		private const string principalId = "tim";
        private int currentIndex = 0;

        public LocalSpout(Context ctx)
        {
            this.ctx = ctx;

            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add("default", new List<Type>() { 
                typeof(string)  // location json message
            });

            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(null, outputSchema));
			this.locations = _ReadLocations(@"..\..\locationData.csv").ToArray();
        }

        public static LocalSpout Get(Context ctx, Dictionary<string, Object> parms)
        {
            Console.WriteLine("Local spout gotten");
            return new LocalSpout(ctx);
        }

        public void NextTuple(Dictionary<string, Object> parms)
        {
            if (this.currentIndex >= this.locations.Length)
                return;

            string currentLocation = this.locations[this.currentIndex++];
            this.ctx.Emit(new Values(
                currentLocation
            ));
        }

		public bool HasNext()
		{
			return this.currentIndex < this.locations.Length;
		}

        public void Ack(long seqId, Dictionary<string, Object> parms)
        {
        }

        public void Fail(long seqId, Dictionary<string, Object> parms)
        {
        }

		private IEnumerable<string> _ReadLocations(string path)
		{
			return File.ReadAllLines(path).Where(line => !line.StartsWith("#")).Select(line =>
			{
				var cols = line.Trim().Split(',');
                Dictionary<string, string> locationDict = new Dictionary<string, string>();

                locationDict.Add("from", principalId.ToString());

                var tsString = DateTime.Parse(cols[0].Trim()).ToString("o");

                locationDict.Add("ts", tsString);
                locationDict.Add("type", "location");
                
                Dictionary<string, string> bodyDict = new Dictionary<string, string>();
                bodyDict.Add("latitude", double.Parse(cols[1].Trim()).ToString());
                bodyDict.Add("longitude", double.Parse(cols[2].Trim()).ToString());

                locationDict.Add("body", JsonConvert.SerializeObject(bodyDict));
                var jsonString = JsonConvert.SerializeObject(locationDict);

                Console.WriteLine(jsonString);
                return jsonString;
			});
		}
	}
}