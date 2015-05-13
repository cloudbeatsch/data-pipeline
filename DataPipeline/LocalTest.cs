using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;

using DataPipeline.Bolts;
using DataPipeline.Spouts;

namespace DataPipeline
{
    class LocalTest
    {
        // Drives the topology components
        public void RunTestCase()
        {
            // An empty dictionary for use when creating components
            Dictionary<string, Object> emptyDictionary = new Dictionary<string, object>();

            #region Run a local spout to stand in for the Event Hub spout
            {
                Console.WriteLine("Starting local spout");
                // LocalContext is a local-mode context that can be used to initialize
                // components in the development environment.
                LocalContext spoutCtx = LocalContext.Get();
                // Get a new instance of the spout, using the local context
                LocalSpout locationSpout = LocalSpout.Get(spoutCtx, emptyDictionary);

                while (locationSpout.HasNext())
                {
                    locationSpout.NextTuple(emptyDictionary);
                }
                // Use LocalContext to persist the data stream to file
                spoutCtx.WriteMsgQueueToFile("locationMessages.txt");
                Console.WriteLine("Local spout finished");
            }
            #endregion

            #region Test the location time bolt
            {
                Console.WriteLine("Starting locations bolt");
                // LocalContext is a local-mode context that can be used to initialize
                // components in the development environment.
                LocalContext locationsCtx = LocalContext.Get();
                // Get a new instance of the bolt
                LocationsBolt locationsBolt = LocationsBolt.Get(locationsCtx, emptyDictionary);

                // Set the data stream to the data created by splitter bolt
                locationsCtx.ReadFromFileToMsgQueue("locationMessages.txt");
                // Get a batch of tuples from the stream
                List<SCPTuple> batch = locationsCtx.RecvFromMsgQueue();
                // Process each tuple in the batch

                Console.WriteLine("locations bolt loaded: " + batch.Count);

                foreach (SCPTuple tuple in batch)
                {
                    Console.WriteLine("executing tuple: " + tuple.GetString(0));
                    locationsBolt.Execute(tuple);
                }
                // Use LocalContext to persist the data stream to file
                locationsCtx.WriteMsgQueueToFile("locations.txt");
                Console.WriteLine("Locations bolt finished");
            }
            #endregion
        }
    }
}
