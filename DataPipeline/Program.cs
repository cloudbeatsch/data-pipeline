using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;

using DataPipeline;
using DataPipeline.Bolts;
using DataPipeline.Entities;
using DataPipeline.Spouts;

using System.Diagnostics;

namespace DataPipeline
{
    [Active(true)]
    class Program : TopologyDescriptor
    {
        const int PRINCIPAL_ID_FIELD = 0;

        static void Main(string[] args)
        {
			Trace.Listeners.Add(new ConsoleTraceListener());
            Console.WriteLine("Starting tests");
            System.Environment.SetEnvironmentVariable("microsoft.scp.logPrefix", "WordCount-LocalTest");
            // Initialize the runtime
            SCPRuntime.Initialize();

            //If we are not running under the local context, throw an error
            if (Context.pluginType != SCPPluginType.SCP_NET_LOCAL)
            {
                throw new Exception(string.Format("unexpected pluginType: {0}", Context.pluginType));
            }
            // Create test instance
            LocalTest tests = new LocalTest();
            // Run tests
            tests.RunTestCase();
            Console.WriteLine("Tests finished");
            Console.ReadKey();
        }

        public ITopologyBuilder GetTopologyBuilder()
        {
            // Create a new topology named 'WordCount'
            TopologyBuilder topologyBuilder = new TopologyBuilder("Location");

            // Begin Java Eventhub Spout Construction
            // From https://github.com/Blackmist/eventhub-storm-hybrid/blob/master/EventHubReader/
            //
            //Get the partition count

            int partitionCount = Properties.Settings.Default.EventHubPartitionCount;

            //Create the constructor for the Java spout
            JavaComponentConstructor constructor = JavaComponentConstructor.CreateFromClojureExpr(
                String.Format(@"(com.microsoft.eventhubs.spout.EventHubSpout. (com.microsoft.eventhubs.spout.EventHubSpoutConfig. " +
                    @"""{0}"" ""{1}"" ""{2}"" ""{3}"" {4} ""{5}""))",
                    Properties.Settings.Default.EventHubPolicyName,
                    Properties.Settings.Default.EventHubPolicyKey,
                    Properties.Settings.Default.EventHubNamespace,
                    Properties.Settings.Default.EventHubName,
                    partitionCount,
                    "")); //zookeeper connection string - leave empty

            //Set the spout to use the JavaComponentConstructor
            topologyBuilder.SetJavaSpout(
                "messages",         //Friendly name of this component
                constructor,        //Pass in the Java constructor
                partitionCount);    //Parallelism hint - partition count

            // Use a JSON Serializer to serialize data from the Java Spout into a JSON string
            List<string> javaSerializerInfo = new List<string>() {
                "microsoft.scp.storm.multilang.CustomizedInteropJSONSerializer" 
            };
            // End Java Eventhub Spout Construction

            // Add the locations bolt to the topology. This filters locations messages out of all of nitrogen.
            // Name the component 'locations'
            // Name the fields that are emitted 'latitude' and 'longitude'
            // Use fieldsGroupting to distribute incoming tuples from the 'locations' spout to instances of the cleanser based on principalId.

            topologyBuilder.SetBolt(
                "locations",
                LocationsBolt.Get,
                new Dictionary<string, List<string>>()
                {
                    {
                        Constants.DEFAULT_STREAM_ID, new List<string>() {
                            "principalId",
                            "measurementTime",
                            "latitude",
                            "longitude"
                        }
                    }
                },
                partitionCount
            ).
            DeclareCustomizedJavaSerializer(javaSerializerInfo). //Use the serializer when sending to the bolt
            shuffleGrouping("messages");

            // Add topology config
            topologyBuilder.SetTopologyConfig(new Dictionary<string, string>() 
            {
                { "topology.kryo.register", "[\"[B\"]" },
                { "topology.worker.childopts",@"""-Xmx1024m"""},
                { "topology.ackers.executors", "0"}
            });

            return topologyBuilder;
        }
    }
}

