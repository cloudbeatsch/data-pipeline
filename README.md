# data-pipeline

This is a skeleton project for building a Apache Storm based data pipeline
based on input from an Azure Event Hub infrastructure.

It has an example data flow based on receiving location messages in JSON format from
Event Hub and breaks these down into typed tuples.

Dataflow in this topology starts with:

1.	[Java] Eventhub Spout -> (in EH, out flat JSON string Tuple)
2.	[C#]   Locations Bolt -> (in flat JSON string Tuple, out [Principal ID, Timestamp, Latitude, Longitude])

## Running locally

1. Open "DataPipeline.sln" in Visual Studio.
2. Press F5.  This will run the manually connected topology from LocalTest.cs using the sample data in locationData.csv.

You can use local mode to develop your own set of Bolts and Spouts. The solution also
includes a test project that you can use to test your Services that your Bolts are 
composed of.

## Running on Azure

1. Right click on the DataPipeline project at the top of the solution and 
change output type to "Class Library" from "Console Application."
2. Right click on the DataPipeline project again and click Settings.
3. Enter a value for StorageConnectionString.  This should be a connection string to your Azure Storage Account.
4. Enter a value for EventHubNamespace.
5. Enter a value for EventHubName.
6. Enter a value for EventHubPolicyKey.
7. Confirm and adjust as necessary the values for EventHubPartitionCount and EventHubPolicyName.