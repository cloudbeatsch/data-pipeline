using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace DataPipeline.Entities
{
    public class Location : TableEntity, ICloneable
    {
        private string _principalId;
		/// <summary>
		/// Car/Driver ID from Nitrogen.  Used as PK.  Typically GUID.
		/// </summary>
        public string PrincipalId 
        {
            get { return this._principalId;  }
            set { this._principalId = this.PartitionKey = value; }
        }

        private long _measurementTime;
		/// <summary>
		/// Timestamp (milliseconds since epoch) of measurement.  
		/// Also used as RK, meaning no more than one measurement per driver per ms.
		/// </summary>
		public long MeasurementTime {
            get { return this._measurementTime; }
            set {
                this._measurementTime = value;
                this.RowKey = value.ToString();
            }
        }

        public double Latitude { get; set; }
        public double Longitude { get; set; }
		
		public object Clone()
		{
			return new Location()
			{
				PrincipalId = this.PrincipalId,
				MeasurementTime = this.MeasurementTime,
				Latitude = this.Latitude,
				Longitude = this.Longitude
			};
		}
	}
}