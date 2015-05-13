using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataPipeline
{
	public static class ConfigurationUtilities
	{
		public static T AppSetting<T>(string settingKey, Func<string, T> converter, T defaultVal = default(T))
		{
			T result = defaultVal;
			var settingStr = ConfigurationManager.AppSettings[settingKey];
			if (!string.IsNullOrWhiteSpace(settingStr))
			{
				result = converter(settingStr);
			}

			return result;
		}

	}
}
