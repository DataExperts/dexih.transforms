using System;
using dexih.functions;

namespace dexih.connections.webservice
{
	public class RestFunction : Table
	{
		public string RestfulUri { get; set; }
		public string RowPath { get; set; }

		public RestFunction()
		{
		}
	}
}
