using System;
using dexih.functions;

namespace dexih.connections.webservice.restful
{
	public class WebAPI : Table
	{
		public string RestfulUri { get; set; }
		public string RowPath { get; set; }

		public WebAPI()
		{
		}
	}
}
