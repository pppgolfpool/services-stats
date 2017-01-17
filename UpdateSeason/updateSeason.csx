#r "..\Common\PppPool.Common.dll"
#r "..\Common\Microsoft.WindowsAzure.Storage.dll"
#r "System.Xml.Linq"
#r "Newtonsoft.Json"

using System;
using System.Xml.Linq;
using System.Xml.XPath;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using PppPool.Common;

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    await Task.Delay(1);
    return null;
}