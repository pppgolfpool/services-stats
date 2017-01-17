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

// season, tour, key(month, user)
public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    var jwt = await req.GetJwt("submitter");
    if (jwt == null)
        return req.CreateError(HttpStatusCode.Unauthorized);

    IDictionary<string, string> query = req.GetQueryNameValuePairs().ToDictionary(pair => pair.Key, pair => pair.Value);

    var connectionString = "StatsStorage".GetEnvVar();
    var blobService = new BlobService(connectionString);

    var tournamentsUrl = "TournamentsUrl".GetEnvVar();
    var seasonData = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/Season", null, "ServiceToken".GetEnvVar());
    var season = (int)seasonData["Season"];
    var week = (int)seasonData["CurrentWeek"];

    if (query["season"].ToLower() != "current")
    {
        season = Convert.ToInt32(query["season"]);
    }

    var tour = query["tour"];
    var key = query["key"].ToLower();

    JArray data = null;
    string file = null;

    if(key == "month")
    {
        file = await blobService.DownloadBlobAsync("monthly", $"{season}/{tour}/month.json");
    }

    if(key == "user")
    {
        file = await blobService.DownloadBlobAsync("monthly", $"{season}/{tour}/poolie.json");
    }

    if (string.IsNullOrEmpty(file))
        throw new Exception("Must have data by now");

    data = JArray.Parse(file);

    return req.CreateOk(data);
}